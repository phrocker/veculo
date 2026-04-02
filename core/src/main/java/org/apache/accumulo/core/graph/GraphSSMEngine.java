/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.graph;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful Recursive Graph Reasoning (SSM) engine. Performs multi-hop graph traversal where each
 * hop updates a hidden state vector, enabling semantic drift along paths of decreasing but
 * meaningful similarity.
 *
 * <p>
 * At each hop the hidden state is updated as:
 *
 * <pre>
 *   h_t = normalize(alpha * h_{t-1} + (1 - alpha) * normalize(x_t))
 * </pre>
 *
 * where {@code x_t} is the current vertex embedding. The next hop follows the unvisited neighbor
 * whose embedding is most aligned with the accumulated hidden state. Traversal terminates when no
 * neighbor exceeds the similarity threshold or the maximum depth is reached.
 */
public final class GraphSSMEngine {

  private static final Logger log = LoggerFactory.getLogger(GraphSSMEngine.class);

  private GraphSSMEngine() {}

  // ─── Inner classes ──────────────────────────────────────────────────────────

  /** A single hop in the reasoning path. */
  public static class ReasoningHop {
    public final String vertexId;
    public final float score;
    public final float[] hiddenState;

    public ReasoningHop(String vertexId, float score, float[] hiddenState) {
      this.vertexId = vertexId;
      this.score = score;
      this.hiddenState = hiddenState;
    }
  }

  /** Result of a reasoning traversal. */
  public static class ReasoningResult {
    public final List<ReasoningHop> path;
    public final float[] finalState;
    public final boolean terminatedEarly;
    public final String terminationReason;

    public ReasoningResult(List<ReasoningHop> path, float[] finalState, boolean terminatedEarly,
        String terminationReason) {
      this.path = path;
      this.finalState = finalState;
      this.terminatedEarly = terminatedEarly;
      this.terminationReason = terminationReason;
    }

    /** Converts this result to a serializable Map suitable for JSON responses. */
    public Map<String,Object> toMap() {
      List<Map<String,Object>> pathMaps = new ArrayList<>();
      for (ReasoningHop hop : path) {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("vertex_id", hop.vertexId);
        m.put("score", hop.score);
        pathMaps.add(m);
      }
      Map<String,Object> result = new LinkedHashMap<>();
      result.put("path", pathMaps);
      result.put("hop_count", path.size());
      result.put("terminated_early", terminatedEarly);
      if (terminationReason != null) {
        result.put("termination_reason", terminationReason);
      }
      // Serialize finalState as comma-separated for resume
      if (finalState != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < finalState.length; i++) {
          if (i > 0) {
            sb.append(",");
          }
          sb.append(finalState[i]);
        }
        result.put("final_state", sb.toString());
      }
      return result;
    }
  }

  /** Internal container for vertex data read from a scan. */
  private static class VertexData {
    final float[] embedding;
    final List<String> neighbors;
    final Map<String,float[]> neighborEmbeddings;

    VertexData(float[] embedding, List<String> neighbors, Map<String,float[]> neighborEmbeddings) {
      this.embedding = embedding;
      this.neighbors = neighbors;
      this.neighborEmbeddings = neighborEmbeddings;
    }
  }

  // ─── Main reasoning method ──────────────────────────────────────────────────

  /**
   * Performs stateful multi-hop graph reasoning starting from a vertex and following the path that
   * best aligns with an evolving hidden state.
   *
   * @param client AccumuloClient for creating scanners
   * @param table the graph table name
   * @param auths authorizations for visibility filtering
   * @param queryVector the query embedding (initial hidden state h_0)
   * @param startVertexId starting vertex ID; if null, finds nearest vertex to queryVector
   * @param maxDepth maximum number of hops
   * @param alpha state momentum (0 = reset each hop, 1 = ignore new embeddings)
   * @param threshold minimum cosine similarity to continue traversal
   * @param useScanServer if true, route scans through inference scan servers
   * @return ReasoningResult containing the traversal path and final hidden state
   */
  public static ReasoningResult reason(AccumuloClient client, String table, Authorizations auths,
      float[] queryVector, String startVertexId, int maxDepth, float alpha, float threshold,
      boolean useScanServer)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return reason(client, table, auths, queryVector, startVertexId, maxDepth, alpha, threshold,
        useScanServer, null);
  }

  /**
   * Performs stateful multi-hop graph reasoning with optional edge type filtering.
   *
   * @param edgeTypes if non-null, only follow edges of these types. Null means follow all.
   */
  public static ReasoningResult reason(AccumuloClient client, String table, Authorizations auths,
      float[] queryVector, String startVertexId, int maxDepth, float alpha, float threshold,
      boolean useScanServer, List<String> edgeTypes)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    // Initialize hidden state as normalized copy of query vector
    float[] hiddenState = normalize(Arrays.copyOf(queryVector, queryVector.length));

    // If no start vertex provided, find the nearest one
    if (startVertexId == null || startVertexId.isEmpty()) {
      try {
        // Use the same scan server routing for seed finding
        List<String> nearest =
            GraphVectorBridge.findSimilarVertexIds(client, table, auths, queryVector, 1, 0.0f);
        if (nearest.isEmpty()) {
          return new ReasoningResult(List.of(), hiddenState, true, "no_vertices_found");
        }
        startVertexId = nearest.get(0);
      } catch (Exception e) {
        log.warn("Seed finding failed: {}", e.getMessage());
        return new ReasoningResult(List.of(), hiddenState, true, "seed_search_failed");
      }
    }

    List<ReasoningHop> path = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    String currentVertexId = startVertexId;

    for (int hop = 0; hop < maxDepth; hop++) {
      // Check for cycle
      if (visited.contains(currentVertexId)) {
        return new ReasoningResult(path, hiddenState, true, "cycle_detected");
      }
      visited.add(currentVertexId);

      // Read vertex data: embedding + outgoing edges + neighbor embeddings
      VertexData vertexData =
          readVertexData(client, table, auths, currentVertexId, useScanServer, edgeTypes);

      // Update hidden state if vertex has embedding; skip update if not.
      // Vertices without embeddings (extracted entities, citations) are still
      // traversable — the hidden state carries forward from the previous hop.
      float score;
      if (vertexData.embedding != null && vertexData.embedding.length == hiddenState.length) {
        float[] normalizedEmbedding =
            normalize(Arrays.copyOf(vertexData.embedding, vertexData.embedding.length));
        for (int i = 0; i < hiddenState.length; i++) {
          hiddenState[i] = alpha * hiddenState[i] + (1 - alpha) * normalizedEmbedding[i];
        }
        hiddenState = normalize(hiddenState);
        score = cosineSimilarity(hiddenState, vertexData.embedding);
      } else {
        // No embedding — carry forward hidden state unchanged, score = 0
        score = 0.0f;
      }
      path.add(
          new ReasoningHop(currentVertexId, score, Arrays.copyOf(hiddenState, hiddenState.length)));

      // Score all unvisited neighbors
      String bestNeighbor = null;
      float bestScore = -1.0f;

      for (String neighbor : vertexData.neighbors) {
        if (visited.contains(neighbor)) {
          continue;
        }
        float[] neighborEmbedding = vertexData.neighborEmbeddings.get(neighbor);
        float neighborScore;
        if (neighborEmbedding != null && neighborEmbedding.length == hiddenState.length) {
          neighborScore = cosineSimilarity(hiddenState, neighborEmbedding);
        } else {
          // No embedding — use a default score so graph structure is still traversable.
          // Slightly below threshold so embedding-rich paths are preferred, but the SSM
          // can still follow structural edges when no embeddings exist.
          neighborScore = threshold + 0.01f;
        }
        if (neighborScore > bestScore) {
          bestScore = neighborScore;
          bestNeighbor = neighbor;
        }
      }

      // Pick best-scoring neighbor above threshold
      if (bestNeighbor == null) {
        return new ReasoningResult(path, hiddenState, true, "no_unvisited_neighbors");
      }
      if (bestScore < threshold) {
        return new ReasoningResult(path, hiddenState, true,
            "below_threshold:" + String.format("%.4f", bestScore));
      }

      currentVertexId = bestNeighbor;
    }

    // Reached max depth
    return new ReasoningResult(path, hiddenState, false, null);
  }

  // ─── Vertex data reader ─────────────────────────────────────────────────────

  /**
   * Reads a vertex's embedding, outgoing edges, and neighbor embeddings from the graph table.
   */
  private static VertexData readVertexData(AccumuloClient client, String table,
      Authorizations auths, String vertexId, boolean useScanServer, List<String> edgeTypes)
      throws TableNotFoundException {

    float[] embedding = null;
    List<String> neighbors = new ArrayList<>();

    // Scan the vertex row for embedding and outgoing edges.
    // If scan server routing fails, retry on tserver.
    try {
      return readVertexDataInternal(client, table, auths, vertexId, useScanServer, edgeTypes);
    } catch (Exception e) {
      if (useScanServer) {
        log.info("Scan server read failed for '{}', falling back to tserver: {}", vertexId,
            e.getMessage());
        return readVertexDataInternal(client, table, auths, vertexId, false, edgeTypes);
      }
      throw e;
    }
  }

  private static VertexData readVertexDataInternal(AccumuloClient client, String table,
      Authorizations auths, String vertexId, boolean useScanServer, List<String> edgeTypes)
      throws TableNotFoundException {
    float[] embedding = null;
    List<String> neighbors = new ArrayList<>();

    try (Scanner scanner = client.createScanner(table, auths)) {
      if (useScanServer) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "ssm_recursive"));
      }
      scanner.setRange(Range.exact(vertexId));

      for (Map.Entry<Key,Value> entry : scanner) {
        String cf = entry.getKey().getColumnFamily().toString();
        String cq = entry.getKey().getColumnQualifier().toString();

        if (GraphSchema.VERTEX_COLFAM.equals(cf) && GraphSchema.EMBEDDING_COLQUAL.equals(cq)) {
          embedding = parseEmbedding(entry.getValue());
        } else if (cf.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
          // Apply edge type filter if specified
          if (edgeTypes != null) {
            String edgeType = cf.substring(GraphSchema.EDGE_COLFAM_PREFIX.length());
            if (!edgeTypes.contains(edgeType)) {
              continue;
            }
          }
          neighbors.add(cq);
        }
      }
    }

    // Read all neighbor embeddings in a single BatchScanner call.
    // This replaces N individual scanner RPCs with 1 parallel batch read across tablets.
    Map<String,float[]> neighborEmbeddings = new HashMap<>();
    if (!neighbors.isEmpty()) {
      List<Range> ranges = new ArrayList<>(neighbors.size());
      for (String neighbor : neighbors) {
        ranges.add(Range.exact(neighbor));
      }

      try (var batchScanner = client.createBatchScanner(table, auths, 4)) {
        batchScanner.setRanges(ranges);
        batchScanner.fetchColumn(new Text(GraphSchema.VERTEX_COLFAM),
            new Text(GraphSchema.EMBEDDING_COLQUAL));

        for (Map.Entry<Key,Value> entry : batchScanner) {
          String neighborId = entry.getKey().getRow().toString();
          float[] neighborEmbedding = parseEmbedding(entry.getValue());
          if (neighborEmbedding != null) {
            neighborEmbeddings.put(neighborId, neighborEmbedding);
          }
        }
      } catch (Exception e) {
        log.debug(
            "BatchScanner failed for neighbor embeddings, falling back to individual scans: {}",
            e.getMessage());
        // Fallback: individual scans (slower but works if BatchScanner is unavailable)
        for (String neighbor : neighbors) {
          try (Scanner scanner = client.createScanner(table, auths)) {
            scanner.setRange(Range.exact(neighbor));
            scanner.fetchColumn(new Text(GraphSchema.VERTEX_COLFAM),
                new Text(GraphSchema.EMBEDDING_COLQUAL));
            for (Map.Entry<Key,Value> entry : scanner) {
              float[] neighborEmbedding = parseEmbedding(entry.getValue());
              if (neighborEmbedding != null) {
                neighborEmbeddings.put(neighbor, neighborEmbedding);
              }
            }
          }
        }
      }
    }

    return new VertexData(embedding, neighbors, neighborEmbeddings);
  }

  // ─── Math helpers ───────────────────────────────────────────────────────────

  /**
   * Parses an embedding from raw value bytes using BIG_ENDIAN float encoding, matching the format
   * used by VectorIndexWriter for RFile round-trip safety.
   */
  private static float[] parseEmbedding(Value value) {
    byte[] bytes = value.get();
    if (bytes == null || bytes.length < 4 || bytes.length % 4 != 0) {
      return null;
    }
    float[] vector = new float[bytes.length / 4];
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(vector);
    return vector;
  }

  /**
   * Computes cosine similarity between two vectors: dot(a,b) / (||a|| * ||b||).
   */
  private static float cosineSimilarity(float[] a, float[] b) {
    if (a.length != b.length) {
      return 0.0f;
    }
    float dotProduct = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;

    for (int i = 0; i < a.length; i++) {
      dotProduct += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }

    if (normA == 0.0f || normB == 0.0f) {
      return 0.0f;
    }

    return dotProduct / (float) (Math.sqrt(normA) * Math.sqrt(normB));
  }

  /**
   * L2-normalizes a vector in place, returning the same array.
   */
  private static float[] normalize(float[] vector) {
    float norm = 0.0f;
    for (float v : vector) {
      norm += v * v;
    }
    if (norm == 0.0f) {
      return vector;
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < vector.length; i++) {
      vector[i] /= norm;
    }
    return vector;
  }
}
