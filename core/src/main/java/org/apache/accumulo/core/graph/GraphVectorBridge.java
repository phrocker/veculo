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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.VectorIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side orchestration connecting vector similarity search with graph traversal for RAG
 * (Retrieval Augmented Generation) workloads.
 *
 * <p>
 * Typical RAG pipeline:
 * <ol>
 * <li>Vector similarity search finds top-K vertices by embedding distance</li>
 * <li>Graph traversal expands neighborhoods around those vertices</li>
 * <li>Combined results provide context for LLM generation</li>
 * </ol>
 *
 * <p>
 * Vector search and graph traversal operate on different key ranges, so they are orchestrated
 * client-side rather than combined in a single server-side iterator.
 */
public final class GraphVectorBridge {

  private static final Logger log = LoggerFactory.getLogger(GraphVectorBridge.class);

  private static final int VECTOR_ITERATOR_PRIORITY = 25;
  private static final int VECTOR_SEEK_ITERATOR_PRIORITY = 25;
  private static final int DEFAULT_SEARCH_RADIUS = 2;

  private GraphVectorBridge() {}

  /**
   * Finds vertex IDs whose embeddings are most similar to the query vector.
   *
   * @param scanner a scanner configured for the graph table
   * @param queryVector the query embedding
   * @param topK number of results to return
   * @param threshold minimum similarity score
   * @return list of vertex IDs ordered by similarity (most similar first)
   */
  public static List<String> findSimilarVertexIds(Scanner scanner, float[] queryVector, int topK,
      float threshold) {
    // Only fetch embedding column — avoids reading large _extracted_text values
    scanner.fetchColumn(new Text(GraphSchema.VERTEX_COLFAM),
        new Text(GraphSchema.EMBEDDING_COLQUAL));

    // Configure vector iterator to search embeddings
    IteratorSetting is =
        new IteratorSetting(VECTOR_ITERATOR_PRIORITY, "vectorSearch", VectorIterator.class);
    is.addOption(VectorIterator.QUERY_VECTOR_OPTION, vectorToString(queryVector));
    is.addOption(VectorIterator.TOP_K_OPTION, String.valueOf(topK));
    is.addOption(VectorIterator.THRESHOLD_OPTION, String.valueOf(threshold));
    is.addOption(VectorIterator.SIMILARITY_TYPE_OPTION, "COSINE");
    scanner.addScanIterator(is);

    // Scan and collect vertex IDs from results
    List<String> vertexIds = new ArrayList<>();
    for (Map.Entry<Key,Value> entry : scanner) {
      vertexIds.add(entry.getKey().getRow().toString());
    }
    return vertexIds;
  }

  /**
   * Expands neighborhoods around a set of vertices by following edges.
   *
   * @param scanner a scanner configured for the graph table
   * @param vertexIds seed vertex IDs to expand from
   * @param edgeType edge type to follow (null for all types)
   * @param depth maximum traversal depth
   * @return map from each seed vertex to its discovered neighbor IDs
   */
  public static Map<String,List<String>> expandNeighborhoods(Scanner scanner,
      List<String> vertexIds, String edgeType, int depth) {
    Map<String,List<String>> neighborhoods = new HashMap<>();

    for (String vertexId : vertexIds) {
      scanner.clearScanIterators();
      scanner.setRange(Range.exact(vertexId));

      if (edgeType != null) {
        GraphScanner.fetchOutgoingEdges(scanner, edgeType);
      }

      List<String> neighbors = new ArrayList<>();
      for (Map.Entry<Key,Value> entry : scanner) {
        String colFam = entry.getKey().getColumnFamily().toString();
        if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
          neighbors.add(entry.getKey().getColumnQualifier().toString());
        }
      }

      // For deeper traversals, recursively expand (depth > 1)
      if (depth > 1 && !neighbors.isEmpty()) {
        Map<String,List<String>> deeper =
            expandNeighborhoods(scanner, neighbors, edgeType, depth - 1);
        for (List<String> deeperNeighbors : deeper.values()) {
          neighbors.addAll(deeperNeighbors);
        }
      }

      neighborhoods.put(vertexId, neighbors);
    }

    return neighborhoods;
  }

  /**
   * Finds vertex IDs whose embeddings are most similar to the query vector, using the vidx table if
   * available for O(log n) seek, or falling back to full-scan VectorIterator.
   *
   * @param client AccumuloClient for checking table existence and creating scanners
   * @param graphTable the graph table name
   * @param auths authorizations for visibility filtering
   * @param queryVector the query embedding
   * @param topK number of results to return
   * @param threshold minimum similarity score
   * @return list of vertex IDs ordered by similarity (most similar first)
   */
  public static List<String> findSimilarVertexIds(AccumuloClient client, String graphTable,
      Authorizations auths, float[] queryVector, int topK, float threshold)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    String vidxTable = VectorIndexTable.vidxTableName(graphTable);

    // Try vidx seek path first
    if (client.tableOperations().exists(vidxTable)) {
      log.debug("Using vidx table '{}' for vector search", vidxTable);
      try (Scanner vidxScanner = client.createScanner(vidxTable, auths)) {
        GraphScanner.configureVectorSearch(vidxScanner, queryVector, topK, threshold,
            DEFAULT_SEARCH_RADIUS);

        List<String> vertexIds = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : vidxScanner) {
          String rowKey = entry.getKey().getRow().toString();
          vertexIds.add(VectorIndexTable.extractVertexId(rowKey));
        }
        if (!vertexIds.isEmpty()) {
          return vertexIds;
        }
        log.debug("vidx table empty, falling back to full-scan on '{}'", graphTable);
      }
    }

    // Fall back to full-scan VectorIterator on graph table
    log.debug("Falling back to full-scan VectorIterator on '{}'", graphTable);
    try (Scanner graphScanner = client.createScanner(graphTable, auths)) {
      return findSimilarVertexIds(graphScanner, queryVector, topK, threshold);
    }
  }

  /**
   * Full RAG pipeline: vector similarity search followed by graph neighborhood expansion.
   *
   * @param client AccumuloClient for creating scanners
   * @param table the graph table name
   * @param auths authorizations for visibility filtering
   * @param queryVector the query embedding
   * @param topK number of similar vertices to find
   * @param threshold minimum similarity score
   * @param edgeType edge type to traverse (null for all)
   * @param depth graph traversal depth
   * @return map from similar vertex IDs to their graph neighborhoods
   */
  public static Map<String,List<String>> vectorGraphQuery(AccumuloClient client, String table,
      Authorizations auths, float[] queryVector, int topK, float threshold, String edgeType,
      int depth) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    // Step 1: Find similar vertices via vidx seek (with full-scan fallback)
    List<String> similarVertices =
        findSimilarVertexIds(client, table, auths, queryVector, topK, threshold);

    if (similarVertices.isEmpty()) {
      return new HashMap<>();
    }

    // Step 2: Expand graph neighborhoods
    try (Scanner graphScanner = client.createScanner(table, auths)) {
      return expandNeighborhoods(graphScanner, similarVertices, edgeType, depth);
    }
  }

  /**
   * Convenience method to add a vertex with both properties and an embedding in one batch.
   *
   * @param writer batch writer for the graph table
   * @param id vertex identifier
   * @param label vertex label
   * @param embedding vector embedding
   * @param properties vertex properties
   * @param visibility column visibility expression
   */
  public static void addVertexWithEmbedding(BatchWriter writer, String id, String label,
      float[] embedding, Map<String,String> properties, ColumnVisibility visibility)
      throws Exception {
    Mutation vertexMut = GraphMutations.addVertex(id, label, properties, visibility);
    Mutation embeddingMut = GraphMutations.addVertexEmbedding(id, embedding, visibility);
    writer.addMutation(vertexMut);
    writer.addMutation(embeddingMut);
  }

  /**
   * Topological Vector Expansion: finds vector-similar seeds AND scores their graph neighbors by
   * combining similarity and connectivity.
   *
   * <p>
   * Phase 1: Vector search to find top-K seeds. Phase 2: For each seed, scan edges to find
   * neighbors. Phase 3: For each neighbor, read their embedding and compute similarity. Phase 4:
   * Score = alpha * vectorSimilarity + (1-alpha) * connectionScore. Phase 5: Return merged,
   * re-ranked results.
   *
   * <p>
   * This solves the "missing context" problem where relevant data is one hop away from the vector
   * match but does not directly match the query embedding.
   *
   * @param client AccumuloClient for table access
   * @param graphTable the graph table name
   * @param auths authorizations for visibility filtering
   * @param queryVector the query embedding
   * @param topK number of results to return
   * @param threshold minimum similarity score for seed search
   * @param alpha weight for combining vector similarity and connectivity (0=pure connectivity,
   *        1=pure similarity)
   * @param expansionDepth graph traversal depth for neighbor expansion
   * @return list of result maps with vertex_id, score, source, and optionally connected_seeds
   */
  public static List<Map<String,Object>> topologicalVectorExpansion(AccumuloClient client,
      String graphTable, Authorizations auths, float[] queryVector, int topK, float threshold,
      float alpha, int expansionDepth)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    // Phase 1: Vector search for seeds
    List<String> seeds =
        findSimilarVertexIds(client, graphTable, auths, queryVector, topK, threshold);

    if (seeds.isEmpty()) {
      return List.of();
    }

    // Phase 2: Get seed similarity scores and expand to neighbors
    Map<String,Float> seedScores = new HashMap<>();
    Map<String,Set<String>> connections = new HashMap<>();

    // Re-scan seeds to get their actual similarity scores
    try (Scanner scanner = client.createScanner(graphTable, auths)) {
      for (String seed : seeds) {
        scanner.clearColumns();
        scanner.clearScanIterators();
        scanner.setRange(Range.exact(seed));
        scanner.fetchColumn(new Text(GraphSchema.VERTEX_COLFAM),
            new Text(GraphSchema.EMBEDDING_COLQUAL));

        for (var entry : scanner) {
          float[] embedding = parseEmbeddingFromValue(entry.getValue());
          if (embedding != null && embedding.length == queryVector.length) {
            float similarity = cosineSimilarity(queryVector, embedding);
            seedScores.put(seed, similarity);
          }
        }
      }
    }

    // For each seed, get neighbors via edge traversal
    try (Scanner scanner = client.createScanner(graphTable, auths)) {
      for (String seed : seeds) {
        scanner.clearColumns();
        scanner.clearScanIterators();
        scanner.setRange(Range.exact(seed));

        for (var entry : scanner) {
          String cf = entry.getKey().getColumnFamily().toString();
          if (cf.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
            String neighbor = entry.getKey().getColumnQualifier().toString();
            connections.computeIfAbsent(neighbor, k -> new HashSet<>()).add(seed);
          }
        }
      }
    }

    // Phase 3: Score neighbors by reading their embeddings
    Map<String,Float> neighborScores = new HashMap<>();
    Set<String> seedSet = new HashSet<>(seeds);

    try (Scanner scanner = client.createScanner(graphTable, auths)) {
      for (String neighbor : connections.keySet()) {
        if (seedSet.contains(neighbor)) {
          continue; // already scored as seed
        }

        scanner.clearColumns();
        scanner.clearScanIterators();
        scanner.setRange(Range.exact(neighbor));
        scanner.fetchColumn(new Text(GraphSchema.VERTEX_COLFAM),
            new Text(GraphSchema.EMBEDDING_COLQUAL));

        for (var entry : scanner) {
          float[] embedding = parseEmbeddingFromValue(entry.getValue());
          if (embedding != null && embedding.length == queryVector.length) {
            float similarity = cosineSimilarity(queryVector, embedding);
            // Connection bonus: how many seeds connect to this neighbor
            float connectionScore = (float) connections.get(neighbor).size() / seeds.size();
            float combinedScore = alpha * similarity + (1 - alpha) * connectionScore;
            neighborScores.put(neighbor, combinedScore);
          }
        }
      }
    }

    // Phase 4: Merge seeds + neighbors, sort by score
    List<Map<String,Object>> results = new ArrayList<>();
    for (String seed : seeds) {
      float score = seedScores.getOrDefault(seed, 0f);
      Map<String,Object> entry = new HashMap<>();
      entry.put("vertex_id", seed);
      entry.put("score", score);
      entry.put("source", "vector");
      results.add(entry);
    }
    for (var entry : neighborScores.entrySet()) {
      Map<String,Object> result = new HashMap<>();
      result.put("vertex_id", entry.getKey());
      result.put("score", entry.getValue());
      result.put("source", "expansion");
      result.put("connected_seeds", new ArrayList<>(connections.get(entry.getKey())));
      results.add(result);
    }

    results.sort((a, b) -> Float.compare(((Number) b.get("score")).floatValue(),
        ((Number) a.get("score")).floatValue()));

    return results.subList(0, Math.min(results.size(), topK));
  }

  /**
   * Parses an embedding from raw value bytes using BIG_ENDIAN float encoding, matching the format
   * used by VectorIterator for RFile round-trip safety.
   */
  private static float[] parseEmbeddingFromValue(Value value) {
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

  private static String vectorToString(float[] vector) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(vector[i]);
    }
    return sb.toString();
  }
}
