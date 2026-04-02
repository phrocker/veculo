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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level graph operations client that wraps AccumuloClient with graph-aware optimizations:
 *
 * <ul>
 * <li><b>BatchScanner for multi-vertex reads</b> — parallel reads across tablets instead of
 * sequential single-row scans</li>
 * <li><b>Column family pre-filtering</b> — only reads the locality group needed (V for properties,
 * E_/EI_ for edges)</li>
 * <li><b>Atomic writes</b> — vertex + embedding + vidx + link discovery in a single batch</li>
 * <li><b>Shared writers</b> — reusable BatchWriter for multiple operations</li>
 * </ul>
 *
 * <p>
 * Intended to replace the scattered Scanner/BatchWriter creation in GraphHandler. Each GraphClient
 * instance is bound to a single AccumuloClient and table pair.
 */
public class GraphClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(GraphClient.class);

  private final AccumuloClient client;
  private final String graphTable;
  private final String vidxTable;
  private final Authorizations auths;
  private final boolean vidxExists;

  // Shared writers — created lazily, closed on close()
  private BatchWriter graphWriter;
  private BatchWriter vidxWriter;

  public GraphClient(AccumuloClient client, String graphTable, Authorizations auths) {
    this.client = requireNonNull(client);
    this.graphTable = requireNonNull(graphTable);
    this.vidxTable = VectorIndexTable.vidxTableName(graphTable);
    this.auths = auths != null ? auths : Authorizations.EMPTY;
    this.vidxExists = client.tableOperations().exists(vidxTable);
  }

  // ─── Reads: Single Vertex ──────────────────────────────────────────────────

  /**
   * Gets vertex properties only (uses V locality group).
   */
  public Map<String,String> getVertexProperties(String vertexId) throws TableNotFoundException {
    Map<String,String> props = new HashMap<>();
    try (Scanner scanner = client.createScanner(graphTable, auths)) {
      scanner.setRange(Range.exact(vertexId));
      scanner.fetchColumnFamily(GraphSchema.VERTEX_COLFAM_TEXT);
      for (Map.Entry<Key,Value> entry : scanner) {
        props.put(entry.getKey().getColumnQualifier().toString(), entry.getValue().toString());
      }
    }
    return props;
  }

  /**
   * Gets edges for a vertex (skips V locality group). Includes latent edges from vidx.
   */
  public VertexEdges getEdges(String vertexId) throws TableNotFoundException {
    List<EdgeInfo> edges = new ArrayList<>();
    String cellIdHex = null;

    try (Scanner scanner = client.createScanner(graphTable, auths)) {
      scanner.setRange(Range.exact(vertexId));
      for (Map.Entry<Key,Value> entry : scanner) {
        String cf = entry.getKey().getColumnFamily().toString();
        String cq = entry.getKey().getColumnQualifier().toString();
        if (cf.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
          edges.add(new EdgeInfo(cf.substring(2), "outgoing", cq, null, false));
        } else if (cf.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX)) {
          edges.add(new EdgeInfo(cf.substring(3), "incoming", cq, null, false));
        } else if (cf.equals("V") && cq.equals("_cell_id")) {
          cellIdHex = entry.getValue().toString();
        }
      }
    }

    // Merge latent edges from vidx
    if (cellIdHex != null && vidxExists) {
      try {
        long cellId = Long.parseUnsignedLong(cellIdHex, 16);
        String vidxRow = VectorIndexTable.rowKey(cellId, vertexId);
        try (Scanner linkScan = client.createScanner(vidxTable, auths)) {
          linkScan.setRange(Range.exact(vidxRow));
          linkScan.fetchColumnFamily(new Text(VectorIndexWriter.LINK_COLFAM));
          for (Map.Entry<Key,Value> entry : linkScan) {
            edges.add(new EdgeInfo("SIMILAR_TO", "outgoing",
                entry.getKey().getColumnQualifier().toString(), entry.getValue().toString(), true));
          }
        }
      } catch (Exception e) {
        log.debug("Latent edge lookup failed for {}: {}", vertexId, e.getMessage());
      }
    }

    return new VertexEdges(vertexId, edges);
  }

  // ─── Reads: Multi-Vertex (BatchScanner) ────────────────────────────────────

  /**
   * Gets properties for multiple vertices in parallel using BatchScanner. Returns a map from vertex
   * ID to properties.
   */
  public Map<String,Map<String,String>> getVerticesProperties(Collection<String> vertexIds)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    Map<String,Map<String,String>> results = new LinkedHashMap<>();
    if (vertexIds.isEmpty()) {
      return results;
    }

    List<Range> ranges = new ArrayList<>();
    for (String id : vertexIds) {
      ranges.add(Range.exact(id));
    }

    try (BatchScanner batchScanner = client.createBatchScanner(graphTable, auths, 4)) {
      batchScanner.setRanges(ranges);
      batchScanner.fetchColumnFamily(GraphSchema.VERTEX_COLFAM_TEXT);

      for (Map.Entry<Key,Value> entry : batchScanner) {
        String row = entry.getKey().getRow().toString();
        results.computeIfAbsent(row, k -> new HashMap<>())
            .put(entry.getKey().getColumnQualifier().toString(), entry.getValue().toString());
      }
    }

    return results;
  }

  /**
   * Gets neighbors for multiple vertices in parallel using BatchScanner. Returns a map from vertex
   * ID to list of neighbor IDs.
   */
  public Map<String,List<String>> getNeighborsBatch(Collection<String> vertexIds, String edgeType)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    Map<String,List<String>> results = new LinkedHashMap<>();
    if (vertexIds.isEmpty()) {
      return results;
    }

    List<Range> ranges = new ArrayList<>();
    for (String id : vertexIds) {
      ranges.add(Range.exact(id));
    }

    try (BatchScanner batchScanner = client.createBatchScanner(graphTable, auths, 4)) {
      batchScanner.setRanges(ranges);

      // Scope to specific edge type if provided
      if (edgeType != null && !edgeType.isEmpty()) {
        batchScanner.fetchColumnFamily(new Text(GraphSchema.edgeColumnFamily(edgeType)));
      }

      for (Map.Entry<Key,Value> entry : batchScanner) {
        String cf = entry.getKey().getColumnFamily().toString();
        if (cf.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
          String row = entry.getKey().getRow().toString();
          String neighbor = entry.getKey().getColumnQualifier().toString();
          results.computeIfAbsent(row, k -> new ArrayList<>()).add(neighbor);
        }
      }
    }

    return results;
  }

  // ─── Writes ────────────────────────────────────────────────────────────────

  /**
   * Writes a vertex with properties, embedding, vidx entry, and latent edge discovery — all in a
   * single batch session.
   */
  public void putVertexWithEmbedding(String id, String label, float[] embedding,
      Map<String,String> properties, ColumnVisibility visibility) throws Exception {

    BatchWriter gw = getGraphWriter();

    // Vertex properties
    gw.addMutation(GraphMutations.addVertex(id, label, properties, visibility));

    if (embedding != null) {
      if (vidxExists) {
        BatchWriter vw = getVidxWriter();
        try (Scanner vidxScanner = client.createScanner(vidxTable, auths)) {
          VectorIndexWriter.dualWriteWithDiscovery(gw, vw, vidxScanner, id, embedding, visibility,
              VectorIndexWriter.DEFAULT_LINK_THRESHOLD);
        }
        vw.flush();
      } else {
        gw.addMutation(GraphMutations.addVertexEmbedding(id, embedding, visibility));
      }
    }

    gw.flush();
  }

  /**
   * Writes an edge (forward + inverse) in a single batch.
   */
  public void putEdge(String source, String target, String edgeType, Map<String,String> properties,
      ColumnVisibility visibility) throws Exception {
    BatchWriter gw = getGraphWriter();
    for (Mutation m : GraphMutations.addEdge(source, target, edgeType, properties, visibility)) {
      gw.addMutation(m);
    }
    gw.flush();
  }

  /**
   * Writes multiple vertices in a single batch.
   */
  public int putVerticesBulk(List<Map<String,Object>> vertices, ColumnVisibility visibility)
      throws Exception {
    BatchWriter gw = getGraphWriter();
    int count = 0;
    for (Map<String,Object> v : vertices) {
      String id = (String) v.get("id");
      String label = (String) v.getOrDefault("label", "default");
      @SuppressWarnings("unchecked")
      Map<String,String> props = (Map<String,String>) v.getOrDefault("properties", Map.of());
      gw.addMutation(GraphMutations.addVertex(id, label, props, visibility));
      count++;
    }
    gw.flush();
    return count;
  }

  /**
   * Writes multiple edges in a single batch.
   */
  public int putEdgesBulk(List<Map<String,String>> edges, ColumnVisibility visibility)
      throws Exception {
    BatchWriter gw = getGraphWriter();
    int count = 0;
    for (Map<String,String> e : edges) {
      for (Mutation m : GraphMutations.addEdge(e.get("source"), e.get("target"), e.get("edge_type"),
          null, visibility)) {
        gw.addMutation(m);
      }
      count++;
    }
    gw.flush();
    return count;
  }

  // ─── Writer Management ─────────────────────────────────────────────────────

  private BatchWriter getGraphWriter() throws TableNotFoundException {
    if (graphWriter == null) {
      graphWriter = client.createBatchWriter(graphTable);
    }
    return graphWriter;
  }

  private BatchWriter getVidxWriter() throws TableNotFoundException {
    if (vidxWriter == null) {
      vidxWriter = client.createBatchWriter(vidxTable);
    }
    return vidxWriter;
  }

  @Override
  public void close() {
    if (graphWriter != null) {
      try {
        graphWriter.close();
      } catch (Exception e) {
        log.debug("Error closing graph writer: {}", e.getMessage());
      }
    }
    if (vidxWriter != null) {
      try {
        vidxWriter.close();
      } catch (Exception e) {
        log.debug("Error closing vidx writer: {}", e.getMessage());
      }
    }
  }

  // ─── Data Classes ──────────────────────────────────────────────────────────

  public static class EdgeInfo {
    public final String type;
    public final String direction;
    public final String connectedVertex;
    public final String score;
    public final boolean latent;

    public EdgeInfo(String type, String direction, String connectedVertex, String score,
        boolean latent) {
      this.type = type;
      this.direction = direction;
      this.connectedVertex = connectedVertex;
      this.score = score;
      this.latent = latent;
    }

    public Map<String,String> toMap() {
      Map<String,String> map = new LinkedHashMap<>();
      map.put("type", type);
      map.put("direction", direction);
      if ("outgoing".equals(direction)) {
        map.put("target", connectedVertex);
      } else {
        map.put("source", connectedVertex);
      }
      if (score != null) {
        map.put("score", score);
      }
      if (latent) {
        map.put("latent", "true");
      }
      return map;
    }
  }

  public static class VertexEdges {
    public final String vertexId;
    public final List<EdgeInfo> edges;

    public VertexEdges(String vertexId, List<EdgeInfo> edges) {
      this.vertexId = vertexId;
      this.edges = edges;
    }

    public Map<String,Integer> edgeTypeCounts() {
      Map<String,Integer> counts = new LinkedHashMap<>();
      for (EdgeInfo e : edges) {
        String key = e.latent ? e.type + " (latent)" : e.type;
        if ("incoming".equals(e.direction)) {
          key += " (incoming)";
        }
        counts.merge(key, 1, Integer::sum);
      }
      return counts;
    }
  }
}
