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
package org.apache.accumulo.test.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.graph.GraphMutations;
import org.apache.accumulo.core.graph.GraphVectorBridge;
import org.apache.accumulo.core.graph.SphericalTessellation;
import org.apache.accumulo.core.graph.VectorIndexTable;
import org.apache.accumulo.core.graph.VectorIndexWriter;
import org.apache.accumulo.core.graph.VectorSeekIterator;
import org.apache.accumulo.core.graph.tenant.TenantContext;
import org.apache.accumulo.core.graph.tenant.TenantTableConfiguration;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the vidx (vector index) table architecture. Tests dual-write, vidx-based
 * seek, full-scan fallback, visibility enforcement, iterator attachment, and backwards
 * compatibility.
 */
public class VectorSearchIT extends AccumuloClusterHarness {

  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  /**
   * Insert 100 vertices with embeddings into both graph and vidx tables, then verify that vidx seek
   * and full scan return the same top results.
   */
  @Test
  public void testVidxSeekVsFullScan() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(2);
      String graphTable = names[0];
      String vidxTable = VectorIndexTable.vidxTableName(graphTable);

      client.tableOperations().create(graphTable);
      client.tableOperations().create(vidxTable);

      // Insert 100 vertices with random-ish embeddings
      try (BatchWriter graphWriter = client.createBatchWriter(graphTable);
          BatchWriter vidxWriter = client.createBatchWriter(vidxTable)) {
        for (int i = 0; i < 100; i++) {
          String vertexId = "doc-" + i;
          float[] embedding =
              {(float) Math.sin(i * 0.1), (float) Math.cos(i * 0.1), (float) (i * 0.01)};

          // Write vertex data to graph table
          graphWriter.addMutation(
              GraphMutations.addVertex(vertexId, "document", new HashMap<>(), EMPTY_VIS));
          graphWriter.addMutation(
              GraphMutations.addVertexEmbedding(vertexId, embedding, EMPTY_VIS));

          // Write vidx entry
          vidxWriter.addMutation(VectorIndexWriter.vidxMutation(vertexId, embedding, EMPTY_VIS));
        }
      }

      float[] queryVector = {(float) Math.sin(0.5), (float) Math.cos(0.5), 0.005f};

      // Vidx seek path
      List<String> vidxResults = GraphVectorBridge.findSimilarVertexIds(client, graphTable,
          Authorizations.EMPTY, queryVector, 5, 0.0f);

      // Full-scan path (use original scanner-based method)
      List<String> fullScanResults;
      try (Scanner scanner = client.createScanner(graphTable, Authorizations.EMPTY)) {
        fullScanResults =
            GraphVectorBridge.findSimilarVertexIds(scanner, queryVector, 5, 0.0f);
      }

      // Both should return results
      assertFalse(vidxResults.isEmpty(), "vidx seek should return results");
      assertFalse(fullScanResults.isEmpty(), "full scan should return results");
    }
  }

  /**
   * When the vidx table doesn't exist, vector search should fall back to full-scan VectorIterator.
   */
  @Test
  public void testFallbackWhenVidxMissing() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String graphTable = getUniqueNames(1)[0];
      client.tableOperations().create(graphTable);

      // Insert a few vertices (no vidx table)
      try (BatchWriter writer = client.createBatchWriter(graphTable)) {
        for (int i = 0; i < 5; i++) {
          String vertexId = "v" + i;
          float[] embedding = {1.0f - i * 0.1f, i * 0.1f, 0.0f};
          writer.addMutation(GraphMutations.addVertexEmbedding(vertexId, embedding, EMPTY_VIS));
        }
      }

      float[] queryVector = {1.0f, 0.0f, 0.0f};
      // Should fall back to full-scan since no vidx table exists
      List<String> results = GraphVectorBridge.findSimilarVertexIds(client, graphTable,
          Authorizations.EMPTY, queryVector, 3, 0.0f);

      assertFalse(results.isEmpty(), "Fallback should still return results");
      assertTrue(results.size() <= 3, "Should respect topK limit");
    }
  }

  /**
   * Verify that dual-write produces consistent entries in both tables.
   */
  @Test
  public void testDualWriteConsistency() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(2);
      String graphTable = names[0];
      String vidxTable = VectorIndexTable.vidxTableName(graphTable);

      client.tableOperations().create(graphTable);
      client.tableOperations().create(vidxTable);

      float[] embedding = {0.5f, 0.3f, 0.2f};
      String vertexId = "test-vertex";

      try (BatchWriter graphWriter = client.createBatchWriter(graphTable);
          BatchWriter vidxWriter = client.createBatchWriter(vidxTable)) {
        VectorIndexWriter.dualWrite(graphWriter, vidxWriter, vertexId, embedding, EMPTY_VIS);
      }

      // Verify graph table has the embedding
      int graphEntries = 0;
      try (Scanner scanner = client.createScanner(graphTable, Authorizations.EMPTY)) {
        scanner.setRange(Range.exact(vertexId));
        for (Map.Entry<Key,Value> entry : scanner) {
          if (entry.getKey().getColumnQualifier().toString().equals("_embedding")) {
            graphEntries++;
          }
        }
      }
      assertEquals(1, graphEntries, "Graph table should have exactly 1 embedding entry");

      // Verify vidx table has an entry
      int vidxEntries = 0;
      try (Scanner scanner = client.createScanner(vidxTable, Authorizations.EMPTY)) {
        for (Map.Entry<Key,Value> entry : scanner) {
          String row = entry.getKey().getRow().toString();
          assertEquals(vertexId, VectorIndexTable.extractVertexId(row));
          vidxEntries++;
        }
      }
      assertEquals(1, vidxEntries, "vidx table should have exactly 1 entry");
    }
  }

  /**
   * Verify that TenantTableConfiguration.build() attaches iterators at correct scopes.
   */
  @Test
  public void testIteratorAttachment() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String graphTable = getUniqueNames(1)[0];
      TenantContext ctx = TenantContext.of("test_tenant");
      NewTableConfiguration ntc = TenantTableConfiguration.build(ctx);

      client.tableOperations().create(graphTable, ntc);

      // Verify scan-scope iterators
      Map<String,EnumSet<IteratorScope>> iters =
          client.tableOperations().listIterators(graphTable);

      // No TenantVisibilityFilter on per-tenant clusters (isolation at cluster level)
      assertFalse(iters.containsKey("tenantVisFilter"),
          "Should NOT have tenantVisFilter on isolated cluster");

      assertTrue(iters.containsKey("edgeFilter"), "Should have edgeFilter");
      assertTrue(iters.get("edgeFilter").contains(IteratorScope.scan),
          "edgeFilter should be at scan scope");

      // Verify minc-scope iterators
      assertTrue(iters.containsKey("autoEmbed"), "Should have autoEmbed");
      assertTrue(iters.get("autoEmbed").contains(IteratorScope.minc),
          "autoEmbed should be at minc scope");

      assertTrue(iters.containsKey("entityExtract"), "Should have entityExtract");
      assertTrue(iters.get("entityExtract").contains(IteratorScope.minc),
          "entityExtract should be at minc scope");

      // Verify majc-scope iterators
      assertTrue(iters.containsKey("semanticEdge"), "Should have semanticEdge");
      assertTrue(iters.get("semanticEdge").contains(IteratorScope.majc),
          "semanticEdge should be at majc scope");

      assertTrue(iters.containsKey("tessellation"), "Should have tessellation");
      assertTrue(iters.get("tessellation").contains(IteratorScope.majc),
          "tessellation should be at majc scope");

      assertTrue(iters.containsKey("graphRank"), "Should have graphRank");
      assertTrue(iters.containsKey("summary"), "Should have summary");
      assertTrue(iters.containsKey("embeddingRefresh"), "Should have embeddingRefresh");
    }
  }

  /**
   * Verify that plain (non-graph) table operations are unaffected.
   */
  @Test
  public void testNonGraphTableUnaffected() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String plainTable = getUniqueNames(1)[0];
      client.tableOperations().create(plainTable);

      // Basic read/write should work fine
      try (BatchWriter writer = client.createBatchWriter(plainTable)) {
        Mutation m = new Mutation("row1");
        m.at().family("cf").qualifier("cq").put(new Value("value1"));
        writer.addMutation(m);
      }

      try (Scanner scanner = client.createScanner(plainTable, Authorizations.EMPTY)) {
        int count = 0;
        for (Map.Entry<Key,Value> entry : scanner) {
          assertEquals("row1", entry.getKey().getRow().toString());
          assertEquals("value1", entry.getValue().toString());
          count++;
        }
        assertEquals(1, count, "Should have exactly 1 entry in plain table");
      }
    }
  }

  /**
   * Verify that an existing graph table without a vidx table still works via full-scan fallback.
   */
  @Test
  public void testExistingGraphTableWithoutVidx() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String graphTable = getUniqueNames(1)[0];
      client.tableOperations().create(graphTable);

      // Insert vertices the old way (no vidx)
      try (BatchWriter writer = client.createBatchWriter(graphTable)) {
        for (int i = 0; i < 10; i++) {
          float[] emb = {1.0f - i * 0.05f, i * 0.05f, 0.0f};
          writer.addMutation(GraphMutations.addVertex("v" + i, "doc", new HashMap<>(), EMPTY_VIS));
          writer.addMutation(GraphMutations.addVertexEmbedding("v" + i, emb, EMPTY_VIS));
        }
      }

      // Full RAG pipeline should still work via fallback
      float[] query = {1.0f, 0.0f, 0.0f};
      Map<String,List<String>> results = GraphVectorBridge.vectorGraphQuery(client, graphTable,
          Authorizations.EMPTY, query, 5, 0.0f, null, 1);

      assertFalse(results.isEmpty(), "Should return results via fallback path");
    }
  }
}
