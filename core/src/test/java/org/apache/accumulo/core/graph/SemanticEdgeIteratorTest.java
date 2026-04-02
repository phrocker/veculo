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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class SemanticEdgeIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final String SIMILAR_EDGE_CF =
      GraphSchema.edgeColumnFamily(GraphSchema.SIMILAR_TO_EDGE_TYPE);
  private static final String SIMILAR_INVERSE_CF =
      GraphSchema.inverseEdgeColumnFamily(GraphSchema.SIMILAR_TO_EDGE_TYPE);

  private List<Key> collectKeys(SemanticEdgeIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testSimilarVectorsGetEdges() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // v1 and v2 have very similar vectors; v3 is different
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label", "public"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "public"),
        Value.newVector(new float[] {0.99f, 0.1f, 0.0f}));
    data.put(new Key("v3", "V", "_label", "public"), new Value("doc3"));
    data.put(new Key("v3", "V", "_embedding", "public"),
        Value.newVector(new float[] {0.0f, 0.0f, 1.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SemanticEdgeIterator.SIMILARITY_THRESHOLD, "0.85");

    SemanticEdgeIterator iter = new SemanticEdgeIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // Should have SIMILAR_TO edges only between v1 and v2
    long forwardEdges =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF)).count();
    long inverseEdges = keys.stream()
        .filter(k -> k.getColumnFamily().toString().equals(SIMILAR_INVERSE_CF)).count();

    assertTrue(forwardEdges > 0, "Should have at least one forward SIMILAR_TO edge");
    assertTrue(inverseEdges > 0, "Should have at least one inverse SIMILAR_TO edge");

    // v3 should not have edges to v1 or v2 (too different)
    assertFalse(keys.stream().anyMatch(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF)
        && k.getRow().toString().equals("v3") && k.getColumnQualifier().toString().equals("v1")));
    assertFalse(keys.stream().anyMatch(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF)
        && k.getRow().toString().equals("v3") && k.getColumnQualifier().toString().equals("v2")));
  }

  @Test
  public void testEdgeHasCorrectCompoundVisibility() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "alpha"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "alpha"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label", "beta"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "beta"),
        Value.newVector(new float[] {0.99f, 0.1f, 0.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SemanticEdgeIterator.SIMILARITY_THRESHOLD, "0.85");

    SemanticEdgeIterator iter = new SemanticEdgeIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // Find a SIMILAR_TO edge and check its visibility
    Key edgeKey = keys.stream().filter(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF))
        .findFirst().orElse(null);
    assertNotNull(edgeKey, "Should have a SIMILAR_TO edge");

    String vis = edgeKey.getColumnVisibility().toString();
    // Should be compound: (alpha)&(beta) or (beta)&(alpha)
    assertTrue(vis.contains("alpha") && vis.contains("beta"),
        "Edge visibility should contain both source visibilities: " + vis);
  }

  @Test
  public void testMaxEdgesPerVertexIsRespected() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // Create 4 vertices all with very similar vectors
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label", "public"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "public"),
        Value.newVector(new float[] {0.99f, 0.05f, 0.0f}));
    data.put(new Key("v3", "V", "_label", "public"), new Value("doc3"));
    data.put(new Key("v3", "V", "_embedding", "public"),
        Value.newVector(new float[] {0.98f, 0.1f, 0.0f}));
    data.put(new Key("v4", "V", "_label", "public"), new Value("doc4"));
    data.put(new Key("v4", "V", "_embedding", "public"),
        Value.newVector(new float[] {0.97f, 0.15f, 0.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SemanticEdgeIterator.SIMILARITY_THRESHOLD, "0.85");
    options.put(SemanticEdgeIterator.MAX_EDGES_PER_VERTEX, "1");

    SemanticEdgeIterator iter = new SemanticEdgeIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // Each vertex should have at most 1 forward edge
    for (String vertexId : new String[] {"v1", "v2", "v3", "v4"}) {
      long edgesFromVertex =
          keys.stream().filter(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF)
              && k.getRow().toString().equals(vertexId)).count();
      assertTrue(edgesFromVertex <= 1,
          "Vertex " + vertexId + " should have at most 1 edge, but had " + edgesFromVertex);
    }
  }

  @Test
  public void testAllSimilarVectorsAboveThreshold() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // Two vertices with identical vectors
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label", "public"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SemanticEdgeIterator.SIMILARITY_THRESHOLD, "0.99");

    SemanticEdgeIterator iter = new SemanticEdgeIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // Identical vectors should have similarity of 1.0, exceeding 0.99 threshold
    long edgeCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF)
            || k.getColumnFamily().toString().equals(SIMILAR_INVERSE_CF)).count();
    assertTrue(edgeCount > 0, "Identical vectors should produce SIMILAR_TO edges");
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SemanticEdgeIterator.SIMILARITY_THRESHOLD, "0.85");

    SemanticEdgeIterator iter = new SemanticEdgeIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // All original entries should still be present
    assertTrue(keys.stream().anyMatch(
        k -> k.getRow().toString().equals("v1") && k.getColumnFamily().toString().equals("V")
            && k.getColumnQualifier().toString().equals("_label")));
    assertTrue(keys.stream().anyMatch(
        k -> k.getRow().toString().equals("v1") && k.getColumnFamily().toString().equals("V")
            && k.getColumnQualifier().toString().equals("_embedding")));
  }

  @Test
  public void testSameVisibilityNotDuplicated() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "secret"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "secret"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label", "secret"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "secret"),
        Value.newVector(new float[] {0.99f, 0.1f, 0.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SemanticEdgeIterator.SIMILARITY_THRESHOLD, "0.85");

    SemanticEdgeIterator iter = new SemanticEdgeIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // When both vertices have the same visibility, the compound should just be "secret"
    Key edgeKey = keys.stream().filter(k -> k.getColumnFamily().toString().equals(SIMILAR_EDGE_CF))
        .findFirst().orElse(null);
    assertNotNull(edgeKey);
    assertEquals("secret", edgeKey.getColumnVisibility().toString());
  }
}
