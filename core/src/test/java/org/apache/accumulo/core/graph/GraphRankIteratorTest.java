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

public class GraphRankIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private Map<Key,Value> collectEntries(GraphRankIterator iter) throws IOException {
    Map<Key,Value> entries = new TreeMap<>();
    while (iter.hasTop()) {
      entries.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
      iter.next();
    }
    return entries;
  }

  private List<Key> collectKeys(GraphRankIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  /**
   * Build a simple cycle graph: A -> B -> C -> A
   */
  private TreeMap<Key,Value> buildCycleGraph() {
    TreeMap<Key,Value> data = new TreeMap<>();

    // Vertex A
    data.put(new Key("A", "V", "_label", "public"), new Value("nodeA"));
    data.put(new Key("A", "E_links", "B"), new Value("{}"));

    // Vertex B
    data.put(new Key("B", "V", "_label", "public"), new Value("nodeB"));
    data.put(new Key("B", "E_links", "C"), new Value("{}"));

    // Vertex C
    data.put(new Key("C", "V", "_label", "public"), new Value("nodeC"));
    data.put(new Key("C", "E_links", "A"), new Value("{}"));

    return data;
  }

  @Test
  public void testAllVerticesGetRankProperty() throws IOException {
    TreeMap<Key,Value> data = buildCycleGraph();

    GraphRankIterator iter = new GraphRankIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // Each vertex should get a _rank property
    for (String vertexId : new String[] {"A", "B", "C"}) {
      boolean hasRank =
          entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals(vertexId)
              && k.getColumnQualifier().toString().equals(GraphSchema.RANK_COLQUAL));
      assertTrue(hasRank, "Vertex " + vertexId + " should have a _rank property");
    }
  }

  @Test
  public void testRanksSumToApproximatelyOne() throws IOException {
    TreeMap<Key,Value> data = buildCycleGraph();

    GraphRankIterator iter = new GraphRankIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    double totalRank = 0.0;
    for (Map.Entry<Key,Value> entry : entries.entrySet()) {
      if (entry.getKey().getColumnQualifier().toString().equals(GraphSchema.RANK_COLQUAL)) {
        double rank = Double.parseDouble(new String(entry.getValue().get()));
        totalRank += rank;
        assertTrue(rank > 0, "Each rank should be positive");
      }
    }

    // PageRank values should sum to approximately 1.0
    assertEquals(1.0, totalRank, 0.05,
        "Ranks should sum to approximately 1.0, but got " + totalRank);
  }

  @Test
  public void testDampingFactorOption() throws IOException {
    TreeMap<Key,Value> data = buildCycleGraph();

    Map<String,String> options = new HashMap<>();
    options.put(GraphRankIterator.DAMPING_FACTOR, "0.5");

    GraphRankIterator iter = new GraphRankIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // With a different damping factor, all vertices should still get ranks
    long rankCount = entries.keySet().stream()
        .filter(k -> k.getColumnQualifier().toString().equals(GraphSchema.RANK_COLQUAL)).count();
    assertEquals(3, rankCount, "All 3 vertices should have _rank property");

    // Ranks should still sum to approximately 1.0
    double totalRank = 0.0;
    for (Map.Entry<Key,Value> entry : entries.entrySet()) {
      if (entry.getKey().getColumnQualifier().toString().equals(GraphSchema.RANK_COLQUAL)) {
        totalRank += Double.parseDouble(new String(entry.getValue().get()));
      }
    }
    assertEquals(1.0, totalRank, 0.05, "Ranks with custom damping factor should still sum to ~1.0");
  }

  @Test
  public void testSymmetricGraphProducesEqualRanks() throws IOException {
    TreeMap<Key,Value> data = buildCycleGraph();

    Map<String,String> options = new HashMap<>();
    options.put(GraphRankIterator.MAX_ITERATIONS, "100");
    options.put(GraphRankIterator.CONVERGENCE_THRESHOLD, "0.000001");

    GraphRankIterator iter = new GraphRankIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // In a symmetric cycle A -> B -> C -> A, all ranks should be equal
    List<Double> ranks = new ArrayList<>();
    for (Map.Entry<Key,Value> entry : entries.entrySet()) {
      if (entry.getKey().getColumnQualifier().toString().equals(GraphSchema.RANK_COLQUAL)) {
        ranks.add(Double.parseDouble(new String(entry.getValue().get())));
      }
    }

    assertEquals(3, ranks.size());
    // All ranks should be approximately equal (1/3)
    for (double rank : ranks) {
      assertEquals(1.0 / 3.0, rank, 0.01, "In a symmetric cycle, each rank should be ~1/3");
    }
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = buildCycleGraph();

    GraphRankIterator iter = new GraphRankIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // All original entries should be present
    for (Key originalKey : data.keySet()) {
      assertTrue(keys.stream().anyMatch(k -> k.equals(originalKey)),
          "Original entry should pass through: " + originalKey);
    }
  }

  @Test
  public void testEdgeTypeFiltering() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // A has two edge types: "links" (to B) and "other" (to C)
    data.put(new Key("A", "V", "_label"), new Value("nodeA"));
    data.put(new Key("A", "E_links", "B"), new Value("{}"));
    data.put(new Key("A", "E_other", "C"), new Value("{}"));
    data.put(new Key("B", "V", "_label"), new Value("nodeB"));
    data.put(new Key("B", "E_links", "A"), new Value("{}"));
    data.put(new Key("C", "V", "_label"), new Value("nodeC"));

    Map<String,String> options = new HashMap<>();
    options.put(GraphRankIterator.EDGE_TYPE, "links");

    GraphRankIterator iter = new GraphRankIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // All 3 vertices should get ranks (they're all discovered from V entries)
    long rankCount = entries.keySet().stream()
        .filter(k -> k.getColumnQualifier().toString().equals(GraphSchema.RANK_COLQUAL)).count();
    assertEquals(3, rankCount, "All vertices should get _rank properties");
  }
}
