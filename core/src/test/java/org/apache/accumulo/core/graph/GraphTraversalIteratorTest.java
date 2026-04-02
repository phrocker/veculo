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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class GraphTraversalIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  /**
   * Builds a small test graph:
   *
   * <pre>
   * A --knows--> B --knows--> C
   * A --knows--> D --knows--> E
   * </pre>
   */
  private TreeMap<Key,Value> buildTestGraph() {
    TreeMap<Key,Value> data = new TreeMap<>();
    byte[] emptyProps = GraphSchema.encodeEdgeProperties(null);

    // Vertex A
    data.put(new Key("A", "V", "_label"), new Value("person"));
    data.put(new Key("A", "V", "name"), new Value("Alice"));
    data.put(new Key("A", "E_knows", "B"), new Value(emptyProps));
    data.put(new Key("A", "E_knows", "D"), new Value(emptyProps));

    // Vertex B
    data.put(new Key("B", "V", "_label"), new Value("person"));
    data.put(new Key("B", "V", "name"), new Value("Bob"));
    data.put(new Key("B", "E_knows", "C"), new Value(emptyProps));

    // Vertex C (leaf)
    data.put(new Key("C", "V", "_label"), new Value("person"));
    data.put(new Key("C", "V", "name"), new Value("Charlie"));

    // Vertex D
    data.put(new Key("D", "V", "_label"), new Value("person"));
    data.put(new Key("D", "V", "name"), new Value("Diana"));
    data.put(new Key("D", "E_knows", "E"), new Value(emptyProps));

    // Vertex E (leaf)
    data.put(new Key("E", "V", "_label"), new Value("person"));
    data.put(new Key("E", "V", "name"), new Value("Eve"));

    return data;
  }

  private Set<String> collectVertexIds(GraphTraversalIterator iter) throws IOException {
    Set<String> vertexIds = new HashSet<>();
    while (iter.hasTop()) {
      vertexIds.add(iter.getTopKey().getRow().toString());
      iter.next();
    }
    return vertexIds;
  }

  @Test
  public void testBFSDepth1() throws IOException {
    TreeMap<Key,Value> data = buildTestGraph();
    GraphTraversalIterator iter = new GraphTraversalIterator();

    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.EDGE_TYPE, "knows");
    options.put(GraphTraversalIterator.MAX_DEPTH, "1");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "BFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Set<String> visited = collectVertexIds(iter);
    // Depth 1 from A: should visit A, B, D
    assertTrue(visited.contains("A"));
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("D"));
    assertFalse(visited.contains("C"));
    assertFalse(visited.contains("E"));
  }

  @Test
  public void testBFSDepth2() throws IOException {
    TreeMap<Key,Value> data = buildTestGraph();
    GraphTraversalIterator iter = new GraphTraversalIterator();

    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.EDGE_TYPE, "knows");
    options.put(GraphTraversalIterator.MAX_DEPTH, "2");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "BFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Set<String> visited = collectVertexIds(iter);
    // Depth 2 from A: should visit A, B, C, D, E
    assertTrue(visited.contains("A"));
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("C"));
    assertTrue(visited.contains("D"));
    assertTrue(visited.contains("E"));
  }

  @Test
  public void testDFSDepth1() throws IOException {
    TreeMap<Key,Value> data = buildTestGraph();
    GraphTraversalIterator iter = new GraphTraversalIterator();

    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.EDGE_TYPE, "knows");
    options.put(GraphTraversalIterator.MAX_DEPTH, "1");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "DFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Set<String> visited = collectVertexIds(iter);
    assertTrue(visited.contains("A"));
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("D"));
    assertFalse(visited.contains("C"));
    assertFalse(visited.contains("E"));
  }

  @Test
  public void testDFSDepth2() throws IOException {
    TreeMap<Key,Value> data = buildTestGraph();
    GraphTraversalIterator iter = new GraphTraversalIterator();

    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.EDGE_TYPE, "knows");
    options.put(GraphTraversalIterator.MAX_DEPTH, "2");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "DFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Set<String> visited = collectVertexIds(iter);
    assertTrue(visited.contains("A"));
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("C"));
    assertTrue(visited.contains("D"));
    assertTrue(visited.contains("E"));
  }

  @Test
  public void testCycleDetection() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    byte[] emptyProps = GraphSchema.encodeEdgeProperties(null);

    // Build cycle: A -> B -> C -> A
    data.put(new Key("A", "V", "_label"), new Value("node"));
    data.put(new Key("A", "E_link", "B"), new Value(emptyProps));
    data.put(new Key("B", "V", "_label"), new Value("node"));
    data.put(new Key("B", "E_link", "C"), new Value(emptyProps));
    data.put(new Key("C", "V", "_label"), new Value("node"));
    data.put(new Key("C", "E_link", "A"), new Value(emptyProps));

    GraphTraversalIterator iter = new GraphTraversalIterator();
    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.EDGE_TYPE, "link");
    options.put(GraphTraversalIterator.MAX_DEPTH, "10");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "BFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Set<String> visited = collectVertexIds(iter);
    // Should visit all 3 without infinite loop
    assertEquals(3, visited.stream().filter(id -> !id.isEmpty()).distinct().count());
    assertTrue(visited.contains("A"));
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("C"));
  }

  @Test
  public void testMaxResultsLimit() throws IOException {
    TreeMap<Key,Value> data = buildTestGraph();
    GraphTraversalIterator iter = new GraphTraversalIterator();

    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.EDGE_TYPE, "knows");
    options.put(GraphTraversalIterator.MAX_DEPTH, "10");
    options.put(GraphTraversalIterator.MAX_RESULTS, "5");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "BFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    int count = 0;
    while (iter.hasTop()) {
      count++;
      iter.next();
    }
    assertTrue(count <= 5, "Result count should be <= maxResults");
  }

  @Test
  public void testNoEdgeTypeTraversesAllEdges() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    byte[] emptyProps = GraphSchema.encodeEdgeProperties(null);

    // A has edges of different types
    data.put(new Key("A", "V", "_label"), new Value("node"));
    data.put(new Key("A", "E_knows", "B"), new Value(emptyProps));
    data.put(new Key("A", "E_likes", "C"), new Value(emptyProps));
    data.put(new Key("B", "V", "_label"), new Value("node"));
    data.put(new Key("C", "V", "_label"), new Value("node"));

    GraphTraversalIterator iter = new GraphTraversalIterator();
    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    // No EDGE_TYPE set — should traverse all edge types
    options.put(GraphTraversalIterator.MAX_DEPTH, "1");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "BFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Set<String> visited = collectVertexIds(iter);
    assertTrue(visited.contains("A"));
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("C"));
  }

  @Test
  public void testStartVertexNotSet() throws IOException {
    GraphTraversalIterator iter = new GraphTraversalIterator();
    Map<String,String> options = new HashMap<>();
    // Don't set START_VERTEX
    iter.init(new SortedMapIterator(new TreeMap<>()), options, null);

    assertThrows(IllegalStateException.class, () -> iter.seek(new Range(), EMPTY_COL_FAMS, false));
  }

  @Test
  public void testResultsSortedByKey() throws IOException {
    TreeMap<Key,Value> data = buildTestGraph();
    GraphTraversalIterator iter = new GraphTraversalIterator();

    Map<String,String> options = new HashMap<>();
    options.put(GraphTraversalIterator.START_VERTEX, "A");
    options.put(GraphTraversalIterator.MAX_DEPTH, "2");
    options.put(GraphTraversalIterator.TRAVERSAL_MODE, "BFS");

    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Key prev = null;
    while (iter.hasTop()) {
      Key current = iter.getTopKey();
      if (prev != null) {
        assertTrue(prev.compareTo(current) <= 0,
            "Results should be sorted by key: " + prev + " > " + current);
      }
      prev = current;
      iter.next();
    }
  }
}
