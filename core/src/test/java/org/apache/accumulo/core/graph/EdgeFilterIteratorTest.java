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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

public class EdgeFilterIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private TreeMap<Key,Value> buildTestData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    // Vertex properties (should always pass through)
    data.put(new Key("v1", "V", "_label"), new Value("person"));
    data.put(new Key("v1", "V", "name"), new Value("Alice"));

    // Outgoing edges with weights
    Map<String,String> edge1Props = new HashMap<>();
    edge1Props.put("weight", "0.9");
    edge1Props.put("since", "2020");
    data.put(new Key("v1", "E_knows", "v2"),
        new Value(GraphSchema.encodeEdgeProperties(edge1Props)));

    Map<String,String> edge2Props = new HashMap<>();
    edge2Props.put("weight", "0.3");
    data.put(new Key("v1", "E_knows", "v3"),
        new Value(GraphSchema.encodeEdgeProperties(edge2Props)));

    Map<String,String> edge3Props = new HashMap<>();
    edge3Props.put("weight", "0.7");
    data.put(new Key("v1", "E_references", "v4"),
        new Value(GraphSchema.encodeEdgeProperties(edge3Props)));

    // Incoming edges
    Map<String,String> edge4Props = new HashMap<>();
    edge4Props.put("weight", "0.5");
    data.put(new Key("v1", "EI_knows", "v5"),
        new Value(GraphSchema.encodeEdgeProperties(edge4Props)));

    return data;
  }

  private List<Key> collectKeys(EdgeFilterIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testVertexPropertiesPassThrough() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    // Filter for edge type that doesn't match any vertex props
    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.EDGE_TYPE, "nonexistent");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Vertex properties should still be present
    assertTrue(keys.stream().anyMatch(k -> k.getColumnFamily().toString().equals("V")));
    // No edges should pass (wrong type)
    assertFalse(keys.stream().anyMatch(k -> k.getColumnFamily().toString().startsWith("E_")));
  }

  @Test
  public void testFilterByEdgeType() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.EDGE_TYPE, "knows");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Should have vertex props + "knows" edges (both E_ and EI_)
    long edgeCount = keys.stream().filter(k -> k.getColumnFamily().toString().startsWith("E_")
        || k.getColumnFamily().toString().startsWith("EI_")).count();
    assertEquals(3, edgeCount); // E_knows/v2, E_knows/v3, EI_knows/v5
  }

  @Test
  public void testFilterByWeightThreshold() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.MIN_WEIGHT, "0.5");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Edges with weight >= 0.5: v2(0.9), v4(0.7), v5(0.5) = 3 edges + 2 vertex props
    long edgeCount = keys.stream().filter(k -> !k.getColumnFamily().toString().equals("V")).count();
    assertEquals(3, edgeCount);
  }

  @Test
  public void testFilterByMaxWeight() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.MAX_WEIGHT, "0.5");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Edges with weight <= 0.5: v3(0.3), v5(0.5) = 2 edges + 2 vertex props
    long edgeCount = keys.stream().filter(k -> !k.getColumnFamily().toString().equals("V")).count();
    assertEquals(2, edgeCount);
  }

  @Test
  public void testFilterByWeightRange() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.MIN_WEIGHT, "0.5");
    options.put(EdgeFilterIterator.MAX_WEIGHT, "0.8");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Edges with 0.5 <= weight <= 0.8: v4(0.7), v5(0.5) = 2 edges + 2 vertex props
    long edgeCount = keys.stream().filter(k -> !k.getColumnFamily().toString().equals("V")).count();
    assertEquals(2, edgeCount);
  }

  @Test
  public void testFilterByPropertyEquality() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.PROPERTY_NAME, "since");
    options.put(EdgeFilterIterator.OPERATOR, "EQ");
    options.put(EdgeFilterIterator.PROPERTY_VALUE, "2020");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Only edge v1->v2 has since=2020
    long edgeCount = keys.stream().filter(k -> !k.getColumnFamily().toString().equals("V")).count();
    assertEquals(1, edgeCount);
    assertTrue(keys.stream().anyMatch(k -> k.getColumnQualifier().toString().equals("v2")
        && k.getColumnFamily().toString().equals("E_knows")));
  }

  @Test
  public void testCombinedEdgeTypeAndWeightFilter() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> options = new HashMap<>();
    options.put(EdgeFilterIterator.EDGE_TYPE, "knows");
    options.put(EdgeFilterIterator.MIN_WEIGHT, "0.5");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // "knows" edges with weight >= 0.5: v2(0.9), v5(0.5) = 2 edges + 2 vertex props
    long edgeCount = keys.stream().filter(k -> !k.getColumnFamily().toString().equals("V")).count();
    assertEquals(2, edgeCount);
  }

  @Test
  public void testNoFilterPassesEverything() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    EdgeFilterIterator iter = new EdgeFilterIterator();

    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(data.size(), keys.size());
  }

  @Test
  public void testDescribeOptions() {
    EdgeFilterIterator iter = new EdgeFilterIterator();
    var options = iter.describeOptions();
    assertEquals("edgeFilter", options.getName());
    assertTrue(options.getNamedOptions().containsKey(EdgeFilterIterator.EDGE_TYPE));
    assertTrue(options.getNamedOptions().containsKey(EdgeFilterIterator.MIN_WEIGHT));
    assertTrue(options.getNamedOptions().containsKey(EdgeFilterIterator.MAX_WEIGHT));
  }

  @Test
  public void testValidateOptions() {
    EdgeFilterIterator iter = new EdgeFilterIterator();

    Map<String,String> valid = new HashMap<>();
    valid.put(EdgeFilterIterator.OPERATOR, "EQ");
    valid.put(EdgeFilterIterator.MIN_WEIGHT, "0.5");
    assertTrue(iter.validateOptions(valid));

    Map<String,String> invalid = new HashMap<>();
    invalid.put(EdgeFilterIterator.OPERATOR, "INVALID");
    try {
      iter.validateOptions(invalid);
      assertTrue(false, "Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
