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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.ValueType;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class GraphMutationsTest {

  private static final ColumnVisibility PUBLIC = new ColumnVisibility("public");
  private static final ColumnVisibility EMPTY = new ColumnVisibility();

  @Test
  public void testAddVertex() {
    Map<String,String> props = new HashMap<>();
    props.put("name", "Alice");
    props.put("age", "30");

    Mutation m = GraphMutations.addVertex("v1", "person", props, PUBLIC);

    assertEquals("v1", new String(m.getRow()));
    List<ColumnUpdate> updates = m.getUpdates();
    // Should have: _label, name, age = 3 updates
    assertEquals(3, updates.size());

    // Verify label is present
    boolean hasLabel = updates.stream()
        .anyMatch(u -> new String(u.getColumnFamily()).equals("V")
            && new String(u.getColumnQualifier()).equals("_label")
            && new String(u.getValue()).equals("person"));
    assertTrue(hasLabel);

    // Verify properties
    boolean hasName = updates.stream()
        .anyMatch(u -> new String(u.getColumnFamily()).equals("V")
            && new String(u.getColumnQualifier()).equals("name")
            && new String(u.getValue()).equals("Alice"));
    assertTrue(hasName);
  }

  @Test
  public void testAddVertexNoProperties() {
    Mutation m = GraphMutations.addVertex("v1", "person", null, EMPTY);
    List<ColumnUpdate> updates = m.getUpdates();
    assertEquals(1, updates.size()); // Only _label
  }

  @Test
  public void testAddVertexEmbedding() {
    float[] embedding = {1.0f, 2.0f, 3.0f};
    Mutation m = GraphMutations.addVertexEmbedding("v1", embedding, PUBLIC);

    assertEquals("v1", new String(m.getRow()));
    List<ColumnUpdate> updates = m.getUpdates();
    assertEquals(1, updates.size());

    ColumnUpdate update = updates.get(0);
    assertEquals("V", new String(update.getColumnFamily()));
    assertEquals("_embedding", new String(update.getColumnQualifier()));

    // Verify the value can be decoded as a vector
    Value val = new Value(update.getValue());
    val.setValueType(ValueType.VECTOR_FLOAT32);
    float[] decoded = val.asVector();
    assertEquals(3, decoded.length);
    assertEquals(1.0f, decoded[0], 0.001f);
    assertEquals(2.0f, decoded[1], 0.001f);
    assertEquals(3.0f, decoded[2], 0.001f);
  }

  @Test
  public void testAddEdge() {
    Map<String,String> props = new HashMap<>();
    props.put("since", "2024");

    List<Mutation> mutations = GraphMutations.addEdge("v1", "v2", "knows", props, PUBLIC);
    assertEquals(2, mutations.size());

    // Forward edge: v1 -> E_knows -> v2
    Mutation forward = mutations.get(0);
    assertEquals("v1", new String(forward.getRow()));
    ColumnUpdate fwdUpdate = forward.getUpdates().get(0);
    assertEquals("E_knows", new String(fwdUpdate.getColumnFamily()));
    assertEquals("v2", new String(fwdUpdate.getColumnQualifier()));

    // Inverse edge: v2 -> EI_knows -> v1
    Mutation inverse = mutations.get(1);
    assertEquals("v2", new String(inverse.getRow()));
    ColumnUpdate invUpdate = inverse.getUpdates().get(0);
    assertEquals("EI_knows", new String(invUpdate.getColumnFamily()));
    assertEquals("v1", new String(invUpdate.getColumnQualifier()));

    // Both should have same JSON value
    Map<String,String> fwdProps = GraphSchema.decodeEdgeProperties(fwdUpdate.getValue());
    assertEquals("2024", fwdProps.get("since"));
  }

  @Test
  public void testAddWeightedEdge() {
    List<Mutation> mutations = GraphMutations.addWeightedEdge("v1", "v2", "knows", 0.85, PUBLIC);
    assertEquals(2, mutations.size());

    ColumnUpdate update = mutations.get(0).getUpdates().get(0);
    Map<String,String> props = GraphSchema.decodeEdgeProperties(update.getValue());
    assertEquals("0.85", props.get("weight"));
  }

  @Test
  public void testRemoveEdge() {
    List<Mutation> mutations = GraphMutations.removeEdge("v1", "v2", "knows");
    assertEquals(2, mutations.size());

    // Forward delete
    Mutation forward = mutations.get(0);
    assertEquals("v1", new String(forward.getRow()));
    ColumnUpdate fwdUpdate = forward.getUpdates().get(0);
    assertTrue(fwdUpdate.isDeleted());
    assertEquals("E_knows", new String(fwdUpdate.getColumnFamily()));
    assertEquals("v2", new String(fwdUpdate.getColumnQualifier()));

    // Inverse delete
    Mutation inverse = mutations.get(1);
    assertEquals("v2", new String(inverse.getRow()));
    ColumnUpdate invUpdate = inverse.getUpdates().get(0);
    assertTrue(invUpdate.isDeleted());
    assertEquals("EI_knows", new String(invUpdate.getColumnFamily()));
    assertEquals("v1", new String(invUpdate.getColumnQualifier()));
  }

  @Test
  public void testRemoveVertex() {
    Mutation m = GraphMutations.removeVertex("v1");
    assertEquals("v1", new String(m.getRow()));
    ColumnUpdate update = m.getUpdates().get(0);
    assertTrue(update.isDeleted());
    assertEquals("V", new String(update.getColumnFamily()));
    assertEquals("_label", new String(update.getColumnQualifier()));
  }

  @Test
  public void testNullChecks() {
    assertThrows(NullPointerException.class,
        () -> GraphMutations.addVertex(null, "label", null, EMPTY));
    assertThrows(NullPointerException.class,
        () -> GraphMutations.addVertex("id", null, null, EMPTY));
    assertThrows(NullPointerException.class,
        () -> GraphMutations.addVertexEmbedding(null, new float[] {1.0f}, EMPTY));
    assertThrows(NullPointerException.class,
        () -> GraphMutations.addVertexEmbedding("id", null, EMPTY));
    assertThrows(NullPointerException.class,
        () -> GraphMutations.addEdge(null, "v2", "knows", null, EMPTY));
    assertThrows(NullPointerException.class, () -> GraphMutations.removeEdge("v1", null, "knows"));
    assertThrows(NullPointerException.class, () -> GraphMutations.removeVertex(null));
  }
}
