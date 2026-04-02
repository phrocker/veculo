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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class GraphSchemaTest {

  @Test
  public void testEdgeColumnFamily() {
    assertEquals("E_knows", GraphSchema.edgeColumnFamily("knows"));
    assertEquals("E_references", GraphSchema.edgeColumnFamily("references"));
  }

  @Test
  public void testInverseEdgeColumnFamily() {
    assertEquals("EI_knows", GraphSchema.inverseEdgeColumnFamily("knows"));
    assertEquals("EI_references", GraphSchema.inverseEdgeColumnFamily("references"));
  }

  @Test
  public void testExtractEdgeType() {
    assertEquals("knows", GraphSchema.extractEdgeType("E_knows"));
    assertEquals("knows", GraphSchema.extractEdgeType("EI_knows"));
    assertEquals("has_parent", GraphSchema.extractEdgeType("E_has_parent"));
  }

  @Test
  public void testExtractEdgeTypeInvalidInput() {
    assertThrows(IllegalArgumentException.class, () -> GraphSchema.extractEdgeType("V"));
    assertThrows(IllegalArgumentException.class, () -> GraphSchema.extractEdgeType("random"));
  }

  @Test
  public void testIsVertexProperty() {
    assertTrue(GraphSchema.isVertexProperty(new Key("v1", "V", "name")));
    assertTrue(GraphSchema.isVertexProperty(new Key("v1", "V", "_label")));
    assertFalse(GraphSchema.isVertexProperty(new Key("v1", "E_knows", "v2")));
    assertFalse(GraphSchema.isVertexProperty(new Key("v1", "EI_knows", "v2")));
  }

  @Test
  public void testIsOutgoingEdge() {
    assertTrue(GraphSchema.isOutgoingEdge(new Key("v1", "E_knows", "v2")));
    assertTrue(GraphSchema.isOutgoingEdge(new Key("v1", "E_references", "v3")));
    assertFalse(GraphSchema.isOutgoingEdge(new Key("v1", "V", "name")));
    assertFalse(GraphSchema.isOutgoingEdge(new Key("v1", "EI_knows", "v2")));
  }

  @Test
  public void testIsIncomingEdge() {
    assertTrue(GraphSchema.isIncomingEdge(new Key("v1", "EI_knows", "v2")));
    assertFalse(GraphSchema.isIncomingEdge(new Key("v1", "E_knows", "v2")));
    assertFalse(GraphSchema.isIncomingEdge(new Key("v1", "V", "name")));
  }

  @Test
  public void testVertexRange() {
    Range range = GraphSchema.vertexRange("vertex1");
    assertTrue(range.contains(new Key("vertex1", "V", "name")));
    assertTrue(range.contains(new Key("vertex1", "E_knows", "v2")));
    assertFalse(range.contains(new Key("vertex2", "V", "name")));
  }

  @Test
  public void testOutgoingEdgesRange() {
    Range range = GraphSchema.outgoingEdgesRange("v1", "knows");
    assertTrue(range.contains(new Key("v1", "E_knows", "v2")));
    assertFalse(range.contains(new Key("v1", "E_references", "v2")));
    assertFalse(range.contains(new Key("v1", "V", "name")));
    assertFalse(range.contains(new Key("v2", "E_knows", "v3")));
  }

  @Test
  public void testIncomingEdgesRange() {
    Range range = GraphSchema.incomingEdgesRange("v1", "knows");
    assertTrue(range.contains(new Key("v1", "EI_knows", "v2")));
    assertFalse(range.contains(new Key("v1", "E_knows", "v2")));
  }

  @Test
  public void testEncodeDecodeEdgeProperties() {
    Map<String,String> props = new HashMap<>();
    props.put("weight", "0.85");
    props.put("since", "2024");

    byte[] encoded = GraphSchema.encodeEdgeProperties(props);
    Map<String,String> decoded = GraphSchema.decodeEdgeProperties(encoded);

    assertEquals("0.85", decoded.get("weight"));
    assertEquals("2024", decoded.get("since"));
    assertEquals(2, decoded.size());
  }

  @Test
  public void testEncodeDecodeEmptyProperties() {
    byte[] encoded = GraphSchema.encodeEdgeProperties(null);
    Map<String,String> decoded = GraphSchema.decodeEdgeProperties(encoded);
    assertTrue(decoded.isEmpty());

    encoded = GraphSchema.encodeEdgeProperties(new HashMap<>());
    decoded = GraphSchema.decodeEdgeProperties(encoded);
    assertTrue(decoded.isEmpty());
  }

  @Test
  public void testDecodeNullBytes() {
    Map<String,String> decoded = GraphSchema.decodeEdgeProperties(null);
    assertTrue(decoded.isEmpty());

    decoded = GraphSchema.decodeEdgeProperties(new byte[0]);
    assertTrue(decoded.isEmpty());
  }

  @Test
  public void testCompoundVisibility() {
    ColumnVisibility a = new ColumnVisibility("alpha");
    ColumnVisibility b = new ColumnVisibility("beta");
    ColumnVisibility result = GraphSchema.compoundVisibility(a, b);
    assertEquals("(alpha)&(beta)", new String(result.getExpression()));
  }

  @Test
  public void testCompoundVisibilitySameExpression() {
    ColumnVisibility a = new ColumnVisibility("secret");
    ColumnVisibility b = new ColumnVisibility("secret");
    ColumnVisibility result = GraphSchema.compoundVisibility(a, b);
    assertEquals("secret", new String(result.getExpression()));
  }

  @Test
  public void testCompoundVisibilityWithEmpty() {
    ColumnVisibility a = new ColumnVisibility("alpha");
    ColumnVisibility empty = new ColumnVisibility();

    assertEquals("alpha", new String(GraphSchema.compoundVisibility(a, empty).getExpression()));
    assertEquals("alpha", new String(GraphSchema.compoundVisibility(empty, a).getExpression()));
    assertEquals(0, GraphSchema.compoundVisibility(empty, empty).getExpression().length);
    assertEquals("alpha", new String(GraphSchema.compoundVisibility(a, null).getExpression()));
    assertEquals("alpha", new String(GraphSchema.compoundVisibility(null, a).getExpression()));
  }

  @Test
  public void testIsPendingMarker() {
    assertTrue(GraphSchema.isPendingMarker(new Key("v1", "P_auto_embed", "text")));
    assertTrue(GraphSchema.isPendingMarker(new Key("v1", "P_summary", "_summary")));
    assertFalse(GraphSchema.isPendingMarker(new Key("v1", "V", "name")));
    assertFalse(GraphSchema.isPendingMarker(new Key("v1", "E_knows", "v2")));
  }

  @Test
  public void testNewConstants() {
    assertEquals("_summary", GraphSchema.SUMMARY_COLQUAL);
    assertEquals("_embedding_model", GraphSchema.EMBEDDING_MODEL_COLQUAL);
    assertEquals("_cell_id", GraphSchema.CELL_ID_COLQUAL);
    assertEquals("_rank", GraphSchema.RANK_COLQUAL);
    assertEquals("_anomaly_score", GraphSchema.ANOMALY_SCORE_COLQUAL);
    assertEquals("P_", GraphSchema.PENDING_COLFAM_PREFIX);
    assertEquals("SIMILAR_TO", GraphSchema.SIMILAR_TO_EDGE_TYPE);
  }
}
