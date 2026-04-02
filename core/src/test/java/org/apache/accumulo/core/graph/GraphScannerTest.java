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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.graph.EdgeFilterIterator.ComparisonOperator;
import org.apache.accumulo.core.graph.GraphTraversalIterator.TraversalMode;
import org.junit.jupiter.api.Test;

/**
 * Tests for GraphScanner utility methods. Since GraphScanner operates on real Scanner objects which
 * require an AccumuloClient, we test the static configuration methods by verifying the
 * IteratorSetting and option patterns they produce.
 */
public class GraphScannerTest {

  @Test
  public void testEdgeFilterConfiguration() {
    IteratorSetting is = new IteratorSetting(15, "edgeFilter", EdgeFilterIterator.class);
    EdgeFilterIterator.setEdgeType(is, "knows");
    EdgeFilterIterator.setPropertyFilter(is, "weight", ComparisonOperator.GTE, "0.5");

    assertEquals("knows", is.getOptions().get(EdgeFilterIterator.EDGE_TYPE));
    assertEquals("weight", is.getOptions().get(EdgeFilterIterator.PROPERTY_NAME));
    assertEquals("GTE", is.getOptions().get(EdgeFilterIterator.OPERATOR));
    assertEquals("0.5", is.getOptions().get(EdgeFilterIterator.PROPERTY_VALUE));
  }

  @Test
  public void testWeightRangeConfiguration() {
    IteratorSetting is = new IteratorSetting(15, "edgeFilter", EdgeFilterIterator.class);
    EdgeFilterIterator.setWeightRange(is, 0.3, 0.9);

    assertEquals("0.3", is.getOptions().get(EdgeFilterIterator.MIN_WEIGHT));
    assertEquals("0.9", is.getOptions().get(EdgeFilterIterator.MAX_WEIGHT));
  }

  @Test
  public void testTraversalIteratorConfiguration() {
    IteratorSetting is = new IteratorSetting(20, "graphTraversal", GraphTraversalIterator.class);
    is.addOption(GraphTraversalIterator.START_VERTEX, "vertex1");
    is.addOption(GraphTraversalIterator.EDGE_TYPE, "knows");
    is.addOption(GraphTraversalIterator.MAX_DEPTH, "3");
    is.addOption(GraphTraversalIterator.MAX_RESULTS, "500");
    is.addOption(GraphTraversalIterator.TRAVERSAL_MODE, TraversalMode.DFS.name());

    assertEquals("vertex1", is.getOptions().get(GraphTraversalIterator.START_VERTEX));
    assertEquals("knows", is.getOptions().get(GraphTraversalIterator.EDGE_TYPE));
    assertEquals("3", is.getOptions().get(GraphTraversalIterator.MAX_DEPTH));
    assertEquals("500", is.getOptions().get(GraphTraversalIterator.MAX_RESULTS));
    assertEquals("DFS", is.getOptions().get(GraphTraversalIterator.TRAVERSAL_MODE));
  }

  @Test
  public void testVertexColfamConstant() {
    assertNotNull(GraphSchema.VERTEX_COLFAM_TEXT);
    assertEquals("V", GraphSchema.VERTEX_COLFAM_TEXT.toString());
  }

  @Test
  public void testSchemaConstants() {
    assertEquals("V", GraphSchema.VERTEX_COLFAM);
    assertEquals("E_", GraphSchema.EDGE_COLFAM_PREFIX);
    assertEquals("EI_", GraphSchema.EDGE_INVERSE_COLFAM_PREFIX);
    assertEquals("_label", GraphSchema.LABEL_COLQUAL);
    assertEquals("_embedding", GraphSchema.EMBEDDING_COLQUAL);
  }

  @Test
  public void testEdgeFilterNullWeightRange() {
    IteratorSetting is = new IteratorSetting(15, "edgeFilter", EdgeFilterIterator.class);
    EdgeFilterIterator.setWeightRange(is, null, 0.9);
    // minWeight should not be set
    assertTrue(!is.getOptions().containsKey(EdgeFilterIterator.MIN_WEIGHT));
    assertEquals("0.9", is.getOptions().get(EdgeFilterIterator.MAX_WEIGHT));
  }
}
