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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.ValueType;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

/**
 * Tests for GraphVectorBridge. Since the full pipeline requires an AccumuloClient, we test the
 * mutation-building and data model aspects that can be verified without a running cluster.
 */
public class GraphVectorBridgeTest {

  private static final ColumnVisibility PUBLIC = new ColumnVisibility("public");

  @Test
  public void testVertexWithEmbeddingMutations() {
    float[] embedding = {0.1f, 0.2f, 0.3f, 0.4f};
    Map<String,String> props = new HashMap<>();
    props.put("title", "Test Document");

    // Build the mutations that addVertexWithEmbedding would create
    Mutation vertexMut = GraphMutations.addVertex("doc1", "document", props, PUBLIC);
    Mutation embeddingMut = GraphMutations.addVertexEmbedding("doc1", embedding, PUBLIC);

    // Verify vertex mutation
    assertEquals("doc1", new String(vertexMut.getRow()));
    List<ColumnUpdate> vertexUpdates = vertexMut.getUpdates();
    assertEquals(2, vertexUpdates.size()); // _label + title

    // Verify embedding mutation
    assertEquals("doc1", new String(embeddingMut.getRow()));
    List<ColumnUpdate> embUpdates = embeddingMut.getUpdates();
    assertEquals(1, embUpdates.size());
    ColumnUpdate embUpdate = embUpdates.get(0);
    assertEquals("V", new String(embUpdate.getColumnFamily()));
    assertEquals("_embedding", new String(embUpdate.getColumnQualifier()));

    // Verify embedding can be decoded
    Value embValue = new Value(embUpdate.getValue());
    embValue.setValueType(ValueType.VECTOR_FLOAT32);
    float[] decoded = embValue.asVector();
    assertEquals(4, decoded.length);
    assertEquals(0.1f, decoded[0], 0.001f);
    assertEquals(0.4f, decoded[3], 0.001f);
  }

  @Test
  public void testGraphSchemaIntegrationWithEdges() {
    // Test that edge mutations work with the graph schema correctly
    Map<String,String> edgeProps = new HashMap<>();
    edgeProps.put("relevance", "0.95");

    List<Mutation> edgeMuts =
        GraphMutations.addEdge("doc1", "doc2", "references", edgeProps, PUBLIC);
    assertEquals(2, edgeMuts.size());

    // Forward: doc1 -> E_references -> doc2
    Mutation fwd = edgeMuts.get(0);
    ColumnUpdate fwdUpdate = fwd.getUpdates().get(0);
    assertEquals("E_references", new String(fwdUpdate.getColumnFamily()));
    assertEquals("doc2", new String(fwdUpdate.getColumnQualifier()));

    // Decode properties from forward edge
    Map<String,String> decodedProps = GraphSchema.decodeEdgeProperties(fwdUpdate.getValue());
    assertEquals("0.95", decodedProps.get("relevance"));

    // Inverse: doc2 -> EI_references -> doc1
    Mutation inv = edgeMuts.get(1);
    ColumnUpdate invUpdate = inv.getUpdates().get(0);
    assertEquals("EI_references", new String(invUpdate.getColumnFamily()));
    assertEquals("doc1", new String(invUpdate.getColumnQualifier()));
  }

  @Test
  public void testRAGPipelineDataModel() {
    // Simulate the data model for a RAG pipeline:
    // 1. Document vertices with embeddings
    // 2. Edges connecting related documents

    float[] emb1 = {1.0f, 0.0f, 0.0f};
    float[] emb2 = {0.9f, 0.1f, 0.0f};
    float[] emb3 = {0.0f, 0.0f, 1.0f};

    // Create document vertices
    Map<String,String> doc1Props = new HashMap<>();
    doc1Props.put("title", "Machine Learning Basics");
    Mutation doc1 = GraphMutations.addVertex("doc1", "document", doc1Props, PUBLIC);
    Mutation doc1Emb = GraphMutations.addVertexEmbedding("doc1", emb1, PUBLIC);

    Map<String,String> doc2Props = new HashMap<>();
    doc2Props.put("title", "Deep Learning Guide");
    Mutation doc2 = GraphMutations.addVertex("doc2", "document", doc2Props, PUBLIC);
    Mutation doc2Emb = GraphMutations.addVertexEmbedding("doc2", emb2, PUBLIC);

    // Create relationship edges
    Map<String,String> relProps = new HashMap<>();
    relProps.put("weight", "0.92");
    List<Mutation> edges = GraphMutations.addEdge("doc1", "doc2", "references", relProps, PUBLIC);

    // Verify the full data model
    assertNotNull(doc1);
    assertNotNull(doc2);
    assertNotNull(doc1Emb);
    assertNotNull(doc2Emb);
    assertEquals(2, edges.size());

    // Verify embedding dimensions match
    Value v1 = new Value(doc1Emb.getUpdates().get(0).getValue());
    v1.setValueType(ValueType.VECTOR_FLOAT32);
    assertEquals(3, v1.asVector().length);
  }

  @Test
  public void testWeightedEdgesForGraphTraversal() {
    // Create weighted edges for traversal filtering
    List<Mutation> edge1 = GraphMutations.addWeightedEdge("A", "B", "knows", 0.9, PUBLIC);
    List<Mutation> edge2 = GraphMutations.addWeightedEdge("A", "C", "knows", 0.3, PUBLIC);
    List<Mutation> edge3 = GraphMutations.addWeightedEdge("B", "D", "knows", 0.7, PUBLIC);

    // Verify weight is encoded in edge properties
    Map<String,String> props1 =
        GraphSchema.decodeEdgeProperties(edge1.get(0).getUpdates().get(0).getValue());
    assertEquals("0.9", props1.get("weight"));

    Map<String,String> props2 =
        GraphSchema.decodeEdgeProperties(edge2.get(0).getUpdates().get(0).getValue());
    assertEquals("0.3", props2.get("weight"));

    Map<String,String> props3 =
        GraphSchema.decodeEdgeProperties(edge3.get(0).getUpdates().get(0).getValue());
    assertEquals("0.7", props3.get("weight"));
  }
}
