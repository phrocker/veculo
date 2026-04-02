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

import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class VectorIndexWriterTest {

  private static final ColumnVisibility PUBLIC = new ColumnVisibility("public");

  @Test
  public void testVidxMutationRowKeyFormat() {
    float[] embedding = {1.0f, 0.0f, 0.0f};
    Mutation m = VectorIndexWriter.vidxMutation("doc-1", embedding, PUBLIC);

    String rowKey = new String(m.getRow());
    // Row key should start with a 16-char hex cell ID
    assertTrue(rowKey.contains(":"), "Row key should contain separator");
    String hexPart = rowKey.substring(0, rowKey.indexOf(':'));
    assertEquals(16, hexPart.length(), "Cell ID hex should be 16 characters");
    // Should parse as a hex number without error
    Long.parseUnsignedLong(hexPart, 16);

    // Vertex ID should be extracted correctly
    assertEquals("doc-1", VectorIndexTable.extractVertexId(rowKey));
  }

  @Test
  public void testVidxMutationColumnFamilyAndQualifier() {
    float[] embedding = {0.5f, 0.5f, 0.5f};
    Mutation m = VectorIndexWriter.vidxMutation("v1", embedding, PUBLIC);

    List<ColumnUpdate> updates = m.getUpdates();
    assertEquals(1, updates.size());

    ColumnUpdate update = updates.get(0);
    assertEquals("V", new String(update.getColumnFamily()));
    assertEquals("_embedding", new String(update.getColumnQualifier()));
  }

  @Test
  public void testDualWriteMutationsCount() {
    float[] embedding = {1.0f, 2.0f, 3.0f};
    List<Mutation> mutations = VectorIndexWriter.dualWriteMutations("doc-1", embedding, PUBLIC);

    assertEquals(2, mutations.size(), "Should return exactly 2 mutations");

    // First mutation is for the graph table (row = vertexId)
    assertEquals("doc-1", new String(mutations.get(0).getRow()));

    // Second mutation is for the vidx table (row = cellIdHex:vertexId)
    String vidxRow = new String(mutations.get(1).getRow());
    assertTrue(vidxRow.contains(":"), "vidx row should contain separator");
    assertEquals("doc-1", VectorIndexTable.extractVertexId(vidxRow));
  }

  @Test
  public void testCellIdDeterminism() {
    float[] embedding = {0.3f, 0.7f, 0.1f};

    Mutation m1 = VectorIndexWriter.vidxMutation("v1", embedding, PUBLIC);
    Mutation m2 = VectorIndexWriter.vidxMutation("v1", embedding, PUBLIC);

    String row1 = new String(m1.getRow());
    String row2 = new String(m2.getRow());

    assertEquals(row1, row2, "Same embedding should always produce the same vidx row key");
  }

  @Test
  public void testScaledVectorsSameCellId() {
    float[] v1 = {1.0f, 2.0f, 3.0f};
    float[] v2 = {2.0f, 4.0f, 6.0f}; // same direction, different magnitude

    Mutation m1 = VectorIndexWriter.vidxMutation("v1", v1, PUBLIC);
    Mutation m2 = VectorIndexWriter.vidxMutation("v1", v2, PUBLIC);

    // Cell ID should be the same since SphericalTessellation normalizes
    long cellId1 = VectorIndexTable.extractCellId(new String(m1.getRow()));
    long cellId2 = VectorIndexTable.extractCellId(new String(m2.getRow()));
    assertEquals(cellId1, cellId2,
        "Scaled vectors should produce the same cell ID after normalization");
  }
}
