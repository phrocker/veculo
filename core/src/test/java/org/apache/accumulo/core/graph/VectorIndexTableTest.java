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

import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

public class VectorIndexTableTest {

  @Test
  public void testFormatCellIdZeroPadding() {
    assertEquals("00000000000000ff", VectorIndexTable.formatCellId(255));
    assertEquals("0000000000000000", VectorIndexTable.formatCellId(0));
    assertEquals("0000000000000001", VectorIndexTable.formatCellId(1));
    assertEquals("00000000000000fe", VectorIndexTable.formatCellId(254));
  }

  @Test
  public void testFormatCellIdLargeValues() {
    assertEquals("000000000000ffff", VectorIndexTable.formatCellId(65535));
    assertEquals("00000000ffffffff", VectorIndexTable.formatCellId(4294967295L));
  }

  @Test
  public void testRowKeyRoundTrip() {
    long cellId = 0x42afL;
    String vertexId = "doc-1";

    String rowKey = VectorIndexTable.rowKey(cellId, vertexId);
    assertEquals("00000000000042af:doc-1", rowKey);

    assertEquals(vertexId, VectorIndexTable.extractVertexId(rowKey));
    assertEquals(cellId, VectorIndexTable.extractCellId(rowKey));
  }

  @Test
  public void testRowKeyWithColonInVertexId() {
    // Vertex IDs could contain colons — extractVertexId splits on first colon only
    long cellId = 100L;
    String vertexId = "ns:doc:1";

    String rowKey = VectorIndexTable.rowKey(cellId, vertexId);
    assertEquals("ns:doc:1", VectorIndexTable.extractVertexId(rowKey));
    assertEquals(cellId, VectorIndexTable.extractCellId(rowKey));
  }

  @Test
  public void testCellRangeLexicographicOrder() {
    String hex100 = VectorIndexTable.formatCellId(100);
    String hex200 = VectorIndexTable.formatCellId(200);
    assertTrue(hex100.compareTo(hex200) < 0,
        "formatCellId(100) should be lexicographically less than formatCellId(200)");
  }

  @Test
  public void testCellRangeEndpoints() {
    Range range = VectorIndexTable.cellRange(100, 200);

    // The range should be inclusive on both sides
    String startRow = range.getStartKey().getRow().toString();
    String endRow = range.getEndKey().getRow().toString();

    // Start should begin with the hex for 100
    assertTrue(startRow.startsWith(VectorIndexTable.formatCellId(100)));
    // End should begin with the hex for 200
    assertTrue(endRow.startsWith(VectorIndexTable.formatCellId(200)));
  }

  @Test
  public void testVidxTableName() {
    assertEquals("graph_vidx", VectorIndexTable.vidxTableName("graph"));
    assertEquals("t_acme.graph_vidx", VectorIndexTable.vidxTableName("t_acme.graph"));
    assertEquals("my_table_vidx", VectorIndexTable.vidxTableName("my_table"));
  }

  @Test
  public void testExtractVertexIdNoSeparatorThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexTable.extractVertexId("noseparator"));
  }

  @Test
  public void testExtractCellIdNoSeparatorThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexTable.extractCellId("noseparator"));
  }

  @Test
  public void testRowKeyNullVertexIdThrows() {
    assertThrows(NullPointerException.class, () -> VectorIndexTable.rowKey(42, null));
  }
}
