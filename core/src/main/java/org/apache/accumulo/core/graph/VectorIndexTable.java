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

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

/**
 * Row key format and range helpers for the {@code graph_vidx} secondary index table. The vidx table
 * maps tessellation cell IDs to vertex IDs, enabling O(log n) vector search via Accumulo range
 * scans.
 *
 * <p>
 * Row key format: {@code <cellId_hex>:<vertexId>} where cellId_hex is a 16-character zero-padded
 * hexadecimal string. This ensures lexicographic ordering matches numeric ordering of cell IDs.
 */
public final class VectorIndexTable {

  /** Width of the hex-encoded cell ID in characters. */
  public static final int CELL_ID_HEX_WIDTH = 16;

  /** Separator between cell ID and vertex ID in row keys. */
  public static final char SEPARATOR = ':';

  /** Suffix appended to a graph table name to derive the vidx table name. */
  public static final String VIDX_SUFFIX = "_vidx";

  /** Default vidx table name when the graph table is named "graph". */
  public static final String DEFAULT_VIDX_TABLE = "graph_vidx";

  private VectorIndexTable() {}

  /**
   * Formats a cell ID as a 16-character zero-padded hexadecimal string.
   *
   * @param cellId the 64-bit cell ID from {@link SphericalTessellation#assignCellId(float[])}
   * @return 16-char hex string, e.g. {@code "00000000000000ff"} for cellId=255
   */
  public static String formatCellId(long cellId) {
    return String.format("%016x", cellId);
  }

  /**
   * Constructs a vidx row key from a cell ID and vertex ID.
   *
   * @param cellId the tessellation cell ID
   * @param vertexId the vertex identifier
   * @return row key in format {@code "<hex>:<vertexId>"}
   */
  public static String rowKey(long cellId, String vertexId) {
    requireNonNull(vertexId, "Vertex ID must not be null");
    return formatCellId(cellId) + SEPARATOR + vertexId;
  }

  /**
   * Extracts the vertex ID from a vidx row key.
   *
   * @param rowKey the full row key
   * @return the vertex ID portion after the separator
   * @throws IllegalArgumentException if the row key does not contain a separator
   */
  public static String extractVertexId(String rowKey) {
    requireNonNull(rowKey, "Row key must not be null");
    int sepIdx = rowKey.indexOf(SEPARATOR);
    if (sepIdx < 0) {
      throw new IllegalArgumentException("Row key does not contain separator: " + rowKey);
    }
    return rowKey.substring(sepIdx + 1);
  }

  /**
   * Extracts the cell ID from a vidx row key.
   *
   * @param rowKey the full row key
   * @return the cell ID parsed from the hex prefix
   * @throws IllegalArgumentException if the row key format is invalid
   */
  public static long extractCellId(String rowKey) {
    requireNonNull(rowKey, "Row key must not be null");
    int sepIdx = rowKey.indexOf(SEPARATOR);
    if (sepIdx < 0) {
      throw new IllegalArgumentException("Row key does not contain separator: " + rowKey);
    }
    return Long.parseUnsignedLong(rowKey.substring(0, sepIdx), 16);
  }

  /**
   * Creates an Accumulo Range that covers all vidx entries for cell IDs in
   * {@code [startCellId, endCellId]}.
   *
   * @param startCellId inclusive start cell ID
   * @param endCellId inclusive end cell ID
   * @return a Range covering all row keys from the start cell to the end of the end cell
   */
  public static Range cellRange(long startCellId, long endCellId) {
    String startRow = formatCellId(startCellId) + SEPARATOR;
    // Use the character after SEPARATOR (':', 0x3A) -> ';' (0x3B) is next, but we want
    // to include all vertex IDs. Use a high Unicode character to cover all vertex IDs.
    String endRow = formatCellId(endCellId) + SEPARATOR + "~";
    return new Range(new Text(startRow), true, new Text(endRow), true);
  }

  /**
   * Derives the vidx table name from a graph table name by appending {@value #VIDX_SUFFIX}.
   *
   * @param graphTable the graph table name
   * @return the corresponding vidx table name
   */
  public static String vidxTableName(String graphTable) {
    requireNonNull(graphTable, "Graph table name must not be null");
    return graphTable + VIDX_SUFFIX;
  }
}
