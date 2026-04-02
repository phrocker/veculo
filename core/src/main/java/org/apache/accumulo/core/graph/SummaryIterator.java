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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Major compaction iterator that detects vertices with properties but no {@code _summary} column
 * qualifier. When a vertex has at least {@code minProperties} non-system properties and lacks a
 * summary, a {@code P_summary} pending marker is emitted so that an external worker can generate a
 * text summary of the vertex's properties.
 *
 * <p>
 * This iterator performs row-aware buffering: it collects all entries for each row (vertex), counts
 * non-system properties, checks for an existing summary, and emits a marker if appropriate.
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code minProperties} — minimum number of non-system properties to trigger summary generation
 * (default 2)</li>
 * </ul>
 */
public class SummaryIterator extends PendingMarkerIterator {

  public static final String MIN_PROPERTIES = "minProperties";

  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "summary";
  private static final int DEFAULT_MIN_PROPERTIES = 2;

  private int minProperties;

  @Override
  protected void initOptions(Map<String,String> options) {
    minProperties = DEFAULT_MIN_PROPERTIES;
    if (options.containsKey(MIN_PROPERTIES)) {
      minProperties = Integer.parseInt(options.get(MIN_PROPERTIES));
    }
  }

  @Override
  protected Map<String,String> getOptionsForCopy() {
    return Map.of(MIN_PROPERTIES, String.valueOf(minProperties));
  }

  /**
   * Not used — this iterator overrides {@link #seek} to perform row-aware buffering instead of the
   * per-entry {@link #shouldMark}/{@link #createMarker} pattern.
   */
  @Override
  protected boolean shouldMark(Key key, Value value) {
    return false;
  }

  /**
   * Not used — markers are created during row-aware buffering in {@link #seek}.
   */
  @Override
  protected Map.Entry<Key,Value> createMarker(Key key, Value value) {
    return null;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    List<Map.Entry<Key,Value>> allResults = new ArrayList<>();
    getSource().seek(range, columnFamilies, inclusive);

    List<Map.Entry<Key,Value>> rowBuffer = new ArrayList<>();
    String currentRow = null;

    while (getSource().hasTop()) {
      Key key = new Key(getSource().getTopKey());
      Value value = new Value(getSource().getTopValue());
      String row = key.getRow().toString();

      if (currentRow != null && !row.equals(currentRow)) {
        processRow(currentRow, rowBuffer, allResults);
        rowBuffer.clear();
      }

      currentRow = row;
      rowBuffer.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
      getSource().next();
    }

    if (!rowBuffer.isEmpty()) {
      processRow(currentRow, rowBuffer, allResults);
    }

    allResults.sort(Comparator.comparing(Map.Entry::getKey));
    setResults(allResults);
  }

  private void processRow(String row, List<Map.Entry<Key,Value>> rowEntries,
      List<Map.Entry<Key,Value>> output) {
    boolean hasSummary = false;
    int nonSystemPropertyCount = 0;
    ColumnVisibility compoundVis = null;

    for (Map.Entry<Key,Value> entry : rowEntries) {
      output.add(entry);

      Key key = entry.getKey();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
        if (colQual.equals(GraphSchema.SUMMARY_COLQUAL)) {
          hasSummary = true;
        } else if (!colQual.startsWith("_")) {
          nonSystemPropertyCount++;
          ColumnVisibility entryVis = new ColumnVisibility(key.getColumnVisibility());
          compoundVis = (compoundVis == null) ? entryVis
              : GraphSchema.compoundVisibility(compoundVis, entryVis);
        }
      }
    }

    if (!hasSummary && nonSystemPropertyCount >= minProperties && compoundVis != null) {
      Key markerKey =
          new Key(row, MARKER_COLFAM, GraphSchema.SUMMARY_COLQUAL, compoundVis.toString());

      String json =
          "{\"sourceVisibility\": \"" + escapeJson(new String(compoundVis.getExpression(), UTF_8))
              + "\", \"vertexId\": \"" + escapeJson(row) + "\"}";

      Value markerValue = new Value(json.getBytes(UTF_8));
      output.add(new AbstractMap.SimpleImmutableEntry<>(markerKey, markerValue));
    }
  }

  // Uses the protected setResults(List) method inherited from PendingMarkerIterator.

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r",
        "\\r");
  }
}
