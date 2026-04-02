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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Minor compaction iterator that detects vertices with text properties but no {@code _embedding}
 * column qualifier. When such a vertex is found, a {@code P_auto_embed} pending marker is emitted
 * so that an external worker can generate and write the embedding asynchronously.
 *
 * <p>
 * This iterator performs row-aware buffering: it collects all entries for each row (vertex), checks
 * whether an embedding already exists, and if not, emits markers for text properties that exceed
 * the minimum text length.
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code textProperties} — comma-separated list of property names to consider as text
 * sources</li>
 * <li>{@code minTextLength} — minimum text length to trigger embedding (default 10)</li>
 * </ul>
 */
public class AutoEmbedIterator extends PendingMarkerIterator {

  public static final String TEXT_PROPERTIES = "textProperties";
  public static final String MIN_TEXT_LENGTH = "minTextLength";

  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "auto_embed";
  private static final int DEFAULT_MIN_TEXT_LENGTH = 10;

  private Set<String> textProperties;
  private int minTextLength;

  @Override
  protected void initOptions(Map<String,String> options) {
    textProperties = new HashSet<>();
    if (options.containsKey(TEXT_PROPERTIES)) {
      for (String prop : options.get(TEXT_PROPERTIES).split(",")) {
        String trimmed = prop.trim();
        if (!trimmed.isEmpty()) {
          textProperties.add(trimmed);
        }
      }
    }
    minTextLength = DEFAULT_MIN_TEXT_LENGTH;
    if (options.containsKey(MIN_TEXT_LENGTH)) {
      minTextLength = Integer.parseInt(options.get(MIN_TEXT_LENGTH));
    }
  }

  @Override
  protected Map<String,String> getOptionsForCopy() {
    if (textProperties.isEmpty()) {
      return Map.of(MIN_TEXT_LENGTH, String.valueOf(minTextLength));
    }
    return Map.of(TEXT_PROPERTIES, String.join(",", textProperties), MIN_TEXT_LENGTH,
        String.valueOf(minTextLength));
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
    // We need direct access to source and results, so we replicate the base seek logic
    // with row-aware buffering
    List<Map.Entry<Key,Value>> allResults = new ArrayList<>();
    getSource().seek(range, columnFamilies, inclusive);

    List<Map.Entry<Key,Value>> rowBuffer = new ArrayList<>();
    String currentRow = null;

    while (getSource().hasTop()) {
      Key key = new Key(getSource().getTopKey());
      Value value = new Value(getSource().getTopValue());
      String row = key.getRow().toString();

      if (currentRow != null && !row.equals(currentRow)) {
        // Process the completed row
        processRow(currentRow, rowBuffer, allResults);
        rowBuffer.clear();
      }

      currentRow = row;
      rowBuffer.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
      getSource().next();
    }

    // Process the last row
    if (!rowBuffer.isEmpty()) {
      processRow(currentRow, rowBuffer, allResults);
    }

    allResults.sort(Comparator.comparing(Map.Entry::getKey));
    setResults(allResults);
  }

  private void processRow(String row, List<Map.Entry<Key,Value>> rowEntries,
      List<Map.Entry<Key,Value>> output) {
    boolean hasEmbedding = false;
    List<Map.Entry<Key,Value>> textEntries = new ArrayList<>();

    for (Map.Entry<Key,Value> entry : rowEntries) {
      // Always pass through the original entry
      output.add(entry);

      Key key = entry.getKey();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
        if (colQual.equals(GraphSchema.EMBEDDING_COLQUAL)) {
          hasEmbedding = true;
        } else if (!colQual.startsWith("_") && isTextProperty(colQual)) {
          String text = new String(entry.getValue().get(), UTF_8);
          if (text.length() >= minTextLength) {
            textEntries.add(entry);
          }
        }
      }
    }

    if (!hasEmbedding && !textEntries.isEmpty()) {
      for (Map.Entry<Key,Value> textEntry : textEntries) {
        Key srcKey = textEntry.getKey();
        String propName = srcKey.getColumnQualifier().toString();
        String text = new String(textEntry.getValue().get(), UTF_8);
        ColumnVisibility srcVis = new ColumnVisibility(srcKey.getColumnVisibility());

        Key markerKey =
            new Key(srcKey.getRow().toString(), MARKER_COLFAM, propName, srcVis.toString());

        String json = "{\"sourceVisibility\": \""
            + escapeJson(new String(srcVis.getExpression(), UTF_8)) + "\", \"textProperty\": \""
            + escapeJson(propName) + "\", \"text\": \"" + escapeJson(text) + "\"}";

        Value markerValue = new Value(json.getBytes(UTF_8));
        output.add(new AbstractMap.SimpleImmutableEntry<>(markerKey, markerValue));
      }
    }
  }

  private boolean isTextProperty(String propName) {
    return textProperties.isEmpty() || textProperties.contains(propName);
  }

  // Uses the protected setResults(List) method from PendingMarkerIterator
  // to replace the results buffer after row-aware processing.

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r",
        "\\r");
  }
}
