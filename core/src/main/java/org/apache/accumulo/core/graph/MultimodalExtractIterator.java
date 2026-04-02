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
 * Minor compaction iterator that detects file vertices (vertices with a {@code _file_uri} property)
 * that have not yet been processed for multimodal extraction. When such a vertex is found, a
 * {@code P_multimodal_extract} pending marker is emitted so that an external worker can perform
 * content extraction (e.g., OCR, transcription, keyframe extraction) asynchronously.
 *
 * <p>
 * This iterator performs row-aware buffering: it collects all entries for each row (vertex), checks
 * whether the vertex has a {@code _file_uri} property and whether it has already been marked as
 * {@code _extracted}. If the vertex has a file URI but has not been extracted, a marker is emitted.
 *
 * <p>
 * The marker value is a JSON object containing:
 * <ul>
 * <li>{@code fileUri} — the GCS URI of the file to process</li>
 * <li>{@code contentType} — the MIME type of the file (if available)</li>
 * <li>{@code sourceVisibility} — the visibility expression for ABAC propagation</li>
 * <li>{@code vertexId} — the vertex ID (row key) of the file vertex</li>
 * </ul>
 *
 * <p>
 * No configuration options are needed.
 */
public class MultimodalExtractIterator extends PendingMarkerIterator {

  private static final String MARKER_COLFAM =
      GraphSchema.PENDING_COLFAM_PREFIX + "multimodal_extract";

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
    // Row-aware buffering: collect all entries per row before deciding whether to emit markers
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
    String fileUri = null;
    String contentType = null;
    boolean extracted = false;
    ColumnVisibility sourceVisibility = null;

    for (Map.Entry<Key,Value> entry : rowEntries) {
      // Always pass through the original entry
      output.add(entry);

      Key key = entry.getKey();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
        if (colQual.equals(GraphSchema.FILE_URI_COLQUAL)) {
          fileUri = new String(entry.getValue().get(), UTF_8);
          sourceVisibility = new ColumnVisibility(key.getColumnVisibility());
        } else if (colQual.equals(GraphSchema.EXTRACTED_COLQUAL)) {
          extracted = true;
        } else if (colQual.equals(GraphSchema.CONTENT_TYPE_COLQUAL)) {
          contentType = new String(entry.getValue().get(), UTF_8);
        }
      }
    }

    // Emit marker if vertex has a file URI but has not been extracted yet
    if (fileUri != null && !extracted) {
      if (sourceVisibility == null) {
        sourceVisibility = new ColumnVisibility();
      }

      Key markerKey =
          new Key(row, MARKER_COLFAM, GraphSchema.FILE_URI_COLQUAL, sourceVisibility.toString());

      String json = "{\"fileUri\": \"" + escapeJson(fileUri) + "\", \"contentType\": \""
          + escapeJson(contentType != null ? contentType : "") + "\", \"sourceVisibility\": \""
          + escapeJson(new String(sourceVisibility.getExpression(), UTF_8)) + "\", \"vertexId\": \""
          + escapeJson(row) + "\"}";

      Value markerValue = new Value(json.getBytes(UTF_8));
      output.add(new AbstractMap.SimpleImmutableEntry<>(markerKey, markerValue));
    }
  }

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r",
        "\\r");
  }
}
