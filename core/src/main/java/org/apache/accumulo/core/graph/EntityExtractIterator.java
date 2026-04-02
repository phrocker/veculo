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

import java.util.AbstractMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Minor compaction iterator that detects text vertex properties suitable for entity extraction.
 * Vertex properties with column family {@code V} and a non-system column qualifier (not starting
 * with {@code _}) whose text exceeds the minimum length threshold will produce a
 * {@code P_entity_extract} pending marker.
 *
 * <p>
 * An external worker polls for these markers and runs named entity recognition on the text,
 * creating new vertices and edges for extracted entities.
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code minTextLength} — minimum text length to trigger extraction (default 50)</li>
 * </ul>
 */
public class EntityExtractIterator extends PendingMarkerIterator {

  public static final String MIN_TEXT_LENGTH = "minTextLength";

  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "entity_extract";
  private static final int DEFAULT_MIN_TEXT_LENGTH = 50;

  private int minTextLength;

  @Override
  protected void initOptions(Map<String,String> options) {
    minTextLength = DEFAULT_MIN_TEXT_LENGTH;
    if (options.containsKey(MIN_TEXT_LENGTH)) {
      minTextLength = Integer.parseInt(options.get(MIN_TEXT_LENGTH));
    }
  }

  @Override
  protected Map<String,String> getOptionsForCopy() {
    return Map.of(MIN_TEXT_LENGTH, String.valueOf(minTextLength));
  }

  @Override
  protected boolean shouldMark(Key key, Value value) {
    String colFam = key.getColumnFamily().toString();
    String colQual = key.getColumnQualifier().toString();

    // Only consider vertex properties (ColFam="V") with non-system qualifiers
    if (!colFam.equals(GraphSchema.VERTEX_COLFAM) || colQual.startsWith("_")) {
      return false;
    }

    String text = new String(value.get(), UTF_8);
    return text.length() >= minTextLength;
  }

  @Override
  protected Map.Entry<Key,Value> createMarker(Key key, Value value) {
    String propName = key.getColumnQualifier().toString();
    String text = new String(value.get(), UTF_8);
    ColumnVisibility srcVis = new ColumnVisibility(key.getColumnVisibility());

    Key markerKey = new Key(key.getRow().toString(), MARKER_COLFAM, propName, srcVis.toString());

    String json = "{\"sourceVisibility\": \""
        + escapeJson(new String(srcVis.getExpression(), UTF_8)) + "\", \"textProperty\": \""
        + escapeJson(propName) + "\", \"text\": \"" + escapeJson(text) + "\"}";

    Value markerValue = new Value(json.getBytes(UTF_8));
    return new AbstractMap.SimpleImmutableEntry<>(markerKey, markerValue);
  }

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r",
        "\\r");
  }
}
