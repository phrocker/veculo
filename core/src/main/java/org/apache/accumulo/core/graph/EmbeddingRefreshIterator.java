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
 * Major compaction iterator that detects stale embeddings by comparing the {@code _embedding_model}
 * column qualifier value against a configured current model name. When the stored model differs
 * from the current model, a {@code P_reembed} pending marker is emitted so that an external worker
 * can regenerate the embedding using the current model.
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code currentModel} — the expected embedding model name; entries with a different model will
 * trigger re-embedding</li>
 * </ul>
 */
public class EmbeddingRefreshIterator extends PendingMarkerIterator {

  public static final String CURRENT_MODEL = "currentModel";

  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "reembed";

  private String currentModel;

  @Override
  protected void initOptions(Map<String,String> options) {
    currentModel = options.get(CURRENT_MODEL);
  }

  @Override
  protected Map<String,String> getOptionsForCopy() {
    if (currentModel == null) {
      return Map.of();
    }
    return Map.of(CURRENT_MODEL, currentModel);
  }

  @Override
  protected boolean shouldMark(Key key, Value value) {
    if (currentModel == null) {
      return false;
    }

    String colFam = key.getColumnFamily().toString();
    String colQual = key.getColumnQualifier().toString();

    // Only consider _embedding_model entries on vertex rows
    if (!colFam.equals(GraphSchema.VERTEX_COLFAM)
        || !colQual.equals(GraphSchema.EMBEDDING_MODEL_COLQUAL)) {
      return false;
    }

    String storedModel = new String(value.get(), UTF_8);
    return !currentModel.equals(storedModel);
  }

  @Override
  protected Map.Entry<Key,Value> createMarker(Key key, Value value) {
    String storedModel = new String(value.get(), UTF_8);
    ColumnVisibility srcVis = new ColumnVisibility(key.getColumnVisibility());

    Key markerKey = new Key(key.getRow().toString(), MARKER_COLFAM, GraphSchema.EMBEDDING_COLQUAL,
        srcVis.toString());

    String json = "{\"sourceVisibility\": \""
        + escapeJson(new String(srcVis.getExpression(), UTF_8)) + "\", \"currentModel\": \""
        + escapeJson(currentModel) + "\", \"oldModel\": \"" + escapeJson(storedModel) + "\"}";

    Value markerValue = new Value(json.getBytes(UTF_8));
    return new AbstractMap.SimpleImmutableEntry<>(markerKey, markerValue);
  }

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r",
        "\\r");
  }
}
