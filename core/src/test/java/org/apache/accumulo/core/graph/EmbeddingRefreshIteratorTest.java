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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class EmbeddingRefreshIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "reembed";

  private List<Key> collectKeys(EmbeddingRefreshIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  private Map<Key,Value> collectEntries(EmbeddingRefreshIterator iter) throws IOException {
    Map<Key,Value> entries = new TreeMap<>();
    while (iter.hasTop()) {
      entries.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
      iter.next();
    }
    return entries;
  }

  @Test
  public void testStaleModelGetsReembedMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v1", "V", "_embedding_model"), new Value("old-model-v1"));

    Map<String,String> options = new HashMap<>();
    options.put(EmbeddingRefreshIterator.CURRENT_MODEL, "new-model-v2");

    EmbeddingRefreshIterator iter = new EmbeddingRefreshIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(1, markerCount, "Vertex with old model should get P_reembed marker");
  }

  @Test
  public void testMatchingModelGetsNoMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v1", "V", "_embedding_model"), new Value("current-model"));

    Map<String,String> options = new HashMap<>();
    options.put(EmbeddingRefreshIterator.CURRENT_MODEL, "current-model");

    EmbeddingRefreshIterator iter = new EmbeddingRefreshIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Vertex with matching model should NOT get P_reembed marker");
  }

  @Test
  public void testVertexWithoutEmbeddingModelGetsNoMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    // No _embedding_model entry

    Map<String,String> options = new HashMap<>();
    options.put(EmbeddingRefreshIterator.CURRENT_MODEL, "new-model-v2");

    EmbeddingRefreshIterator iter = new EmbeddingRefreshIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Vertex without _embedding_model should NOT get P_reembed marker");
  }

  @Test
  public void testNoCurrentModelConfigured() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding_model"), new Value("old-model"));

    EmbeddingRefreshIterator iter = new EmbeddingRefreshIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount,
        "Without currentModel option configured, no markers should be emitted");
  }

  @Test
  public void testMarkerContainsModelInfo() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "secret"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding_model", "secret"), new Value("old-model-v1"));

    Map<String,String> options = new HashMap<>();
    options.put(EmbeddingRefreshIterator.CURRENT_MODEL, "new-model-v2");

    EmbeddingRefreshIterator iter = new EmbeddingRefreshIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);
    Map.Entry<Key,
        Value> markerEntry = entries.entrySet().stream()
            .filter(e -> e.getKey().getColumnFamily().toString().equals(MARKER_COLFAM)).findFirst()
            .orElse(null);
    assertNotNull(markerEntry, "Should have a P_reembed marker");

    String markerValue = new String(markerEntry.getValue().get());
    assertTrue(markerValue.contains("currentModel"), "Marker value should contain currentModel");
    assertTrue(markerValue.contains("new-model-v2"),
        "Marker value should contain the new model name");
    assertTrue(markerValue.contains("oldModel"), "Marker value should contain oldModel");
    assertTrue(markerValue.contains("old-model-v1"),
        "Marker value should contain the old model name");
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding_model"), new Value("old-model"));

    Map<String,String> options = new HashMap<>();
    options.put(EmbeddingRefreshIterator.CURRENT_MODEL, "new-model");

    EmbeddingRefreshIterator iter = new EmbeddingRefreshIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // All original entries should be present
    assertTrue(keys.stream().anyMatch(k -> k.getColumnQualifier().toString().equals("_label")));
    assertTrue(
        keys.stream().anyMatch(k -> k.getColumnQualifier().toString().equals("_embedding_model")));
  }
}
