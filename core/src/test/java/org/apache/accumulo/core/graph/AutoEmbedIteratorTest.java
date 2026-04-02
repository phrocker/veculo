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

public class AutoEmbedIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "auto_embed";

  private List<Key> collectKeys(AutoEmbedIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  private Map<Key,Value> collectEntries(AutoEmbedIterator iter) throws IOException {
    Map<Key,Value> entries = new TreeMap<>();
    while (iter.hasTop()) {
      entries.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
      iter.next();
    }
    return entries;
  }

  @Test
  public void testVertexWithTextButNoEmbeddingGetsMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "description"),
        new Value("This is a long enough text property for embedding generation"));

    AutoEmbedIterator iter = new AutoEmbedIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(1, markerCount, "Should produce exactly one P_auto_embed marker");
  }

  @Test
  public void testVertexWithEmbeddingGetsNoMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v1", "V", "description"),
        new Value("This is a long enough text property for embedding generation"));

    AutoEmbedIterator iter = new AutoEmbedIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Vertex with _embedding should NOT get a marker");
  }

  @Test
  public void testShortTextBelowMinLengthGetsNoMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "title"), new Value("Short"));

    AutoEmbedIterator iter = new AutoEmbedIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Short text should not trigger marker");
  }

  @Test
  public void testMarkerValueContainsSourceVisibility() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "public"), new Value("document"));
    data.put(new Key("v1", "V", "description", "secret"),
        new Value("This is a long enough text property for embedding generation"));

    AutoEmbedIterator iter = new AutoEmbedIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);
    Map.Entry<Key,
        Value> markerEntry = entries.entrySet().stream()
            .filter(e -> e.getKey().getColumnFamily().toString().equals(MARKER_COLFAM)).findFirst()
            .orElse(null);
    assertNotNull(markerEntry, "Marker entry should exist");
    String markerValue = new String(markerEntry.getValue().get());
    assertTrue(markerValue.contains("sourceVisibility"),
        "Marker value should contain sourceVisibility");
    assertTrue(markerValue.contains("secret"),
        "Marker value should contain the source visibility expression");
  }

  @Test
  public void testCustomTextPropertiesOption() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "description"),
        new Value("This is a long enough text property for embedding generation"));
    data.put(new Key("v1", "V", "notes"),
        new Value("These are also long enough notes that should be ignored by custom filter"));

    Map<String,String> options = new HashMap<>();
    options.put(AutoEmbedIterator.TEXT_PROPERTIES, "description");

    AutoEmbedIterator iter = new AutoEmbedIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(1, markerCount,
        "Only the configured textProperty 'description' should produce a marker");

    assertTrue(
        keys.stream()
            .anyMatch(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)
                && k.getColumnQualifier().toString().equals("description")),
        "Marker should be for 'description' property");
  }

  @Test
  public void testSystemPropertiesNotMarked() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"),
        new Value("This is actually a really long label value for testing"));
    data.put(new Key("v1", "V", "_summary"),
        new Value("This is a really long summary value that should not be marked"));

    AutoEmbedIterator iter = new AutoEmbedIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "System properties (starting with _) should not be marked");
  }
}
