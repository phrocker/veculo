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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class PendingMarkerIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  /**
   * Concrete test subclass that marks entries where the column qualifier starts with "text".
   */
  private static class TextMarkerIterator extends PendingMarkerIterator {

    @Override
    protected boolean shouldMark(Key key, Value value) {
      return key.getColumnFamily().toString().equals(GraphSchema.VERTEX_COLFAM)
          && key.getColumnQualifier().toString().startsWith("text");
    }

    @Override
    protected Map.Entry<Key,Value> createMarker(Key key, Value value) {
      Key markerKey =
          new Key(key.getRow().toString(), GraphSchema.PENDING_COLFAM_PREFIX + "test_marker",
              key.getColumnQualifier().toString(), key.getColumnVisibility().toString());
      Value markerValue = new Value("marker");
      return new AbstractMap.SimpleImmutableEntry<>(markerKey, markerValue);
    }
  }

  private TreeMap<Key,Value> buildTestData() {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "textContent"), new Value("This is some text content"));
    data.put(new Key("v1", "V", "textTitle"), new Value("My Title"));
    data.put(new Key("v1", "V", "author"), new Value("Alice"));
    data.put(new Key("v2", "V", "_label"), new Value("person"));
    data.put(new Key("v2", "V", "name"), new Value("Bob"));
    return data;
  }

  private List<Key> collectKeys(PendingMarkerIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    TextMarkerIterator iter = new TextMarkerIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // All original entries should be present
    for (Key originalKey : data.keySet()) {
      assertTrue(keys.stream().anyMatch(k -> k.equals(originalKey)),
          "Original entry missing: " + originalKey);
    }
  }

  @Test
  public void testMarkersGeneratedForMatchingEntries() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    TextMarkerIterator iter = new TextMarkerIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Should have markers for textContent and textTitle on v1
    long markerCount = keys.stream()
        .filter(k -> k.getColumnFamily().toString().startsWith(GraphSchema.PENDING_COLFAM_PREFIX))
        .count();
    assertEquals(2, markerCount, "Expected 2 markers for text properties");
  }

  @Test
  public void testCombinedOutputIsSortedByKey() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    TextMarkerIterator iter = new TextMarkerIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    for (int i = 1; i < keys.size(); i++) {
      assertTrue(keys.get(i - 1).compareTo(keys.get(i)) <= 0,
          "Keys not sorted at index " + i + ": " + keys.get(i - 1) + " > " + keys.get(i));
    }
  }

  @Test
  public void testNonMatchingEntriesDontGetMarkers() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    TextMarkerIterator iter = new TextMarkerIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // v2 has no text* properties, so no markers should be on v2 rows
    long v2MarkerCount = keys.stream()
        .filter(k -> k.getRow().toString().equals("v2")
            && k.getColumnFamily().toString().startsWith(GraphSchema.PENDING_COLFAM_PREFIX))
        .count();
    assertEquals(0, v2MarkerCount, "v2 should have no markers");

    // _label and author on v1 should not produce markers
    assertFalse(keys.stream()
        .anyMatch(k -> k.getColumnFamily().toString().startsWith(GraphSchema.PENDING_COLFAM_PREFIX)
            && k.getColumnQualifier().toString().equals("_label")));
    assertFalse(keys.stream()
        .anyMatch(k -> k.getColumnFamily().toString().startsWith(GraphSchema.PENDING_COLFAM_PREFIX)
            && k.getColumnQualifier().toString().equals("author")));
  }

  @Test
  public void testTotalEntryCount() throws IOException {
    TreeMap<Key,Value> data = buildTestData();
    TextMarkerIterator iter = new TextMarkerIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // 6 original entries + 2 markers = 8 total
    assertEquals(8, keys.size());
  }
}
