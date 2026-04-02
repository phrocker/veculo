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

public class EntityExtractIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "entity_extract";

  private List<Key> collectKeys(EntityExtractIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testLongTextPropertyGetsMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    // Text longer than default minTextLength (50)
    data.put(new Key("v1", "V", "description"), new Value(
        "This is a very long description text that exceeds the minimum text length threshold for entity extraction processing"));

    EntityExtractIterator iter = new EntityExtractIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(1, markerCount, "Should produce a P_entity_extract marker for long text");
  }

  @Test
  public void testShortTextDoesNotTriggerMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "description"), new Value("Short text"));

    EntityExtractIterator iter = new EntityExtractIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Short text should not trigger entity extraction marker");
  }

  @Test
  public void testSystemPropertiesDontTriggerMarkers() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value(
        "This is a very long label that would exceed the min text length if it were a regular property"));
    data.put(new Key("v1", "V", "_summary"), new Value(
        "This is a very long summary that would exceed the min text length if it were a regular property"));
    data.put(new Key("v1", "V", "_embedding_model"), new Value(
        "This is a very long model string that would exceed the min text length if it were a regular property"));

    EntityExtractIterator iter = new EntityExtractIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "System properties (starting with _) should not trigger markers");
  }

  @Test
  public void testCustomMinTextLength() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "title"), new Value("A Medium-Length Title"));

    Map<String,String> options = new HashMap<>();
    options.put(EntityExtractIterator.MIN_TEXT_LENGTH, "10");

    EntityExtractIterator iter = new EntityExtractIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(1, markerCount,
        "With minTextLength=10, 'A Medium-Length Title' should trigger a marker");
  }

  @Test
  public void testEdgeEntriesNotMarked() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "E_knows", "v2"), new Value(
        "This is edge property data that is very long and exceeds any reasonable minimum text length"));

    EntityExtractIterator iter = new EntityExtractIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Edge entries should not trigger entity extraction markers");
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("document"));
    data.put(new Key("v1", "V", "description"), new Value(
        "This is a very long description text that exceeds the minimum text length threshold"));

    EntityExtractIterator iter = new EntityExtractIterator();
    iter.init(new SortedMapIterator(data), Collections.emptyMap(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // Original entries plus markers
    assertTrue(keys.stream().anyMatch(k -> k.getColumnFamily().toString().equals("V")
        && k.getColumnQualifier().toString().equals("_label")));
    assertTrue(keys.stream().anyMatch(k -> k.getColumnFamily().toString().equals("V")
        && k.getColumnQualifier().toString().equals("description")));
  }
}
