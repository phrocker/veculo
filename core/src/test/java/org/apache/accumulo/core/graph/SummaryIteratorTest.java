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

public class SummaryIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final String MARKER_COLFAM = GraphSchema.PENDING_COLFAM_PREFIX + "summary";

  private List<Key> collectKeys(SummaryIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  private Map<Key,Value> collectEntries(SummaryIterator iter) throws IOException {
    Map<Key,Value> entries = new TreeMap<>();
    while (iter.hasTop()) {
      entries.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
      iter.next();
    }
    return entries;
  }

  @Test
  public void testVertexWithPropertiesButNoSummaryGetsMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("person"));
    data.put(new Key("v1", "V", "name"), new Value("Alice"));
    data.put(new Key("v1", "V", "age"), new Value("30"));
    data.put(new Key("v1", "V", "email"), new Value("alice@example.com"));

    SummaryIterator iter = new SummaryIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(1, markerCount,
        "Vertex with 3 non-system properties and no _summary should get P_summary marker");
  }

  @Test
  public void testVertexWithSummaryAlreadyPresentGetsNoMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("person"));
    data.put(new Key("v1", "V", "_summary"), new Value("Alice is a 30-year-old person"));
    data.put(new Key("v1", "V", "name"), new Value("Alice"));
    data.put(new Key("v1", "V", "age"), new Value("30"));
    data.put(new Key("v1", "V", "email"), new Value("alice@example.com"));

    SummaryIterator iter = new SummaryIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount, "Vertex with existing _summary should NOT get a P_summary marker");
  }

  @Test
  public void testVertexWithFewerThanMinPropertiesGetsNoMarker() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("person"));
    data.put(new Key("v1", "V", "name"), new Value("Alice"));

    // Default minProperties is 2; "name" is only 1 non-system property
    SummaryIterator iter = new SummaryIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount,
        "Vertex with fewer than minProperties non-system properties should not get marker");
  }

  @Test
  public void testCompoundVisibilityInMarkerValue() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "public"), new Value("person"));
    data.put(new Key("v1", "V", "name", "alpha"), new Value("Alice"));
    data.put(new Key("v1", "V", "age", "beta"), new Value("30"));

    SummaryIterator iter = new SummaryIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    Map.Entry<Key,
        Value> markerEntry = entries.entrySet().stream()
            .filter(e -> e.getKey().getColumnFamily().toString().equals(MARKER_COLFAM)).findFirst()
            .orElse(null);
    assertNotNull(markerEntry, "Should have a P_summary marker");

    String markerValue = new String(markerEntry.getValue().get());
    assertTrue(markerValue.contains("sourceVisibility"),
        "Marker value should contain sourceVisibility");

    // The marker key's visibility should be compound of the non-system properties' visibilities
    String markerVis = markerEntry.getKey().getColumnVisibility().toString();
    assertTrue(markerVis.contains("alpha") && markerVis.contains("beta"),
        "Marker visibility should combine both property visibilities: " + markerVis);
  }

  @Test
  public void testCustomMinProperties() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("person"));
    data.put(new Key("v1", "V", "name"), new Value("Alice"));
    data.put(new Key("v1", "V", "age"), new Value("30"));
    data.put(new Key("v1", "V", "email"), new Value("alice@example.com"));

    Map<String,String> options = new HashMap<>();
    options.put(SummaryIterator.MIN_PROPERTIES, "5");

    SummaryIterator iter = new SummaryIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    long markerCount =
        keys.stream().filter(k -> k.getColumnFamily().toString().equals(MARKER_COLFAM)).count();
    assertEquals(0, markerCount,
        "With minProperties=5, vertex with 3 non-system properties should not get marker");
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("person"));
    data.put(new Key("v1", "V", "name"), new Value("Alice"));
    data.put(new Key("v1", "V", "age"), new Value("30"));

    SummaryIterator iter = new SummaryIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // All 3 original entries should be present
    assertTrue(keys.stream().anyMatch(k -> k.getColumnQualifier().toString().equals("_label")));
    assertTrue(keys.stream().anyMatch(k -> k.getColumnQualifier().toString().equals("name")));
    assertTrue(keys.stream().anyMatch(k -> k.getColumnQualifier().toString().equals("age")));
  }
}
