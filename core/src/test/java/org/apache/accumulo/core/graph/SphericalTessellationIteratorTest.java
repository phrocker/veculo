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

public class SphericalTessellationIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private Map<Key,Value> collectEntries(SphericalTessellationIterator iter) throws IOException {
    Map<Key,Value> entries = new TreeMap<>();
    while (iter.hasTop()) {
      entries.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
      iter.next();
    }
    return entries;
  }

  private List<Key> collectKeys(SphericalTessellationIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testCellIdPropertiesAreAdded() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label", "public"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "public"),
        Value.newVector(new float[] {0.0f, 1.0f, 0.0f}));

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // Both vertices should get _cell_id entries
    boolean v1HasCellId = entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v1")
        && k.getColumnQualifier().toString().equals(GraphSchema.CELL_ID_COLQUAL));
    boolean v2HasCellId = entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v2")
        && k.getColumnQualifier().toString().equals(GraphSchema.CELL_ID_COLQUAL));

    assertTrue(v1HasCellId, "v1 should have a _cell_id property");
    assertTrue(v2HasCellId, "v2 should have a _cell_id property");
  }

  @Test
  public void testCellIdValuesAreLongs() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    Map.Entry<Key,Value> cellIdEntry = entries.entrySet().stream()
        .filter(e -> e.getKey().getColumnQualifier().toString().equals(GraphSchema.CELL_ID_COLQUAL))
        .findFirst().orElse(null);

    assertNotNull(cellIdEntry, "Should have a _cell_id entry");
    String cellIdStr = new String(cellIdEntry.getValue().get());

    // Should be parseable as a long
    long cellId = Long.parseLong(cellIdStr);
    assertTrue(cellId >= 0, "Cell ID should be non-negative");
  }

  @Test
  public void testVertexWithoutEmbeddingDoesNotGetCellId() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "name", "public"), new Value("Alice"));
    // No _embedding for v1

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    boolean hasCellId = entries.keySet().stream()
        .anyMatch(k -> k.getColumnQualifier().toString().equals(GraphSchema.CELL_ID_COLQUAL));
    assertFalse(hasCellId, "Vertex without embedding should not get a _cell_id property");
  }

  @Test
  public void testOriginalEntriesPassThrough() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v1", "V", "name", "public"), new Value("Alice"));

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // All original entries should be present
    for (Key originalKey : data.keySet()) {
      assertTrue(keys.stream().anyMatch(k -> k.equals(originalKey)),
          "Original entry should pass through: " + originalKey);
    }
  }

  @Test
  public void testCustomDepthOption() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));

    Map<String,String> options = new HashMap<>();
    options.put(SphericalTessellationIterator.DEPTH, "3");

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    boolean hasCellId = entries.keySet().stream()
        .anyMatch(k -> k.getColumnQualifier().toString().equals(GraphSchema.CELL_ID_COLQUAL));
    assertTrue(hasCellId, "Should still produce _cell_id with custom depth");
  }

  @Test
  public void testCellIdInheritsVisibility() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "secret"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "secret"),
        Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    Map.Entry<Key,Value> cellIdEntry = entries.entrySet().stream()
        .filter(e -> e.getKey().getColumnQualifier().toString().equals(GraphSchema.CELL_ID_COLQUAL))
        .findFirst().orElse(null);

    assertNotNull(cellIdEntry, "Should have a _cell_id entry");
    assertEquals("secret", cellIdEntry.getKey().getColumnVisibility().toString(),
        "_cell_id should inherit the embedding's visibility");
  }

  @Test
  public void testResultsAreSorted() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding"), Value.newVector(new float[] {1.0f, 0.0f, 0.0f}));
    data.put(new Key("v2", "V", "_label"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding"), Value.newVector(new float[] {0.0f, 1.0f, 0.0f}));

    SphericalTessellationIterator iter = new SphericalTessellationIterator();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    for (int i = 1; i < keys.size(); i++) {
      assertTrue(keys.get(i - 1).compareTo(keys.get(i)) <= 0, "Results should be sorted by Key");
    }
  }
}
