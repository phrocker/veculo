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

import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class VectorSeekIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private TreeMap<Key,Value> buildVidxData(String[] vertexIds, float[][] embeddings) {
    TreeMap<Key,Value> data = new TreeMap<>();
    for (int i = 0; i < vertexIds.length; i++) {
      long cellId = SphericalTessellation.assignCellId(embeddings[i]);
      String rowKey = VectorIndexTable.rowKey(cellId, vertexIds[i]);
      data.put(new Key(rowKey, "V", "_embedding"), Value.newVector(embeddings[i]));
    }
    return data;
  }

  private List<Key> collectKeys(VectorSeekIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testExactMatchReturned() throws IOException {
    float[] queryVector = {1.0f, 0.0f, 0.0f};
    float[][] embeddings = {{1.0f, 0.0f, 0.0f}, {0.0f, 1.0f, 0.0f}};
    String[] ids = {"v1", "v2"};

    TreeMap<Key,Value> data = buildVidxData(ids, embeddings);

    Map<String,String> options = new HashMap<>();
    options.put(VectorSeekIterator.QUERY_VECTOR_OPTION, "1.0,0.0,0.0");
    options.put(VectorSeekIterator.TOP_K_OPTION, "10");
    options.put(VectorSeekIterator.THRESHOLD_OPTION, "0.9");

    VectorSeekIterator iter = new VectorSeekIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> results = collectKeys(iter);
    // At minimum the exact match (v1) should be returned
    assertTrue(results.size() >= 1, "Should return at least the exact match");

    // Find the exact match entry
    boolean foundExactMatch = results.stream()
        .anyMatch(k -> VectorIndexTable.extractVertexId(k.getRow().toString()).equals("v1"));
    assertTrue(foundExactMatch, "Exact match v1 should be in results");
  }

  @Test
  public void testTopKLimit() throws IOException {
    // Create 10 candidates with varying similarity
    float[][] embeddings = new float[10][];
    String[] ids = new String[10];
    for (int i = 0; i < 10; i++) {
      embeddings[i] = new float[] {1.0f - i * 0.05f, i * 0.05f, 0.0f};
      ids[i] = "v" + i;
    }

    TreeMap<Key,Value> data = buildVidxData(ids, embeddings);

    Map<String,String> options = new HashMap<>();
    options.put(VectorSeekIterator.QUERY_VECTOR_OPTION, "1.0,0.0,0.0");
    options.put(VectorSeekIterator.TOP_K_OPTION, "3");
    options.put(VectorSeekIterator.THRESHOLD_OPTION, "0.0");

    VectorSeekIterator iter = new VectorSeekIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> results = collectKeys(iter);
    assertTrue(results.size() <= 3, "Should return at most topK=3 results, got " + results.size());
  }

  @Test
  public void testThresholdFiltering() throws IOException {
    float[] queryVector = {1.0f, 0.0f, 0.0f};
    // v1 is very similar, v2 is perpendicular (similarity ≈ 0)
    float[][] embeddings = {{0.99f, 0.1f, 0.0f}, {0.0f, 1.0f, 0.0f}};
    String[] ids = {"v1", "v2"};

    TreeMap<Key,Value> data = buildVidxData(ids, embeddings);

    Map<String,String> options = new HashMap<>();
    options.put(VectorSeekIterator.QUERY_VECTOR_OPTION, "1.0,0.0,0.0");
    options.put(VectorSeekIterator.TOP_K_OPTION, "10");
    options.put(VectorSeekIterator.THRESHOLD_OPTION, "0.9");

    VectorSeekIterator iter = new VectorSeekIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> results = collectKeys(iter);
    // v2 should be filtered out by the 0.9 threshold
    for (Key k : results) {
      String vertexId = VectorIndexTable.extractVertexId(k.getRow().toString());
      assertTrue(!"v2".equals(vertexId) || results.size() == 0,
          "v2 (perpendicular) should not pass threshold 0.9");
    }
  }

  @Test
  public void testResultsSortedByKey() throws IOException {
    float[][] embeddings = {{1.0f, 0.0f, 0.0f}, {0.9f, 0.1f, 0.0f}, {0.8f, 0.2f, 0.0f}};
    String[] ids = {"v1", "v2", "v3"};

    TreeMap<Key,Value> data = buildVidxData(ids, embeddings);

    Map<String,String> options = new HashMap<>();
    options.put(VectorSeekIterator.QUERY_VECTOR_OPTION, "1.0,0.0,0.0");
    options.put(VectorSeekIterator.TOP_K_OPTION, "10");
    options.put(VectorSeekIterator.THRESHOLD_OPTION, "0.0");

    VectorSeekIterator iter = new VectorSeekIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> results = collectKeys(iter);
    // Verify results are sorted by key (SortedKeyValueIterator contract)
    for (int i = 1; i < results.size(); i++) {
      assertTrue(results.get(i - 1).compareTo(results.get(i)) <= 0,
          "Results should be sorted by key");
    }
  }

  @Test
  public void testEmptyRange() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>(); // no data

    Map<String,String> options = new HashMap<>();
    options.put(VectorSeekIterator.QUERY_VECTOR_OPTION, "1.0,0.0,0.0");
    options.put(VectorSeekIterator.TOP_K_OPTION, "10");
    options.put(VectorSeekIterator.THRESHOLD_OPTION, "0.0");

    VectorSeekIterator iter = new VectorSeekIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertFalse(iter.hasTop(), "Empty data should produce no results");
  }

  @Test
  public void testDeepCopy() throws IOException {
    float[][] embeddings = {{1.0f, 0.0f, 0.0f}};
    String[] ids = {"v1"};
    TreeMap<Key,Value> data = buildVidxData(ids, embeddings);

    Map<String,String> options = new HashMap<>();
    options.put(VectorSeekIterator.QUERY_VECTOR_OPTION, "1.0,0.0,0.0");
    options.put(VectorSeekIterator.TOP_K_OPTION, "10");
    options.put(VectorSeekIterator.THRESHOLD_OPTION, "0.0");

    VectorSeekIterator iter = new VectorSeekIterator();
    iter.init(new SortedMapIterator(data), options, null);

    // deepCopy should not throw
    SortedKeyValueIterator<Key,Value> copy = iter.deepCopy(null);
    assertTrue(copy instanceof VectorSeekIterator);
  }
}
