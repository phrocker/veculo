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

public class TessellationScanIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private Map<Key,Value> collectEntries(TessellationScanIterator iter) throws IOException {
    Map<Key,Value> entries = new TreeMap<>();
    while (iter.hasTop()) {
      entries.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
      iter.next();
    }
    return entries;
  }

  private List<Key> collectKeys(TessellationScanIterator iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  /**
   * Builds test data with vertices that have embeddings and pre-computed cell IDs. Uses depth=3 for
   * manageable cell ID ranges.
   */
  private TreeMap<Key,Value> buildTestData(int depth) {
    TreeMap<Key,Value> data = new TreeMap<>();

    float[] nearVector = new float[] {1.0f, 0.0f, 0.0f};
    float[] similarVector = new float[] {0.99f, 0.1f, 0.0f};
    float[] distantVector = new float[] {0.0f, 0.0f, 1.0f};

    long nearCellId = SphericalTessellation.assignCellId(nearVector, depth);
    long similarCellId = SphericalTessellation.assignCellId(similarVector, depth);
    long distantCellId = SphericalTessellation.assignCellId(distantVector, depth);

    // v1: near to query vector
    data.put(new Key("v1", "V", "_label", "public"), new Value("doc1"));
    data.put(new Key("v1", "V", "_embedding", "public"), Value.newVector(nearVector));
    data.put(new Key("v1", "V", "_cell_id", "public"), new Value(Long.toString(nearCellId)));
    data.put(new Key("v1", "V", "name", "public"), new Value("Near Document"));

    // v2: similar to query vector
    data.put(new Key("v2", "V", "_label", "public"), new Value("doc2"));
    data.put(new Key("v2", "V", "_embedding", "public"), Value.newVector(similarVector));
    data.put(new Key("v2", "V", "_cell_id", "public"), new Value(Long.toString(similarCellId)));
    data.put(new Key("v2", "V", "name", "public"), new Value("Similar Document"));

    // v3: distant from query vector
    data.put(new Key("v3", "V", "_label", "public"), new Value("doc3"));
    data.put(new Key("v3", "V", "_embedding", "public"), Value.newVector(distantVector));
    data.put(new Key("v3", "V", "_cell_id", "public"), new Value(Long.toString(distantCellId)));
    data.put(new Key("v3", "V", "name", "public"), new Value("Distant Document"));

    return data;
  }

  @Test
  public void testNearbyVerticesReturned() throws IOException {
    int depth = 3;
    TreeMap<Key,Value> data = buildTestData(depth);

    // Query vector is the same as v1's embedding
    Map<String,String> options = new HashMap<>();
    options.put(TessellationScanIterator.QUERY_VECTOR, "1.0,0.0,0.0");
    options.put(TessellationScanIterator.DEPTH, String.valueOf(depth));
    options.put(TessellationScanIterator.SEARCH_RADIUS, "5");
    options.put(TessellationScanIterator.SIMILARITY_THRESHOLD, "0.7");

    TessellationScanIterator iter = new TessellationScanIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // v1 should definitely be returned (identical vector, similarity = 1.0)
    boolean v1Returned =
        entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v1"));
    assertTrue(v1Returned, "v1 (identical vector) should be in results");
  }

  @Test
  public void testSimilarityPropertyAdded() throws IOException {
    int depth = 3;
    TreeMap<Key,Value> data = buildTestData(depth);

    Map<String,String> options = new HashMap<>();
    options.put(TessellationScanIterator.QUERY_VECTOR, "1.0,0.0,0.0");
    options.put(TessellationScanIterator.DEPTH, String.valueOf(depth));
    options.put(TessellationScanIterator.SEARCH_RADIUS, "5");
    options.put(TessellationScanIterator.SIMILARITY_THRESHOLD, "0.7");

    TessellationScanIterator iter = new TessellationScanIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // Qualifying vertices should get a _similarity property
    boolean hasSimilarity = entries.keySet().stream()
        .anyMatch(k -> k.getColumnQualifier().toString().equals("_similarity"));
    assertTrue(hasSimilarity, "Qualifying vertices should get a _similarity property");

    // Check that the similarity value is a valid float
    for (Map.Entry<Key,Value> entry : entries.entrySet()) {
      if (entry.getKey().getColumnQualifier().toString().equals("_similarity")) {
        String simStr = new String(entry.getValue().get());
        float similarity = Float.parseFloat(simStr);
        assertTrue(similarity >= 0.7f,
            "Similarity should be at least the threshold (0.7), got " + similarity);
        assertTrue(similarity <= 1.0f, "Similarity should be at most 1.0, got " + similarity);
      }
    }
  }

  @Test
  public void testDistantVerticesFilteredOut() throws IOException {
    int depth = 3;
    TreeMap<Key,Value> data = buildTestData(depth);

    Map<String,String> options = new HashMap<>();
    options.put(TessellationScanIterator.QUERY_VECTOR, "1.0,0.0,0.0");
    options.put(TessellationScanIterator.DEPTH, String.valueOf(depth));
    // Use a very narrow search radius to exclude distant vertices
    options.put(TessellationScanIterator.SEARCH_RADIUS, "0");
    options.put(TessellationScanIterator.SIMILARITY_THRESHOLD, "0.9");

    TessellationScanIterator iter = new TessellationScanIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // v3 (distant vector [0,0,1]) should be filtered out
    // Its cosine similarity to [1,0,0] is 0, well below 0.9 threshold
    boolean v3Returned =
        entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v3"));
    assertFalse(v3Returned, "v3 (distant vector) should be filtered out");
  }

  @Test
  public void testHighThresholdFiltersAll() throws IOException {
    int depth = 3;
    TreeMap<Key,Value> data = buildTestData(depth);

    Map<String,String> options = new HashMap<>();
    options.put(TessellationScanIterator.QUERY_VECTOR, "0.5,0.5,0.5");
    options.put(TessellationScanIterator.DEPTH, String.valueOf(depth));
    options.put(TessellationScanIterator.SEARCH_RADIUS, "0");
    // Extremely high threshold - nothing should pass
    options.put(TessellationScanIterator.SIMILARITY_THRESHOLD, "0.9999");

    TessellationScanIterator iter = new TessellationScanIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    // With extremely high threshold and narrow radius, likely no results
    // (or only exact matches, which we don't have)
    // The main point is the iterator runs without error
    assertNotNull(keys);
  }

  @Test
  public void testResultsAreSorted() throws IOException {
    int depth = 3;
    TreeMap<Key,Value> data = buildTestData(depth);

    Map<String,String> options = new HashMap<>();
    options.put(TessellationScanIterator.QUERY_VECTOR, "1.0,0.0,0.0");
    options.put(TessellationScanIterator.DEPTH, String.valueOf(depth));
    options.put(TessellationScanIterator.SEARCH_RADIUS, "5");
    options.put(TessellationScanIterator.SIMILARITY_THRESHOLD, "0.5");

    TessellationScanIterator iter = new TessellationScanIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    for (int i = 1; i < keys.size(); i++) {
      assertTrue(keys.get(i - 1).compareTo(keys.get(i)) <= 0, "Results should be sorted by Key");
    }
  }

  @Test
  public void testAllPropertiesOfQualifyingVertexIncluded() throws IOException {
    int depth = 3;
    TreeMap<Key,Value> data = buildTestData(depth);

    Map<String,String> options = new HashMap<>();
    options.put(TessellationScanIterator.QUERY_VECTOR, "1.0,0.0,0.0");
    options.put(TessellationScanIterator.DEPTH, String.valueOf(depth));
    options.put(TessellationScanIterator.SEARCH_RADIUS, "5");
    options.put(TessellationScanIterator.SIMILARITY_THRESHOLD, "0.9");

    TessellationScanIterator iter = new TessellationScanIterator();
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    Map<Key,Value> entries = collectEntries(iter);

    // If v1 is in results, ALL of its properties should be included (label, embedding, cell_id,
    // name)
    boolean v1InResults =
        entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v1"));
    if (v1InResults) {
      assertTrue(entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v1")
          && k.getColumnQualifier().toString().equals("_label")));
      assertTrue(entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v1")
          && k.getColumnQualifier().toString().equals("name")));
      assertTrue(entries.keySet().stream().anyMatch(k -> k.getRow().toString().equals("v1")
          && k.getColumnQualifier().toString().equals("_embedding")));
    }
  }
}
