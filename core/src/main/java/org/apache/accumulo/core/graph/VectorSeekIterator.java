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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.ValueType;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Scan-scope iterator for the {@code graph_vidx} table. Unlike
 * {@link org.apache.accumulo.core.file.rfile.VectorIterator VectorIterator} which full-scans the
 * graph table, this iterator operates on a pre-scoped cell ID range and only scores the candidates
 * within that range.
 *
 * <p>
 * Options:
 * <ul>
 * <li>{@value #QUERY_VECTOR_OPTION} — comma-separated float values of the query vector</li>
 * <li>{@value #TOP_K_OPTION} — maximum results to return (default 10)</li>
 * <li>{@value #THRESHOLD_OPTION} — minimum cosine similarity (default 0.0)</li>
 * <li>{@value #SIMILARITY_TYPE_OPTION} — "COSINE" (default) or "DOT_PRODUCT"</li>
 * </ul>
 *
 * <p>
 * Results are buffered in {@link #seek} and served sorted by key via
 * {@link #hasTop()}/{@link #next()}, satisfying the {@link SortedKeyValueIterator} contract.
 */
public class VectorSeekIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String QUERY_VECTOR_OPTION = "queryVector";
  public static final String TOP_K_OPTION = "topK";
  public static final String THRESHOLD_OPTION = "threshold";
  public static final String SIMILARITY_TYPE_OPTION = "similarityType";

  private SortedKeyValueIterator<Key,Value> source;

  private float[] queryVector;
  private int topK = 10;
  private float threshold = 0.0f;
  private String similarityType = "COSINE";

  private List<Map.Entry<Key,Value>> results;
  private int currentIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(QUERY_VECTOR_OPTION)) {
      queryVector = parseVector(options.get(QUERY_VECTOR_OPTION));
    }
    if (options.containsKey(TOP_K_OPTION)) {
      topK = Integer.parseInt(options.get(TOP_K_OPTION));
    }
    if (options.containsKey(THRESHOLD_OPTION)) {
      threshold = Float.parseFloat(options.get(THRESHOLD_OPTION));
    }
    if (options.containsKey(SIMILARITY_TYPE_OPTION)) {
      similarityType = options.get(SIMILARITY_TYPE_OPTION).toUpperCase();
    }

    results = new ArrayList<>();
    currentIndex = 0;
  }

  @Override
  public boolean hasTop() {
    return currentIndex < results.size();
  }

  @Override
  public void next() throws IOException {
    currentIndex++;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    if (queryVector == null) {
      throw new IllegalStateException("Query vector not set");
    }

    results.clear();
    currentIndex = 0;

    source.seek(range, columnFamilies, inclusive);

    // Score all candidates within the pre-scoped range
    List<ScoredEntry> scored = new ArrayList<>();
    while (source.hasTop()) {
      Key key = source.getTopKey();
      Value value = source.getTopValue();

      if (value.getValueType() == ValueType.VECTOR_FLOAT32) {
        try {
          float[] vector = value.asVector();
          if (vector.length == queryVector.length) {
            float similarity = computeSimilarity(queryVector, vector);
            if (similarity >= threshold) {
              scored.add(new ScoredEntry(new Key(key), new Value(value), similarity));
            }
          }
        } catch (Exception e) {
          // Skip malformed vectors
        }
      }

      source.next();
    }

    // Sort by similarity descending, keep top-K
    scored.sort(Comparator.<ScoredEntry>comparingDouble(e -> e.similarity).reversed());
    if (scored.size() > topK) {
      scored = new ArrayList<>(scored.subList(0, topK));
    }

    // Re-sort by key for SortedKeyValueIterator contract
    scored.sort(Comparator.comparing(e -> e.key));

    for (ScoredEntry entry : scored) {
      results.add(Map.entry(entry.key, entry.value));
    }
  }

  @Override
  public Key getTopKey() {
    if (!hasTop()) {
      return null;
    }
    return results.get(currentIndex).getKey();
  }

  @Override
  public Value getTopValue() {
    if (!hasTop()) {
      return null;
    }
    return results.get(currentIndex).getValue();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    VectorSeekIterator copy = new VectorSeekIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy VectorSeekIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    if (queryVector != null) {
      options.put(QUERY_VECTOR_OPTION, vectorToString(queryVector));
    }
    options.put(TOP_K_OPTION, String.valueOf(topK));
    options.put(THRESHOLD_OPTION, String.valueOf(threshold));
    options.put(SIMILARITY_TYPE_OPTION, similarityType);
    return options;
  }

  private float computeSimilarity(float[] v1, float[] v2) {
    if ("DOT_PRODUCT".equals(similarityType)) {
      return dotProduct(v1, v2);
    }
    return cosineSimilarity(v1, v2);
  }

  private static float cosineSimilarity(float[] v1, float[] v2) {
    float dot = 0f, norm1 = 0f, norm2 = 0f;
    for (int i = 0; i < v1.length; i++) {
      dot += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }
    if (norm1 == 0f || norm2 == 0f) {
      return 0f;
    }
    return dot / (float) (Math.sqrt(norm1) * Math.sqrt(norm2));
  }

  private static float dotProduct(float[] v1, float[] v2) {
    float dot = 0f;
    for (int i = 0; i < v1.length; i++) {
      dot += v1[i] * v2[i];
    }
    return dot;
  }

  private static float[] parseVector(String str) {
    String[] parts = str.split(",");
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vector[i] = Float.parseFloat(parts[i].trim());
    }
    return vector;
  }

  private static String vectorToString(float[] vector) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(vector[i]);
    }
    return sb.toString();
  }

  private static class ScoredEntry {
    final Key key;
    final Value value;
    final float similarity;

    ScoredEntry(Key key, Value value, float similarity) {
      this.key = key;
      this.value = value;
      this.similarity = similarity;
    }
  }
}
