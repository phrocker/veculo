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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Scan-time iterator that uses spherical tessellation cell IDs to narrow vector similarity search.
 * Implements {@link SortedKeyValueIterator SortedKeyValueIterator&lt;Key,Value&gt;} directly (same
 * pattern as {@link GraphTraversalIterator}).
 *
 * <p>
 * At query time, given a query vector this iterator:
 * <ol>
 * <li>Scans all entries from the source</li>
 * <li>Collects vertex cell IDs (from the {@code _cell_id} property written by
 * {@link SphericalTessellationIterator}) and embeddings</li>
 * <li>Computes the query cell range via
 * {@link SphericalTessellation#getCellRange(float[], int, int)}</li>
 * <li>Filters to vertices whose cell ID falls within the search range</li>
 * <li>Computes exact cosine similarity for qualifying candidates</li>
 * <li>Includes all entries (all column families) for vertices above the similarity threshold</li>
 * <li>Adds a synthetic {@code _similarity} property with the computed score</li>
 * </ol>
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code queryVector} — comma-separated floats (required)</li>
 * <li>{@code depth} — tessellation depth (default 6)</li>
 * <li>{@code searchRadius} — cell range expansion (default 2)</li>
 * <li>{@code similarityThreshold} — minimum cosine similarity to include (default 0.7)</li>
 * <li>{@code maxResults} — maximum number of qualifying vertices (default 100)</li>
 * </ul>
 */
public class TessellationScanIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String QUERY_VECTOR = "queryVector";
  public static final String DEPTH = "depth";
  public static final String SEARCH_RADIUS = "searchRadius";
  public static final String SIMILARITY_THRESHOLD = "similarityThreshold";
  public static final String MAX_RESULTS = "maxResults";

  private static final String SIMILARITY_COLQUAL = "_similarity";

  private SortedKeyValueIterator<Key,Value> source;
  private IteratorEnvironment env;

  private float[] queryVector;
  private int depth = SphericalTessellation.DEFAULT_DEPTH;
  private int searchRadius = 2;
  private float similarityThreshold = 0.7f;
  private int maxResults = 100;

  private List<Map.Entry<Key,Value>> results;
  private int currentIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.env = env;
    this.results = new ArrayList<>();
    this.currentIndex = 0;

    if (options.containsKey(QUERY_VECTOR)) {
      queryVector = parseVector(options.get(QUERY_VECTOR));
    }
    if (options.containsKey(DEPTH)) {
      depth = Integer.parseInt(options.get(DEPTH));
    }
    if (options.containsKey(SEARCH_RADIUS)) {
      searchRadius = Integer.parseInt(options.get(SEARCH_RADIUS));
    }
    if (options.containsKey(SIMILARITY_THRESHOLD)) {
      similarityThreshold = Float.parseFloat(options.get(SIMILARITY_THRESHOLD));
    }
    if (options.containsKey(MAX_RESULTS)) {
      maxResults = Integer.parseInt(options.get(MAX_RESULTS));
    }
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
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    results.clear();
    currentIndex = 0;

    if (queryVector == null) {
      throw new IllegalStateException("Query vector not set");
    }

    source.seek(range, columnFamilies, inclusive);

    // Compute the cell range for the query vector
    long[] cellRange = SphericalTessellation.getCellRange(queryVector, depth, searchRadius);
    long startCellId = cellRange[0];
    long endCellId = cellRange[1];

    // First pass: buffer all entries grouped by row, collecting cell IDs and embeddings
    Map<String,VertexData> vertexMap = new HashMap<>();
    List<Map.Entry<Key,Value>> allEntries = new ArrayList<>();

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }

      Key key = new Key(source.getTopKey());
      Value sourceValue = source.getTopValue();

      allEntries.add(new AbstractMap.SimpleImmutableEntry<>(key, new Value(sourceValue)));

      String row = key.getRow().toString();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
        VertexData vd = vertexMap.computeIfAbsent(row, k -> new VertexData());

        if (colQual.equals(GraphSchema.CELL_ID_COLQUAL)) {
          try {
            vd.cellId = Long.parseLong(new String(sourceValue.get(), UTF_8));
            vd.hasCellId = true;
          } catch (NumberFormatException e) {
            // Malformed cell ID — skip
          }
        } else if (colQual.equals(GraphSchema.EMBEDDING_COLQUAL)) {
          try {
            vd.embedding = sourceValue.asVector();
            vd.embeddingVisibility = new ColumnVisibility(key.getColumnVisibility());
          } catch (IllegalStateException | IllegalArgumentException e) {
            // Malformed embedding — skip
          }
        }
      }

      source.next();
    }

    // Second pass: determine which vertices qualify
    Set<String> qualifyingVertices = new HashSet<>();
    Map<String,Float> similarityScores = new HashMap<>();
    int qualifiedCount = 0;

    for (Map.Entry<String,VertexData> entry : vertexMap.entrySet()) {
      if (qualifiedCount >= maxResults) {
        break;
      }

      VertexData vd = entry.getValue();

      // Check cell ID range filter
      if (vd.hasCellId) {
        if (vd.cellId < startCellId || vd.cellId > endCellId) {
          continue;
        }
      } else {
        // No cell ID — skip this vertex (it hasn't been tessellated yet)
        continue;
      }

      // Compute exact cosine similarity for candidates that pass the cell filter
      if (vd.embedding != null) {
        float similarity = cosineSimilarity(queryVector, vd.embedding);
        if (similarity >= similarityThreshold) {
          qualifyingVertices.add(entry.getKey());
          similarityScores.put(entry.getKey(), similarity);
          qualifiedCount++;
        }
      }
    }

    // Third pass: emit all entries for qualifying vertices, plus synthetic _similarity
    Set<String> similarityEmitted = new HashSet<>();

    for (Map.Entry<Key,Value> entry : allEntries) {
      String row = entry.getKey().getRow().toString();
      if (qualifyingVertices.contains(row)) {
        results.add(entry);

        // Emit _similarity once per qualifying vertex
        if (!similarityEmitted.contains(row)) {
          similarityEmitted.add(row);
          Float score = similarityScores.get(row);
          if (score != null) {
            VertexData vd = vertexMap.get(row);
            String visStr = "";
            if (vd != null && vd.embeddingVisibility != null) {
              visStr = new String(vd.embeddingVisibility.getExpression(), UTF_8);
            }

            Key simKey = new Key(row, GraphSchema.VERTEX_COLFAM, SIMILARITY_COLQUAL, visStr);
            Value simValue = new Value(Float.toString(score).getBytes(UTF_8));
            results.add(new AbstractMap.SimpleImmutableEntry<>(simKey, simValue));
          }
        }
      }
    }

    // Sort all results by Key for correct iterator ordering
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  /**
   * Computes cosine similarity between two vectors.
   *
   * @param a first vector
   * @param b second vector
   * @return cosine similarity in range [-1, 1], or 0 if either vector has zero magnitude
   */
  static float cosineSimilarity(float[] a, float[] b) {
    float dotProduct = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;

    int len = Math.min(a.length, b.length);
    for (int i = 0; i < len; i++) {
      dotProduct += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }

    if (normA == 0.0f || normB == 0.0f) {
      return 0.0f;
    }

    return dotProduct / (float) (Math.sqrt(normA) * Math.sqrt(normB));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    TessellationScanIterator copy = new TessellationScanIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy TessellationScanIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    if (queryVector != null) {
      options.put(QUERY_VECTOR, vectorToString(queryVector));
    }
    options.put(DEPTH, String.valueOf(depth));
    options.put(SEARCH_RADIUS, String.valueOf(searchRadius));
    options.put(SIMILARITY_THRESHOLD, String.valueOf(similarityThreshold));
    options.put(MAX_RESULTS, String.valueOf(maxResults));
    return options;
  }

  /**
   * Parses a comma-separated string of floats into a float array.
   */
  private static float[] parseVector(String vectorStr) {
    String[] parts = vectorStr.split(",");
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vector[i] = Float.parseFloat(parts[i].trim());
    }
    return vector;
  }

  /**
   * Serializes a float array to a comma-separated string.
   */
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

  /** Per-vertex accumulator used during the scan pass. */
  private static class VertexData {
    long cellId;
    boolean hasCellId;
    float[] embedding;
    ColumnVisibility embeddingVisibility;
  }
}
