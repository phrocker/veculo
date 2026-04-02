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
 * Scan-scope iterator that detects anomalous vertices by computing embedding z-scores relative to
 * their graph neighborhood. Implements {@link SortedKeyValueIterator
 * SortedKeyValueIterator&lt;Key,Value&gt;} directly (same pattern as GraphTraversalIterator).
 *
 * <p>
 * For each vertex that has an embedding, this iterator finds its neighbors via edge column
 * families, collects neighbor embeddings, computes the mean cosine similarity to neighbors, then
 * derives a z-score. If the z-score exceeds the configured threshold, an {@code _anomaly_score}
 * property entry is added to the results.
 *
 * <p>
 * All original entries are passed through unchanged. Anomaly score entries are added for vertices
 * whose z-score exceeds the threshold. This is pure computation with no external API calls.
 */
public class AnomalyDetectIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String EDGE_TYPE = "edgeType";
  public static final String MAX_NEIGHBORS = "maxNeighbors";
  public static final String Z_SCORE_THRESHOLD = "zScoreThreshold";

  private SortedKeyValueIterator<Key,Value> source;

  private String edgeType;
  private int maxNeighbors = 50;
  private double zScoreThreshold = 2.0;

  private List<Map.Entry<Key,Value>> results;
  private int currentResultIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(EDGE_TYPE)) {
      edgeType = options.get(EDGE_TYPE);
    }
    if (options.containsKey(MAX_NEIGHBORS)) {
      maxNeighbors = Integer.parseInt(options.get(MAX_NEIGHBORS));
    }
    if (options.containsKey(Z_SCORE_THRESHOLD)) {
      zScoreThreshold = Double.parseDouble(options.get(Z_SCORE_THRESHOLD));
    }

    results = new ArrayList<>();
    currentResultIndex = 0;
  }

  @Override
  public boolean hasTop() {
    return currentResultIndex < results.size();
  }

  @Override
  public void next() throws IOException {
    currentResultIndex++;
  }

  @Override
  public Key getTopKey() {
    if (!hasTop()) {
      return null;
    }
    return results.get(currentResultIndex).getKey();
  }

  @Override
  public Value getTopValue() {
    if (!hasTop()) {
      return null;
    }
    return results.get(currentResultIndex).getValue();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    results.clear();
    currentResultIndex = 0;

    List<Map.Entry<Key,Value>> originalEntries = new ArrayList<>();

    // Per-vertex data: embeddings, neighbors, and visibility
    Map<String,float[]> vertexEmbeddings = new HashMap<>();
    Map<String,ColumnVisibility> vertexVisibilities = new HashMap<>();
    Map<String,Set<String>> vertexNeighbors = new HashMap<>();

    // Pass 1: scan all source entries
    source.seek(range, columnFamilies, inclusive);
    while (source.hasTop()) {
      Key key = new Key(source.getTopKey());
      Value sourceValue = source.getTopValue();
      originalEntries.add(new AbstractMap.SimpleImmutableEntry<>(key, new Value(sourceValue)));

      String row = key.getRow().toString();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      // Collect vertex embeddings — use source value to preserve type
      if (colFam.equals(GraphSchema.VERTEX_COLFAM)
          && colQual.equals(GraphSchema.EMBEDDING_COLQUAL)) {
        try {
          vertexEmbeddings.put(row, sourceValue.asVector());
          vertexVisibilities.put(row, key.getColumnVisibilityParsed());
        } catch (IllegalStateException | IllegalArgumentException e) {
          // Not a valid vector — skip
        }
      }

      // Collect neighbor relationships from edge column families
      if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
        if (edgeType == null || colFam.equals(GraphSchema.edgeColumnFamily(edgeType))) {
          vertexNeighbors.computeIfAbsent(row, k -> new HashSet<>()).add(colQual);
        }
      }

      source.next();
    }

    // Start with all original entries
    results.addAll(originalEntries);

    if (isRunningLowOnMemory()) {
      results.sort(Comparator.comparing(Map.Entry::getKey));
      return;
    }

    // Pass 2: compute anomaly scores for vertices with embeddings
    // First, collect all similarity scores for each vertex to its neighbors
    List<Double> allSimilarities = new ArrayList<>();
    Map<String,Double> vertexMeanSimilarity = new HashMap<>();

    for (Map.Entry<String,float[]> entry : vertexEmbeddings.entrySet()) {
      if (isRunningLowOnMemory()) {
        break;
      }

      String vertexId = entry.getKey();
      float[] vertexVector = entry.getValue();
      Set<String> neighbors = vertexNeighbors.get(vertexId);

      if (neighbors == null || neighbors.isEmpty()) {
        continue;
      }

      // Compute mean similarity to neighbors (up to maxNeighbors)
      double sumSimilarity = 0.0;
      int neighborCount = 0;

      for (String neighborId : neighbors) {
        if (neighborCount >= maxNeighbors) {
          break;
        }
        float[] neighborVector = vertexEmbeddings.get(neighborId);
        if (neighborVector == null) {
          continue;
        }
        double similarity = SemanticEdgeIterator.cosineSimilarity(vertexVector, neighborVector);
        sumSimilarity += similarity;
        neighborCount++;
      }

      if (neighborCount > 0) {
        double meanSimilarity = sumSimilarity / neighborCount;
        vertexMeanSimilarity.put(vertexId, meanSimilarity);
        allSimilarities.add(meanSimilarity);
      }
    }

    // Compute global mean and standard deviation of mean similarities
    if (allSimilarities.size() < 2) {
      results.sort(Comparator.comparing(Map.Entry::getKey));
      return;
    }

    double globalMean = 0.0;
    for (double s : allSimilarities) {
      globalMean += s;
    }
    globalMean /= allSimilarities.size();

    double variance = 0.0;
    for (double s : allSimilarities) {
      double diff = s - globalMean;
      variance += diff * diff;
    }
    variance /= allSimilarities.size();
    double stdDev = Math.sqrt(variance);

    // Avoid division by zero
    if (stdDev == 0.0) {
      results.sort(Comparator.comparing(Map.Entry::getKey));
      return;
    }

    // Emit anomaly score entries for vertices exceeding the z-score threshold
    for (Map.Entry<String,Double> entry : vertexMeanSimilarity.entrySet()) {
      String vertexId = entry.getKey();
      double meanSimilarity = entry.getValue();
      double zScore = Math.abs((meanSimilarity - globalMean) / stdDev);

      if (zScore >= zScoreThreshold) {
        ColumnVisibility vis = vertexVisibilities.get(vertexId);
        String visString = new String(vis.getExpression(), java.nio.charset.StandardCharsets.UTF_8);
        Key anomalyKey = new Key(vertexId, GraphSchema.VERTEX_COLFAM,
            GraphSchema.ANOMALY_SCORE_COLQUAL, visString);
        Value anomalyValue = new Value(String.valueOf(zScore));
        results.add(new AbstractMap.SimpleImmutableEntry<>(anomalyKey, anomalyValue));
      }
    }

    // Sort all results by Key for correct iterator ordering
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    AnomalyDetectIterator copy = new AnomalyDetectIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy AnomalyDetectIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    if (edgeType != null) {
      options.put(EDGE_TYPE, edgeType);
    }
    options.put(MAX_NEIGHBORS, String.valueOf(maxNeighbors));
    options.put(Z_SCORE_THRESHOLD, String.valueOf(zScoreThreshold));
    return options;
  }
}
