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
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Major-compaction-scope iterator that computes pairwise cosine similarity between vertex
 * embeddings and emits {@code SIMILAR_TO} edges for pairs exceeding a configurable threshold.
 * Implements {@link SortedKeyValueIterator SortedKeyValueIterator&lt;Key,Value&gt;} directly (same
 * pattern as GraphTraversalIterator).
 *
 * <p>
 * This iterator performs pure computation with no external API calls. During {@link #seek}, it
 * scans all entries from the source, collects vertex embeddings, passes through all original
 * entries, and computes pairwise cosine similarity. For each pair above the similarity threshold,
 * both forward ({@code E_SIMILAR_TO}) and inverse ({@code EI_SIMILAR_TO}) edge entries are emitted.
 *
 * <p>
 * Safety: if more than {@code maxVectors} embeddings are found, similarity computation is skipped
 * entirely. The iterator also checks {@link #isRunningLowOnMemory()} during computation to bail out
 * early if needed.
 */
public class SemanticEdgeIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String SIMILARITY_THRESHOLD = "similarityThreshold";
  public static final String MAX_EDGES_PER_VERTEX = "maxEdgesPerVertex";
  public static final String MAX_VECTORS = "maxVectors";

  private SortedKeyValueIterator<Key,Value> source;

  private double similarityThreshold = 0.85;
  private int maxEdgesPerVertex = 10;
  private int maxVectors = 10000;

  private List<Map.Entry<Key,Value>> results;
  private int currentResultIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(SIMILARITY_THRESHOLD)) {
      similarityThreshold = Double.parseDouble(options.get(SIMILARITY_THRESHOLD));
    }
    if (options.containsKey(MAX_EDGES_PER_VERTEX)) {
      maxEdgesPerVertex = Integer.parseInt(options.get(MAX_EDGES_PER_VERTEX));
    }
    if (options.containsKey(MAX_VECTORS)) {
      maxVectors = Integer.parseInt(options.get(MAX_VECTORS));
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
    Map<String,EmbeddingInfo> embeddings = new HashMap<>();

    // Pass 1: scan all source entries, collect originals and embeddings
    source.seek(range, columnFamilies, inclusive);
    while (source.hasTop()) {
      Key key = new Key(source.getTopKey());
      Value sourceValue = source.getTopValue();
      originalEntries.add(new AbstractMap.SimpleImmutableEntry<>(key, new Value(sourceValue)));

      // Collect vertex embeddings — check type on source value before copy loses it
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();
      if (colFam.equals(GraphSchema.VERTEX_COLFAM)
          && colQual.equals(GraphSchema.EMBEDDING_COLQUAL)) {
        try {
          float[] vector = sourceValue.asVector();
          String vertexId = key.getRow().toString();
          ColumnVisibility vis = key.getColumnVisibilityParsed();
          embeddings.put(vertexId, new EmbeddingInfo(vertexId, vector, vis));
        } catch (IllegalStateException | IllegalArgumentException e) {
          // Not a valid vector — skip
        }
      }

      source.next();
    }

    // Start with all original entries
    results.addAll(originalEntries);

    // Safety check: skip similarity computation if too many vectors
    if (embeddings.size() > maxVectors) {
      results.sort(Comparator.comparing(Map.Entry::getKey));
      return;
    }

    // Pass 2: compute pairwise cosine similarity and emit SIMILAR_TO edges
    List<EmbeddingInfo> embeddingList = new ArrayList<>(embeddings.values());
    // Track candidate edges per vertex: vertexId -> list of (neighborId, similarity)
    Map<String,List<SimilarityCandidate>> candidatesPerVertex = new HashMap<>();

    for (int i = 0; i < embeddingList.size(); i++) {
      if (isRunningLowOnMemory()) {
        break;
      }
      EmbeddingInfo a = embeddingList.get(i);
      for (int j = i + 1; j < embeddingList.size(); j++) {
        if (isRunningLowOnMemory()) {
          break;
        }
        EmbeddingInfo b = embeddingList.get(j);
        double similarity = cosineSimilarity(a.vector, b.vector);
        if (similarity >= similarityThreshold) {
          candidatesPerVertex.computeIfAbsent(a.vertexId, k -> new ArrayList<>())
              .add(new SimilarityCandidate(b.vertexId, similarity));
          candidatesPerVertex.computeIfAbsent(b.vertexId, k -> new ArrayList<>())
              .add(new SimilarityCandidate(a.vertexId, similarity));
        }
      }
    }

    // Limit to maxEdgesPerVertex per vertex (keep highest similarity)
    String edgeCf = GraphSchema.edgeColumnFamily(GraphSchema.SIMILAR_TO_EDGE_TYPE);
    String inverseEdgeCf = GraphSchema.inverseEdgeColumnFamily(GraphSchema.SIMILAR_TO_EDGE_TYPE);

    for (Map.Entry<String,List<SimilarityCandidate>> entry : candidatesPerVertex.entrySet()) {
      String vertexId = entry.getKey();
      List<SimilarityCandidate> candidates = entry.getValue();

      // Sort by similarity descending, keep top maxEdgesPerVertex
      candidates.sort((c1, c2) -> Double.compare(c2.similarity, c1.similarity));
      int limit = Math.min(candidates.size(), maxEdgesPerVertex);

      EmbeddingInfo sourceInfo = embeddings.get(vertexId);

      for (int i = 0; i < limit; i++) {
        SimilarityCandidate candidate = candidates.get(i);
        EmbeddingInfo targetInfo = embeddings.get(candidate.neighborId);

        ColumnVisibility compoundVis =
            GraphSchema.compoundVisibility(sourceInfo.visibility, targetInfo.visibility);
        String visString =
            new String(compoundVis.getExpression(), java.nio.charset.StandardCharsets.UTF_8);
        String edgeValue = "{\"weight\":\"" + candidate.similarity + "\"}";

        // Forward edge: source -> target
        Key forwardKey = new Key(vertexId, edgeCf, candidate.neighborId, visString);
        results.add(new AbstractMap.SimpleImmutableEntry<>(forwardKey, new Value(edgeValue)));

        // Inverse edge: target -> source
        Key inverseKey = new Key(candidate.neighborId, inverseEdgeCf, vertexId, visString);
        results.add(new AbstractMap.SimpleImmutableEntry<>(inverseKey, new Value(edgeValue)));
      }
    }

    // Sort all results by Key for correct iterator ordering
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  /**
   * Computes the cosine similarity between two vectors. Returns a value between -1 and 1, where 1
   * indicates identical direction, 0 indicates orthogonality, and -1 indicates opposite direction.
   *
   * @param a first vector
   * @param b second vector
   * @return cosine similarity between the two vectors, or 0 if either vector has zero magnitude
   */
  public static double cosineSimilarity(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimensions must match: " + a.length + " != " + b.length);
    }

    double dotProduct = 0.0;
    double magnitudeA = 0.0;
    double magnitudeB = 0.0;

    for (int i = 0; i < a.length; i++) {
      dotProduct += (double) a[i] * (double) b[i];
      magnitudeA += (double) a[i] * (double) a[i];
      magnitudeB += (double) b[i] * (double) b[i];
    }

    magnitudeA = Math.sqrt(magnitudeA);
    magnitudeB = Math.sqrt(magnitudeB);

    if (magnitudeA == 0.0 || magnitudeB == 0.0) {
      return 0.0;
    }

    return dotProduct / (magnitudeA * magnitudeB);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    SemanticEdgeIterator copy = new SemanticEdgeIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy SemanticEdgeIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    options.put(SIMILARITY_THRESHOLD, String.valueOf(similarityThreshold));
    options.put(MAX_EDGES_PER_VERTEX, String.valueOf(maxEdgesPerVertex));
    options.put(MAX_VECTORS, String.valueOf(maxVectors));
    return options;
  }

  /** Holds embedding information for a single vertex. */
  private static class EmbeddingInfo {
    final String vertexId;
    final float[] vector;
    final ColumnVisibility visibility;

    EmbeddingInfo(String vertexId, float[] vector, ColumnVisibility visibility) {
      this.vertexId = vertexId;
      this.vector = vector;
      this.visibility = visibility;
    }
  }

  /** Candidate edge with neighbor ID and similarity score for ranking. */
  private static class SimilarityCandidate {
    final String neighborId;
    final double similarity;

    SimilarityCandidate(String neighborId, double similarity) {
      this.neighborId = neighborId;
      this.similarity = similarity;
    }
  }
}
