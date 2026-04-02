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
 * Major-compaction-scope iterator that builds an in-memory adjacency list and runs iterative
 * PageRank, emitting {@code _rank} property entries for each vertex. Implements
 * {@link SortedKeyValueIterator SortedKeyValueIterator&lt;Key,Value&gt;} directly (same pattern as
 * GraphTraversalIterator).
 *
 * <p>
 * During {@link #seek}, all source entries are scanned and passed through. An adjacency list is
 * built from edge column families, and PageRank is computed iteratively using the standard formula:
 * {@code rank(v) = (1-d)/N + d * sum(rank(u)/outDegree(u))} for each incoming neighbor {@code u}.
 *
 * <p>
 * After convergence or reaching the maximum number of iterations, a {@code _rank} property is
 * emitted for each vertex. This is pure computation with no external API calls.
 *
 * <p>
 * Safety: if more than {@code maxVertices} vertices are found, rank computation is skipped
 * entirely.
 */
public class GraphRankIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String DAMPING_FACTOR = "dampingFactor";
  public static final String MAX_ITERATIONS = "maxIterations";
  public static final String EDGE_TYPE = "edgeType";
  public static final String MAX_VERTICES = "maxVertices";
  public static final String CONVERGENCE_THRESHOLD = "convergenceThreshold";

  private SortedKeyValueIterator<Key,Value> source;

  private double dampingFactor = 0.85;
  private int maxIterations = 20;
  private String edgeType;
  private int maxVertices = 100000;
  private double convergenceThreshold = 0.0001;

  private List<Map.Entry<Key,Value>> results;
  private int currentResultIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(DAMPING_FACTOR)) {
      dampingFactor = Double.parseDouble(options.get(DAMPING_FACTOR));
    }
    if (options.containsKey(MAX_ITERATIONS)) {
      maxIterations = Integer.parseInt(options.get(MAX_ITERATIONS));
    }
    if (options.containsKey(EDGE_TYPE)) {
      edgeType = options.get(EDGE_TYPE);
    }
    if (options.containsKey(MAX_VERTICES)) {
      maxVertices = Integer.parseInt(options.get(MAX_VERTICES));
    }
    if (options.containsKey(CONVERGENCE_THRESHOLD)) {
      convergenceThreshold = Double.parseDouble(options.get(CONVERGENCE_THRESHOLD));
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

    // Track vertices (rows with at least one V: entry) and their label visibility
    Map<String,VertexData> vertices = new HashMap<>();
    // Adjacency list: source -> list of targets (outgoing edges)
    Map<String,List<String>> outgoing = new HashMap<>();
    // Incoming adjacency: target -> set of sources
    Map<String,Set<String>> incoming = new HashMap<>();

    // Pass 1: scan all source entries, collect graph structure
    source.seek(range, columnFamilies, inclusive);
    while (source.hasTop()) {
      Key key = new Key(source.getTopKey());
      Value value = new Value(source.getTopValue());
      originalEntries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));

      String row = key.getRow().toString();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      // Track vertices: rows with V column family entries
      if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
        if (!vertices.containsKey(row)) {
          vertices.put(row, new VertexData(row, key.getColumnVisibilityParsed()));
        }
        // Prefer the _label entry's visibility for the rank output
        if (colQual.equals(GraphSchema.LABEL_COLQUAL)) {
          vertices.get(row).visibility = key.getColumnVisibilityParsed();
        }
      }

      // Build adjacency list from edge column families
      if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
        if (edgeType == null || colFam.equals(GraphSchema.edgeColumnFamily(edgeType))) {
          outgoing.computeIfAbsent(row, k -> new ArrayList<>()).add(colQual);
          incoming.computeIfAbsent(colQual, k -> new HashSet<>()).add(row);
        }
      }

      source.next();
    }

    // Start with all original entries
    results.addAll(originalEntries);

    // Safety check: skip if too many vertices
    if (vertices.size() > maxVertices || vertices.isEmpty()) {
      results.sort(Comparator.comparing(Map.Entry::getKey));
      return;
    }

    // Initialize ranks to 1/N
    int n = vertices.size();
    double initialRank = 1.0 / n;
    Map<String,Double> ranks = new HashMap<>();
    for (String vertexId : vertices.keySet()) {
      ranks.put(vertexId, initialRank);
    }

    // Run iterative PageRank
    for (int iter = 0; iter < maxIterations; iter++) {
      if (isRunningLowOnMemory()) {
        break;
      }

      Map<String,Double> newRanks = new HashMap<>();
      double maxDelta = 0.0;

      for (String vertexId : vertices.keySet()) {
        double rankSum = 0.0;

        // Sum rank contributions from incoming neighbors
        Set<String> incomingNeighbors = incoming.get(vertexId);
        if (incomingNeighbors != null) {
          for (String neighbor : incomingNeighbors) {
            Double neighborRank = ranks.get(neighbor);
            if (neighborRank != null) {
              List<String> neighborOutgoing = outgoing.get(neighbor);
              int outDegree = neighborOutgoing != null ? neighborOutgoing.size() : 1;
              rankSum += neighborRank / outDegree;
            }
          }
        }

        double newRank = (1.0 - dampingFactor) / n + dampingFactor * rankSum;
        newRanks.put(vertexId, newRank);

        double delta = Math.abs(newRank - ranks.getOrDefault(vertexId, 0.0));
        if (delta > maxDelta) {
          maxDelta = delta;
        }
      }

      ranks = newRanks;

      // Check convergence
      if (maxDelta < convergenceThreshold) {
        break;
      }
    }

    // Emit _rank property for each vertex
    for (Map.Entry<String,Double> entry : ranks.entrySet()) {
      String vertexId = entry.getKey();
      double rank = entry.getValue();
      VertexData vertexData = vertices.get(vertexId);

      ColumnVisibility vis = vertexData.visibility;
      String visString = new String(vis.getExpression(), java.nio.charset.StandardCharsets.UTF_8);
      Key rankKey =
          new Key(vertexId, GraphSchema.VERTEX_COLFAM, GraphSchema.RANK_COLQUAL, visString);
      Value rankValue = new Value(String.valueOf(rank));
      results.add(new AbstractMap.SimpleImmutableEntry<>(rankKey, rankValue));
    }

    // Sort all results by Key for correct iterator ordering
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    GraphRankIterator copy = new GraphRankIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy GraphRankIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    options.put(DAMPING_FACTOR, String.valueOf(dampingFactor));
    options.put(MAX_ITERATIONS, String.valueOf(maxIterations));
    if (edgeType != null) {
      options.put(EDGE_TYPE, edgeType);
    }
    options.put(MAX_VERTICES, String.valueOf(maxVertices));
    options.put(CONVERGENCE_THRESHOLD, String.valueOf(convergenceThreshold));
    return options;
  }

  /** Tracks vertex identity and visibility during adjacency list construction. */
  private static class VertexData {
    final String vertexId;
    ColumnVisibility visibility;

    VertexData(String vertexId, ColumnVisibility visibility) {
      this.vertexId = vertexId;
      this.visibility = visibility;
    }
  }
}
