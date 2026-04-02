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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Server-side BFS/DFS graph traversal iterator. Implements {@link SortedKeyValueIterator
 * SortedKeyValueIterator&lt;Key,Value&gt;} directly (same pattern as VectorIterator) because it
 * must re-seek the source to different rows during traversal.
 *
 * <p>
 * On {@link #seek}, performs a full traversal from the start vertex, materializes results in a
 * sorted list, and serves them through the standard iterator interface. Results are sorted by Key
 * for correctness.
 *
 * <p>
 * Frontier vertices (those discovered outside the current tablet range) are reported as results
 * with a special "_frontier" column qualifier so the client can continue traversal on other
 * tablets.
 */
public class GraphTraversalIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String START_VERTEX = "startVertex";
  public static final String EDGE_TYPE = "edgeType";
  public static final String MAX_DEPTH = "maxDepth";
  public static final String MAX_RESULTS = "maxResults";
  public static final String TRAVERSAL_MODE = "traversalMode";

  public static final String FRONTIER_COLQUAL = "_frontier";

  public enum TraversalMode {
    BFS, DFS
  }

  private SortedKeyValueIterator<Key,Value> source;

  private String startVertex;
  private String edgeType;
  private int maxDepth = 2;
  private int maxResults = 1000;
  private TraversalMode traversalMode = TraversalMode.BFS;

  private List<Map.Entry<Key,Value>> results;
  private int currentResultIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(START_VERTEX)) {
      startVertex = options.get(START_VERTEX);
    }
    if (options.containsKey(EDGE_TYPE)) {
      edgeType = options.get(EDGE_TYPE);
    }
    if (options.containsKey(MAX_DEPTH)) {
      maxDepth = Integer.parseInt(options.get(MAX_DEPTH));
    }
    if (options.containsKey(MAX_RESULTS)) {
      maxResults = Integer.parseInt(options.get(MAX_RESULTS));
    }
    if (options.containsKey(TRAVERSAL_MODE)) {
      traversalMode = TraversalMode.valueOf(options.get(TRAVERSAL_MODE));
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
    if (startVertex == null) {
      throw new IllegalStateException("Start vertex not set");
    }

    results.clear();
    currentResultIndex = 0;

    // Collect discovered vertex data during traversal
    Map<Key,Value> resultMap = new LinkedHashMap<>();

    Set<String> visited = new HashSet<>();
    String edgeCf = edgeType != null ? GraphSchema.edgeColumnFamily(edgeType) : null;

    if (traversalMode == TraversalMode.BFS) {
      traverseBFS(range, visited, resultMap, edgeCf);
    } else {
      traverseDFS(range, visited, resultMap, edgeCf);
    }

    // Sort results by Key for correct iterator ordering
    results = new ArrayList<>(resultMap.entrySet());
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  private void traverseBFS(Range tabletRange, Set<String> visited, Map<Key,Value> resultMap,
      String edgeCf) throws IOException {
    Deque<VertexAtDepth> queue = new ArrayDeque<>();
    queue.add(new VertexAtDepth(startVertex, 0));
    visited.add(startVertex);

    while (!queue.isEmpty() && resultMap.size() < maxResults) {
      VertexAtDepth current = queue.poll();

      if (isRunningLowOnMemory()) {
        break;
      }

      // Collect vertex properties and edges by re-seeking the source
      List<String> neighbors = collectVertexData(current.vertexId, tabletRange, resultMap, edgeCf);

      // Enqueue neighbors if within depth limit
      if (current.depth < maxDepth) {
        for (String neighbor : neighbors) {
          if (!visited.contains(neighbor) && resultMap.size() < maxResults) {
            visited.add(neighbor);
            queue.add(new VertexAtDepth(neighbor, current.depth + 1));
          }
        }
      }
    }
  }

  private void traverseDFS(Range tabletRange, Set<String> visited, Map<Key,Value> resultMap,
      String edgeCf) throws IOException {
    Deque<VertexAtDepth> stack = new ArrayDeque<>();
    stack.push(new VertexAtDepth(startVertex, 0));
    visited.add(startVertex);

    while (!stack.isEmpty() && resultMap.size() < maxResults) {
      VertexAtDepth current = stack.pop();

      if (isRunningLowOnMemory()) {
        break;
      }

      List<String> neighbors = collectVertexData(current.vertexId, tabletRange, resultMap, edgeCf);

      if (current.depth < maxDepth) {
        for (String neighbor : neighbors) {
          if (!visited.contains(neighbor) && resultMap.size() < maxResults) {
            visited.add(neighbor);
            stack.push(new VertexAtDepth(neighbor, current.depth + 1));
          }
        }
      }
    }
  }

  /**
   * Re-seeks the source iterator to the given vertex's row, collects all data, and returns the list
   * of neighbor vertex IDs discovered from outgoing edges.
   */
  private List<String> collectVertexData(String vertexId, Range tabletRange,
      Map<Key,Value> resultMap, String edgeCf) throws IOException {

    Range vertexRange = GraphSchema.vertexRange(vertexId);

    // Check if vertex is within the tablet's range
    if (!rangesOverlap(tabletRange, vertexRange)) {
      // Report as frontier vertex
      Key frontierKey = new Key(vertexId, GraphSchema.VERTEX_COLFAM, FRONTIER_COLQUAL);
      resultMap.put(frontierKey, new Value("frontier"));
      return Collections.emptyList();
    }

    // Re-seek source to this vertex's row
    source.seek(vertexRange, Collections.emptyList(), false);

    List<String> neighbors = new ArrayList<>();
    while (source.hasTop()) {
      Key key = source.getTopKey();
      Value value = source.getTopValue();

      // Store a copy of the key-value pair
      resultMap.put(new Key(key), new Value(value));

      // Collect neighbor IDs from outgoing edges
      String colFam = key.getColumnFamily().toString();
      if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
        if (edgeCf == null || colFam.equals(edgeCf)) {
          neighbors.add(key.getColumnQualifier().toString());
        }
      }

      if (resultMap.size() >= maxResults) {
        break;
      }

      source.next();
    }

    return neighbors;
  }

  private static boolean rangesOverlap(Range tabletRange, Range vertexRange) {
    // If no tablet range constraint, everything is local
    if (tabletRange == null
        || tabletRange.isInfiniteStartKey() && tabletRange.isInfiniteStopKey()) {
      return true;
    }
    // Check if the vertex range overlaps with the tablet range
    Key vertexStart = vertexRange.getStartKey();
    if (vertexStart == null) {
      return true;
    }
    if (!tabletRange.isInfiniteStopKey() && tabletRange.getEndKey() != null
        && vertexStart.compareTo(tabletRange.getEndKey()) > 0) {
      return false;
    }
    if (!tabletRange.isInfiniteStartKey() && tabletRange.getStartKey() != null) {
      Key tabletEnd = tabletRange.getEndKey();
      if (tabletEnd != null && vertexStart.compareTo(tabletRange.getStartKey()) < 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    GraphTraversalIterator copy = new GraphTraversalIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy GraphTraversalIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new java.util.HashMap<>();
    if (startVertex != null) {
      options.put(START_VERTEX, startVertex);
    }
    if (edgeType != null) {
      options.put(EDGE_TYPE, edgeType);
    }
    options.put(MAX_DEPTH, String.valueOf(maxDepth));
    options.put(MAX_RESULTS, String.valueOf(maxResults));
    options.put(TRAVERSAL_MODE, traversalMode.name());
    return options;
  }

  /** Helper class to track vertex ID with its current traversal depth. */
  private static class VertexAtDepth {
    final String vertexId;
    final int depth;

    VertexAtDepth(String vertexId, int depth) {
      this.vertexId = vertexId;
      this.depth = depth;
    }
  }
}
