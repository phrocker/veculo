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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side graph pattern matching engine. Orchestrates multiple scans to find paths,
 * intersections, and triangles in the graph.
 */
public final class GraphPatternEngine {

  private static final Logger log = LoggerFactory.getLogger(GraphPatternEngine.class);

  private GraphPatternEngine() {}

  /**
   * Finds the shortest path between two vertices using BFS.
   *
   * @param client AccumuloClient for creating scanners
   * @param table the graph table name
   * @param auths authorizations for visibility filtering
   * @param source source vertex ID
   * @param target target vertex ID
   * @param edgeType edge type to follow (null for all types)
   * @param maxDepth maximum traversal depth
   * @return ordered list of vertex IDs from source to target, or empty list if no path found
   */
  public static List<String> findShortestPath(AccumuloClient client, String table,
      Authorizations auths, String source, String target, String edgeType, int maxDepth)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    if (source.equals(target)) {
      return List.of(source);
    }

    Map<String,String> parentMap = new HashMap<>();
    parentMap.put(source, null);

    Queue<String> frontier = new LinkedList<>();
    frontier.add(source);

    int depth = 0;

    while (!frontier.isEmpty() && depth < maxDepth) {
      int levelSize = frontier.size();
      for (int i = 0; i < levelSize; i++) {
        String current = frontier.poll();

        List<String> neighbors = getNeighbors(client, table, auths, current, edgeType, "outgoing");

        for (String neighbor : neighbors) {
          if (parentMap.containsKey(neighbor)) {
            continue;
          }
          parentMap.put(neighbor, current);

          if (neighbor.equals(target)) {
            // Reconstruct path
            List<String> path = new ArrayList<>();
            String node = target;
            while (node != null) {
              path.add(0, node);
              node = parentMap.get(node);
            }
            return path;
          }

          frontier.add(neighbor);
        }
      }
      depth++;
    }

    return List.of();
  }

  /**
   * Finds all paths between two vertices using DFS with backtracking.
   *
   * @param client AccumuloClient for creating scanners
   * @param table the graph table name
   * @param auths authorizations for visibility filtering
   * @param source source vertex ID
   * @param target target vertex ID
   * @param edgeType edge type to follow (null for all types)
   * @param maxDepth maximum path length
   * @param maxPaths maximum number of paths to return
   * @return list of paths, where each path is a list of vertex IDs
   */
  public static List<List<String>> findAllPaths(AccumuloClient client, String table,
      Authorizations auths, String source, String target, String edgeType, int maxDepth,
      int maxPaths) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    List<List<String>> results = new ArrayList<>();
    List<String> currentPath = new ArrayList<>();
    Set<String> visited = new HashSet<>();

    currentPath.add(source);
    visited.add(source);

    findAllPathsDfs(client, table, auths, source, target, edgeType, maxDepth, maxPaths, currentPath,
        visited, results);

    return results;
  }

  private static void findAllPathsDfs(AccumuloClient client, String table, Authorizations auths,
      String current, String target, String edgeType, int maxDepth, int maxPaths,
      List<String> currentPath, Set<String> visited, List<List<String>> results)
      throws TableNotFoundException {

    if (results.size() >= maxPaths) {
      return;
    }

    if (current.equals(target)) {
      results.add(new ArrayList<>(currentPath));
      return;
    }

    if (currentPath.size() > maxDepth) {
      return;
    }

    List<String> neighbors = getNeighbors(client, table, auths, current, edgeType, "outgoing");

    for (String neighbor : neighbors) {
      if (results.size() >= maxPaths) {
        return;
      }
      if (visited.contains(neighbor)) {
        continue;
      }

      visited.add(neighbor);
      currentPath.add(neighbor);

      findAllPathsDfs(client, table, auths, neighbor, target, edgeType, maxDepth, maxPaths,
          currentPath, visited, results);

      currentPath.remove(currentPath.size() - 1);
      visited.remove(neighbor);
    }
  }

  /**
   * Finds vertices connected to all anchor vertices (set intersection of neighborhoods).
   *
   * @param client AccumuloClient for creating scanners
   * @param table the graph table name
   * @param auths authorizations for visibility filtering
   * @param anchorVertices list of vertex IDs that results must connect to
   * @param anchorEdgeTypes edge types to follow; if one entry, use for all anchors; if matching
   *        length, use corresponding types; if empty, follow all types
   * @param direction "outgoing", "incoming", or "both"
   * @return set of vertex IDs connected to all anchor vertices
   */
  public static Set<String> findIntersection(AccumuloClient client, String table,
      Authorizations auths, List<String> anchorVertices, List<String> anchorEdgeTypes,
      String direction)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    if (anchorVertices == null || anchorVertices.isEmpty()) {
      return Set.of();
    }

    Set<String> candidates = null;

    for (int i = 0; i < anchorVertices.size(); i++) {
      String anchor = anchorVertices.get(i);

      // Determine edge type for this anchor
      String edgeType = null;
      if (anchorEdgeTypes != null && !anchorEdgeTypes.isEmpty()) {
        if (anchorEdgeTypes.size() == 1) {
          edgeType = anchorEdgeTypes.get(0);
        } else if (i < anchorEdgeTypes.size()) {
          edgeType = anchorEdgeTypes.get(i);
        }
      }

      List<String> neighbors = getNeighbors(client, table, auths, anchor, edgeType, direction);
      Set<String> neighborSet = new HashSet<>(neighbors);

      if (candidates == null) {
        candidates = neighborSet;
      } else {
        candidates.retainAll(neighborSet);
        if (candidates.isEmpty()) {
          return candidates;
        }
      }
    }

    return candidates != null ? candidates : Set.of();
  }

  /**
   * Finds triangles involving a vertex. A triangle is a set of three vertices (A, B, C) where A is
   * connected to B, B is connected to C, and A is connected to C.
   *
   * @param client AccumuloClient for creating scanners
   * @param table the graph table name
   * @param auths authorizations for visibility filtering
   * @param vertexId center vertex ID
   * @param edgeType edge type filter (null for all types)
   * @param maxTriangles maximum number of triangles to return
   * @return list of triangles, where each triangle is [vertexId, neighborA, neighborB]
   */
  public static List<List<String>> findTriangles(AccumuloClient client, String table,
      Authorizations auths, String vertexId, String edgeType, int maxTriangles)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    List<List<String>> triangles = new ArrayList<>();

    // Step 1: Get all neighbors of vertexId
    List<String> neighbors = getNeighbors(client, table, auths, vertexId, edgeType, "outgoing");

    if (neighbors.size() < 2) {
      return triangles;
    }

    // Step 2: For each pair (i, j) where i < j, check if neighbors[i] → neighbors[j]
    for (int i = 0; i < neighbors.size() && triangles.size() < maxTriangles; i++) {
      for (int j = i + 1; j < neighbors.size() && triangles.size() < maxTriangles; j++) {
        String neighborA = neighbors.get(i);
        String neighborB = neighbors.get(j);

        // Check if neighborA has an edge to neighborB
        if (hasEdge(client, table, auths, neighborA, neighborB, edgeType)) {
          triangles.add(List.of(vertexId, neighborA, neighborB));
        }
      }
    }

    return triangles;
  }

  /**
   * Checks if an edge exists from source to target.
   */
  private static boolean hasEdge(AccumuloClient client, String table, Authorizations auths,
      String source, String target, String edgeType) throws TableNotFoundException {
    try (Scanner scanner = client.createScanner(table, auths)) {
      scanner.setRange(Range.exact(source));

      if (edgeType != null && !edgeType.isEmpty()) {
        scanner.fetchColumnFamily(new Text(GraphSchema.edgeColumnFamily(edgeType)));
      }

      for (Map.Entry<Key,Value> entry : scanner) {
        String cf = entry.getKey().getColumnFamily().toString();
        if (!cf.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
          continue;
        }
        if (entry.getKey().getColumnQualifier().toString().equals(target)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets the neighbor vertex IDs for a vertex, optionally filtered by edge type and direction.
   */
  private static List<String> getNeighbors(AccumuloClient client, String table,
      Authorizations auths, String vertexId, String edgeType, String direction)
      throws TableNotFoundException {
    List<String> neighbors = new ArrayList<>();
    try (Scanner scanner = client.createScanner(table, auths)) {
      scanner.setRange(Range.exact(vertexId));
      for (Map.Entry<Key,Value> entry : scanner) {
        String cf = entry.getKey().getColumnFamily().toString();
        boolean isOutgoing = cf.startsWith(GraphSchema.EDGE_COLFAM_PREFIX);
        boolean isIncoming = cf.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX);

        if ("outgoing".equals(direction) && !isOutgoing) {
          continue;
        }
        if ("incoming".equals(direction) && !isIncoming) {
          continue;
        }
        if (!isOutgoing && !isIncoming) {
          continue;
        }

        if (edgeType != null && !edgeType.isEmpty()) {
          String type = isOutgoing ? cf.substring(GraphSchema.EDGE_COLFAM_PREFIX.length())
              : cf.substring(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX.length());
          if (!edgeType.equals(type)) {
            continue;
          }
        }

        neighbors.add(entry.getKey().getColumnQualifier().toString());
      }
    }
    return neighbors;
  }
}
