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
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.graph.EdgeFilterIterator.ComparisonOperator;
import org.apache.accumulo.core.graph.GraphAggregationIterator.AggregationType;
import org.apache.accumulo.core.graph.GraphTraversalIterator.TraversalMode;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.hadoop.io.Text;

/**
 * Client-side helpers for configuring scanners with graph iterators and reading graph data.
 */
public final class GraphScanner {

  private static final int TEMPORAL_FILTER_ITERATOR_PRIORITY = 10;
  private static final int TRAVERSAL_ITERATOR_PRIORITY = 20;
  private static final int EDGE_FILTER_ITERATOR_PRIORITY = 15;
  private static final int AGGREGATION_ITERATOR_PRIORITY = 22;

  private GraphScanner() {}

  /** Configures the scanner to fetch only vertex properties (ColFam "V"). */
  public static void fetchVertexProperties(Scanner scanner) {
    scanner.fetchColumnFamily(GraphSchema.VERTEX_COLFAM_TEXT);
  }

  /** Configures the scanner to fetch outgoing edges of the specified type. */
  public static void fetchOutgoingEdges(Scanner scanner, String edgeType) {
    scanner.fetchColumnFamily(new Text(GraphSchema.edgeColumnFamily(edgeType)));
  }

  /** Configures the scanner to fetch incoming edges of the specified type. */
  public static void fetchIncomingEdges(Scanner scanner, String edgeType) {
    scanner.fetchColumnFamily(new Text(GraphSchema.inverseEdgeColumnFamily(edgeType)));
  }

  /**
   * Configures a graph traversal iterator on the scanner.
   *
   * @param scanner the scanner to configure
   * @param startVertex starting vertex ID
   * @param edgeType edge type to traverse (null for all types)
   * @param maxDepth maximum traversal depth
   * @param maxResults maximum number of results
   * @param mode BFS or DFS traversal
   */
  public static void configureTraversal(Scanner scanner, String startVertex, String edgeType,
      int maxDepth, int maxResults, TraversalMode mode) {
    IteratorSetting is = new IteratorSetting(TRAVERSAL_ITERATOR_PRIORITY, "graphTraversal",
        GraphTraversalIterator.class);
    is.addOption(GraphTraversalIterator.START_VERTEX, startVertex);
    if (edgeType != null) {
      is.addOption(GraphTraversalIterator.EDGE_TYPE, edgeType);
    }
    is.addOption(GraphTraversalIterator.MAX_DEPTH, String.valueOf(maxDepth));
    is.addOption(GraphTraversalIterator.MAX_RESULTS, String.valueOf(maxResults));
    is.addOption(GraphTraversalIterator.TRAVERSAL_MODE, mode.name());
    scanner.addScanIterator(is);
  }

  /**
   * Configures an edge filter iterator on the scanner.
   *
   * @param scanner the scanner to configure
   * @param edgeType edge type to accept (null for all types)
   * @param propertyName property to filter on (null to skip property filter)
   * @param operator comparison operator
   * @param value value to compare against
   */
  public static void configureEdgeFilter(Scanner scanner, String edgeType, String propertyName,
      ComparisonOperator operator, String value) {
    IteratorSetting is =
        new IteratorSetting(EDGE_FILTER_ITERATOR_PRIORITY, "edgeFilter", EdgeFilterIterator.class);
    if (edgeType != null) {
      EdgeFilterIterator.setEdgeType(is, edgeType);
    }
    if (propertyName != null && operator != null && value != null) {
      EdgeFilterIterator.setPropertyFilter(is, propertyName, operator, value);
    }
    scanner.addScanIterator(is);
  }

  /**
   * Reads all vertex properties for a given vertex ID.
   *
   * @param scanner a scanner already positioned or configured for the graph table
   * @param vertexId the vertex to read
   * @return map of property name to value
   */
  public static Map<String,String> getVertexProperties(Scanner scanner, String vertexId) {
    scanner.setRange(GraphSchema.vertexRange(vertexId));
    scanner.fetchColumnFamily(GraphSchema.VERTEX_COLFAM_TEXT);

    Map<String,String> properties = new HashMap<>();
    for (Map.Entry<Key,Value> entry : scanner) {
      String colQual = entry.getKey().getColumnQualifier().toString();
      properties.put(colQual, entry.getValue().toString());
    }
    return properties;
  }

  /**
   * Gets the neighbor vertex IDs for outgoing edges of the specified type.
   *
   * @param scanner a scanner configured for the graph table
   * @param vertexId the source vertex
   * @param edgeType the edge type to follow
   * @return list of neighbor vertex IDs
   */
  public static List<String> getNeighbors(Scanner scanner, String vertexId, String edgeType) {
    scanner.setRange(GraphSchema.outgoingEdgesRange(vertexId, edgeType));

    List<String> neighbors = new ArrayList<>();
    for (Map.Entry<Key,Value> entry : scanner) {
      neighbors.add(entry.getKey().getColumnQualifier().toString());
    }
    return neighbors;
  }

  /**
   * Configures a temporal filter on the scanner using Accumulo's built-in TimestampFilter. Filters
   * entries based on their write-time (Accumulo cell timestamp).
   *
   * @param scanner the scanner to configure
   * @param startMillis start of the time range in epoch milliseconds (inclusive), or null for no
   *        lower bound
   * @param endMillis end of the time range in epoch milliseconds (inclusive), or null for no upper
   *        bound
   */
  public static void configureTemporalFilter(Scanner scanner, Long startMillis, Long endMillis) {
    IteratorSetting is = new IteratorSetting(TEMPORAL_FILTER_ITERATOR_PRIORITY, "temporalFilter",
        org.apache.accumulo.core.iterators.user.TimestampFilter.class);
    if (startMillis != null) {
      TimestampFilter.setStart(is, startMillis, true);
    }
    if (endMillis != null) {
      TimestampFilter.setEnd(is, endMillis, true);
    }
    scanner.addScanIterator(is);
  }

  /**
   * Configures a graph aggregation iterator on the scanner.
   *
   * @param scanner the scanner to configure
   * @param type the aggregation type to compute
   * @param vertexId optional vertex ID to scope aggregation
   * @param edgeType optional edge type filter
   * @param timeBucket for GROUP_BY_TIME: HOUR, DAY, WEEK, or MONTH
   * @param maxGroups maximum number of groups to return
   */
  public static void configureAggregation(Scanner scanner, AggregationType type, String vertexId,
      String edgeType, String timeBucket, int maxGroups) {
    IteratorSetting is = new IteratorSetting(AGGREGATION_ITERATOR_PRIORITY, "graphAggregation",
        GraphAggregationIterator.class);
    is.addOption(GraphAggregationIterator.AGGREGATION_TYPE, type.name());
    if (vertexId != null) {
      is.addOption(GraphAggregationIterator.VERTEX_ID, vertexId);
    }
    if (edgeType != null) {
      is.addOption(GraphAggregationIterator.EDGE_TYPE, edgeType);
    }
    if (timeBucket != null) {
      is.addOption(GraphAggregationIterator.TIME_BUCKET, timeBucket);
    }
    is.addOption(GraphAggregationIterator.MAX_GROUPS, String.valueOf(maxGroups));
    scanner.addScanIterator(is);
  }

  private static final int VECTOR_SEEK_ITERATOR_PRIORITY = 25;

  /**
   * Configures a vidx scanner for vector similarity search. Sets the range based on tessellation
   * cell IDs and attaches a {@link VectorSeekIterator} to score candidates.
   *
   * @param vidxScanner a scanner for the vidx table
   * @param queryVector the query embedding
   * @param topK number of results to return
   * @param threshold minimum similarity score
   * @param searchRadius tessellation search radius (controls how many cells are scanned)
   */
  public static void configureVectorSearch(Scanner vidxScanner, float[] queryVector, int topK,
      float threshold, int searchRadius) {
    // Set range to cover the tessellation cell neighborhood
    long[] cellRange = SphericalTessellation.getCellRange(queryVector, searchRadius);
    vidxScanner.setRange(VectorIndexTable.cellRange(cellRange[0], cellRange[1]));

    // Attach VectorSeekIterator to score candidates within the range
    IteratorSetting is =
        new IteratorSetting(VECTOR_SEEK_ITERATOR_PRIORITY, "vectorSeek", VectorSeekIterator.class);
    is.addOption(VectorSeekIterator.QUERY_VECTOR_OPTION, vectorToString(queryVector));
    is.addOption(VectorSeekIterator.TOP_K_OPTION, String.valueOf(topK));
    is.addOption(VectorSeekIterator.THRESHOLD_OPTION, String.valueOf(threshold));
    is.addOption(VectorSeekIterator.SIMILARITY_TYPE_OPTION, "COSINE");
    vidxScanner.addScanIterator(is);
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
}
