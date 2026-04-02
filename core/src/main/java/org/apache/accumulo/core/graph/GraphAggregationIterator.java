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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Server-side aggregation iterator for graph queries. Materializes aggregate results during
 * {@link #seek} and emits them as synthetic {@link Key},{@link Value} pairs.
 *
 * <p>
 * Supported aggregation types:
 * <ul>
 * <li>{@code COUNT} — count entries matching optional vertex/edge filters</li>
 * <li>{@code COUNT_DISTINCT} — count distinct target vertex IDs (column qualifiers)</li>
 * <li>{@code GROUP_BY_EDGE_TYPE} — group edge counts by edge type</li>
 * <li>{@code GROUP_BY_TIME} — group edge counts by timestamp bucket</li>
 * <li>{@code DEGREE} — compute in-degree, out-degree, and total for a single vertex</li>
 * <li>{@code TOP_CONNECTED} — find the most-connected vertices by out-degree</li>
 * </ul>
 *
 * <p>
 * Results are emitted with synthetic keys: Row={@code _agg}, ColFam={@code result},
 * ColQual=&lt;group_key&gt;, Value=JSON.
 */
public class GraphAggregationIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String AGGREGATION_TYPE = "aggregationType";
  public static final String VERTEX_ID = "vertexId";
  public static final String EDGE_TYPE = "edgeType";
  public static final String TIME_BUCKET = "timeBucket";
  public static final String MAX_GROUPS = "maxGroups";

  private static final String AGG_ROW = "_agg";
  private static final String RESULT_COLFAM = "result";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum AggregationType {
    COUNT, COUNT_DISTINCT, GROUP_BY_EDGE_TYPE, GROUP_BY_TIME, DEGREE, TOP_CONNECTED
  }

  public enum TimeBucket {
    HOUR(3600000L), DAY(86400000L), WEEK(604800000L), MONTH(2592000000L);

    public final long millis;

    TimeBucket(long millis) {
      this.millis = millis;
    }
  }

  private SortedKeyValueIterator<Key,Value> source;

  private AggregationType aggregationType;
  private String vertexId;
  private String edgeType;
  private TimeBucket timeBucket;
  private int maxGroups = 10000;

  private List<Map.Entry<Key,Value>> results;
  private int currentResultIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(AGGREGATION_TYPE)) {
      aggregationType = AggregationType.valueOf(options.get(AGGREGATION_TYPE));
    } else {
      throw new IllegalArgumentException("aggregationType option is required");
    }

    if (options.containsKey(VERTEX_ID)) {
      vertexId = options.get(VERTEX_ID);
    }
    if (options.containsKey(EDGE_TYPE)) {
      edgeType = options.get(EDGE_TYPE);
    }
    if (options.containsKey(TIME_BUCKET)) {
      timeBucket = TimeBucket.valueOf(options.get(TIME_BUCKET));
    }
    if (options.containsKey(MAX_GROUPS)) {
      maxGroups = Integer.parseInt(options.get(MAX_GROUPS));
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

    // Seek the source iterator
    source.seek(range, columnFamilies, inclusive);

    switch (aggregationType) {
      case COUNT:
        computeCount();
        break;
      case COUNT_DISTINCT:
        computeCountDistinct();
        break;
      case GROUP_BY_EDGE_TYPE:
        computeGroupByEdgeType();
        break;
      case GROUP_BY_TIME:
        computeGroupByTime();
        break;
      case DEGREE:
        computeDegree();
        break;
      case TOP_CONNECTED:
        computeTopConnected();
        break;
    }

    // Sort results by Key for correct iterator ordering
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  /**
   * COUNT: count entries matching optional vertexId + edgeType filter.
   */
  private void computeCount() throws IOException {
    long count = 0;

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }
      Key key = source.getTopKey();
      if (isEdgeEntry(key) && matchesFilter(key)) {
        count++;
      }
      source.next();
    }

    addResult("total", Map.of("count", count));
  }

  /**
   * COUNT_DISTINCT: count distinct target vertex IDs (column qualifiers on edge entries).
   */
  private void computeCountDistinct() throws IOException {
    Set<String> distinctTargets = new HashSet<>();

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }
      Key key = source.getTopKey();
      if (isEdgeEntry(key) && matchesFilter(key)) {
        distinctTargets.add(key.getColumnQualifier().toString());
        if (distinctTargets.size() >= maxGroups) {
          break;
        }
      }
      source.next();
    }

    addResult("total", Map.of("count", distinctTargets.size()));
  }

  /**
   * GROUP_BY_EDGE_TYPE: group edge counts by edge type extracted from column family.
   */
  private void computeGroupByEdgeType() throws IOException {
    Map<String,Long> typeCounts = new LinkedHashMap<>();

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }
      Key key = source.getTopKey();
      String colFam = key.getColumnFamily().toString();

      if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
        String type = colFam.substring(GraphSchema.EDGE_COLFAM_PREFIX.length());
        typeCounts.merge(type, 1L, Long::sum);
        if (typeCounts.size() >= maxGroups) {
          // Stop tracking new types, but continue counting existing ones
          while (source.hasTop()) {
            if (isRunningLowOnMemory()) {
              break;
            }
            Key k2 = source.getTopKey();
            String cf2 = k2.getColumnFamily().toString();
            if (cf2.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)) {
              String t2 = cf2.substring(GraphSchema.EDGE_COLFAM_PREFIX.length());
              typeCounts.computeIfPresent(t2, (k, v) -> v + 1);
            }
            source.next();
          }
          break;
        }
      }
      source.next();
    }

    for (Map.Entry<String,Long> entry : typeCounts.entrySet()) {
      addResult(entry.getKey(), Map.of("edge_type", entry.getKey(), "count", entry.getValue()));
    }
  }

  /**
   * GROUP_BY_TIME: group edge counts by timestamp bucket.
   */
  private void computeGroupByTime() throws IOException {
    if (timeBucket == null) {
      timeBucket = TimeBucket.DAY;
    }
    long bucketMillis = timeBucket.millis;
    Map<Long,Long> bucketCounts = new LinkedHashMap<>();

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }
      Key key = source.getTopKey();
      if (isEdgeEntry(key) && matchesFilter(key)) {
        long bucket = (key.getTimestamp() / bucketMillis) * bucketMillis;
        bucketCounts.merge(bucket, 1L, Long::sum);
        if (bucketCounts.size() >= maxGroups) {
          // Stop tracking new buckets, but continue counting existing ones
          while (source.hasTop()) {
            if (isRunningLowOnMemory()) {
              break;
            }
            Key k2 = source.getTopKey();
            if (isEdgeEntry(k2) && matchesFilter(k2)) {
              long b2 = (k2.getTimestamp() / bucketMillis) * bucketMillis;
              bucketCounts.computeIfPresent(b2, (k, v) -> v + 1);
            }
            source.next();
          }
          break;
        }
      }
      source.next();
    }

    for (Map.Entry<Long,Long> entry : bucketCounts.entrySet()) {
      addResult(String.valueOf(entry.getKey()),
          Map.of("bucket", entry.getKey(), "count", entry.getValue()));
    }
  }

  /**
   * DEGREE: for a single vertex, count outgoing (E_) and incoming (EI_) edges separately.
   */
  private void computeDegree() throws IOException {
    long outDegree = 0;
    long inDegree = 0;

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }
      Key key = source.getTopKey();
      String colFam = key.getColumnFamily().toString();

      if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)
          && !colFam.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX)) {
        if (edgeType == null || colFam.equals(GraphSchema.edgeColumnFamily(edgeType))) {
          outDegree++;
        }
      } else if (colFam.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX)) {
        if (edgeType == null || colFam.equals(GraphSchema.inverseEdgeColumnFamily(edgeType))) {
          inDegree++;
        }
      }

      source.next();
    }

    Map<String,Object> result = new LinkedHashMap<>();
    result.put("in_degree", inDegree);
    result.put("out_degree", outDegree);
    result.put("total", inDegree + outDegree);
    addResult("degree", result);
  }

  /**
   * TOP_CONNECTED: full scan, track out-degree per vertex (count E_ entries per row), sort
   * descending, return top N.
   */
  private void computeTopConnected() throws IOException {
    Map<String,Integer> degreeCounts = new HashMap<>();

    while (source.hasTop()) {
      if (isRunningLowOnMemory()) {
        break;
      }
      Key key = source.getTopKey();
      String colFam = key.getColumnFamily().toString();

      if (colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)
          && !colFam.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX)) {
        String row = key.getRow().toString();
        degreeCounts.merge(row, 1, Integer::sum);
      }

      source.next();
    }

    // Sort by degree descending and take top N
    List<Map.Entry<String,Integer>> sorted = degreeCounts.entrySet().stream()
        .sorted(Map.Entry.<String,Integer>comparingByValue().reversed()).limit(maxGroups)
        .collect(Collectors.toList());

    for (Map.Entry<String,Integer> entry : sorted) {
      addResult(entry.getKey(), Map.of("vertex_id", entry.getKey(), "degree", entry.getValue()));
    }
  }

  /**
   * Returns true if the key represents an edge entry (E_ or EI_ column family).
   */
  private boolean isEdgeEntry(Key key) {
    String colFam = key.getColumnFamily().toString();
    return colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)
        || colFam.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX);
  }

  /**
   * Checks if the entry matches the optional vertexId and edgeType filters.
   */
  private boolean matchesFilter(Key key) {
    if (vertexId != null && !key.getRow().toString().equals(vertexId)) {
      return false;
    }
    if (edgeType != null) {
      String colFam = key.getColumnFamily().toString();
      return colFam.equals(GraphSchema.edgeColumnFamily(edgeType))
          || colFam.equals(GraphSchema.inverseEdgeColumnFamily(edgeType));
    }
    return true;
  }

  /**
   * Adds a synthetic result entry to the output list.
   */
  private void addResult(String groupKey, Map<String,Object> value) {
    Key key = new Key(AGG_ROW, RESULT_COLFAM, groupKey);
    try {
      byte[] json = MAPPER.writeValueAsBytes(value);
      results.add(Map.entry(key, new Value(json)));
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    GraphAggregationIterator copy = new GraphAggregationIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy GraphAggregationIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    options.put(AGGREGATION_TYPE, aggregationType.name());
    if (vertexId != null) {
      options.put(VERTEX_ID, vertexId);
    }
    if (edgeType != null) {
      options.put(EDGE_TYPE, edgeType);
    }
    if (timeBucket != null) {
      options.put(TIME_BUCKET, timeBucket.name());
    }
    options.put(MAX_GROUPS, String.valueOf(maxGroups));
    return options;
  }
}
