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
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Server-side edge filter iterator. Filters edges by type, property values, and weight thresholds.
 * Non-edge entries (column family "V") always pass through.
 *
 * <p>
 * Extends {@link Filter} using the accept/reject pattern. Implements
 * {@link org.apache.accumulo.core.iterators.OptionDescriber} for shell integration.
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code edgeType} — only accept edges of this type</li>
 * <li>{@code propertyName} — edge property to filter on</li>
 * <li>{@code operator} — comparison operator (EQ, GT, LT, GTE, LTE)</li>
 * <li>{@code propertyValue} — value to compare against</li>
 * <li>{@code minWeight} — minimum weight threshold (inclusive)</li>
 * <li>{@code maxWeight} — maximum weight threshold (inclusive)</li>
 * </ul>
 */
public class EdgeFilterIterator extends Filter {

  public static final String EDGE_TYPE = "edgeType";
  public static final String PROPERTY_NAME = "propertyName";
  public static final String OPERATOR = "operator";
  public static final String PROPERTY_VALUE = "propertyValue";
  public static final String MIN_WEIGHT = "minWeight";
  public static final String MAX_WEIGHT = "maxWeight";
  public static final String EVENT_TIME_START = "eventTimeStart";
  public static final String EVENT_TIME_END = "eventTimeEnd";

  public enum ComparisonOperator {
    EQ, GT, LT, GTE, LTE
  }

  private String edgeType;
  private String propertyName;
  private ComparisonOperator operator;
  private String propertyValue;
  private Double minWeight;
  private Double maxWeight;
  private Long eventTimeStart;
  private Long eventTimeEnd;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    if (options.containsKey(EDGE_TYPE)) {
      edgeType = options.get(EDGE_TYPE);
    }
    if (options.containsKey(PROPERTY_NAME)) {
      propertyName = options.get(PROPERTY_NAME);
    }
    if (options.containsKey(OPERATOR)) {
      operator = ComparisonOperator.valueOf(options.get(OPERATOR));
    }
    if (options.containsKey(PROPERTY_VALUE)) {
      propertyValue = options.get(PROPERTY_VALUE);
    }
    if (options.containsKey(MIN_WEIGHT)) {
      minWeight = Double.parseDouble(options.get(MIN_WEIGHT));
    }
    if (options.containsKey(MAX_WEIGHT)) {
      maxWeight = Double.parseDouble(options.get(MAX_WEIGHT));
    }
    if (options.containsKey(EVENT_TIME_START)) {
      eventTimeStart = Long.parseLong(options.get(EVENT_TIME_START));
    }
    if (options.containsKey(EVENT_TIME_END)) {
      eventTimeEnd = Long.parseLong(options.get(EVENT_TIME_END));
    }
  }

  @Override
  public boolean accept(Key k, Value v) {
    String colFam = k.getColumnFamily().toString();

    // Non-edge entries (vertex properties) always pass through
    if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
      return true;
    }

    // Only filter outgoing and incoming edges
    boolean isEdge = colFam.startsWith(GraphSchema.EDGE_COLFAM_PREFIX)
        || colFam.startsWith(GraphSchema.EDGE_INVERSE_COLFAM_PREFIX);
    if (!isEdge) {
      return true;
    }

    // Filter by edge type if specified
    if (edgeType != null) {
      String actualType = GraphSchema.extractEdgeType(colFam);
      if (!edgeType.equals(actualType)) {
        return false;
      }
    }

    // Filter by property or weight if specified
    if ((propertyName != null && operator != null && propertyValue != null) || minWeight != null
        || maxWeight != null) {
      Map<String,String> props;
      try {
        props = GraphSchema.decodeEdgeProperties(v.get());
      } catch (Exception e) {
        return false;
      }

      // Weight threshold filtering
      if (minWeight != null || maxWeight != null) {
        String weightStr = props.get("weight");
        if (weightStr == null) {
          return false;
        }
        double weight;
        try {
          weight = Double.parseDouble(weightStr);
        } catch (NumberFormatException e) {
          return false;
        }
        if (minWeight != null && weight < minWeight) {
          return false;
        }
        if (maxWeight != null && weight > maxWeight) {
          return false;
        }
      }

      // Property value comparison
      if (propertyName != null && operator != null && propertyValue != null) {
        String actual = props.get(propertyName);
        if (actual == null) {
          return false;
        }
        if (!compareProperty(actual, operator, propertyValue)) {
          return false;
        }
      }
    }

    // Event time filtering on edge entries
    if (eventTimeStart != null || eventTimeEnd != null) {
      Map<String,String> props;
      try {
        props = GraphSchema.decodeEdgeProperties(v.get());
      } catch (Exception e) {
        return false;
      }
      String eventTimeStr = props.get("_event_time");
      if (eventTimeStr == null) {
        return false;
      }
      long eventTime;
      try {
        eventTime = Long.parseLong(eventTimeStr);
      } catch (NumberFormatException e) {
        return false;
      }
      if (eventTimeStart != null && eventTime < eventTimeStart) {
        return false;
      }
      if (eventTimeEnd != null && eventTime > eventTimeEnd) {
        return false;
      }
    }

    return true;
  }

  private static boolean compareProperty(String actual, ComparisonOperator op, String expected) {
    // Try numeric comparison first, fall back to string comparison
    try {
      double actualNum = Double.parseDouble(actual);
      double expectedNum = Double.parseDouble(expected);
      switch (op) {
        case EQ:
          return actualNum == expectedNum;
        case GT:
          return actualNum > expectedNum;
        case LT:
          return actualNum < expectedNum;
        case GTE:
          return actualNum >= expectedNum;
        case LTE:
          return actualNum <= expectedNum;
        default:
          return false;
      }
    } catch (NumberFormatException e) {
      // Fall back to string comparison
      int cmp = actual.compareTo(expected);
      switch (op) {
        case EQ:
          return cmp == 0;
        case GT:
          return cmp > 0;
        case LT:
          return cmp < 0;
        case GTE:
          return cmp >= 0;
        case LTE:
          return cmp <= 0;
        default:
          return false;
      }
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("edgeFilter");
    io.setDescription("Filters graph edges by type, property values, and weight thresholds");
    io.addNamedOption(EDGE_TYPE, "Edge type to accept (e.g., 'knows')");
    io.addNamedOption(PROPERTY_NAME, "Edge property name to filter on");
    io.addNamedOption(OPERATOR, "Comparison operator: EQ, GT, LT, GTE, LTE");
    io.addNamedOption(PROPERTY_VALUE, "Value to compare the property against");
    io.addNamedOption(MIN_WEIGHT, "Minimum edge weight (inclusive)");
    io.addNamedOption(MAX_WEIGHT, "Maximum edge weight (inclusive)");
    io.addNamedOption(EVENT_TIME_START, "Event time range start in epoch milliseconds (inclusive)");
    io.addNamedOption(EVENT_TIME_END, "Event time range end in epoch milliseconds (inclusive)");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    super.validateOptions(options);
    if (options.containsKey(OPERATOR)) {
      try {
        ComparisonOperator.valueOf(options.get(OPERATOR));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid operator: " + options.get(OPERATOR));
      }
    }
    if (options.containsKey(MIN_WEIGHT)) {
      try {
        Double.parseDouble(options.get(MIN_WEIGHT));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid minWeight: " + options.get(MIN_WEIGHT));
      }
    }
    if (options.containsKey(MAX_WEIGHT)) {
      try {
        Double.parseDouble(options.get(MAX_WEIGHT));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid maxWeight: " + options.get(MAX_WEIGHT));
      }
    }
    if (options.containsKey(EVENT_TIME_START)) {
      try {
        Long.parseLong(options.get(EVENT_TIME_START));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid eventTimeStart: " + options.get(EVENT_TIME_START));
      }
    }
    if (options.containsKey(EVENT_TIME_END)) {
      try {
        Long.parseLong(options.get(EVENT_TIME_END));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid eventTimeEnd: " + options.get(EVENT_TIME_END));
      }
    }
    return true;
  }

  /** Convenience method to configure edge type filtering on an IteratorSetting. */
  public static void setEdgeType(IteratorSetting is, String edgeType) {
    is.addOption(EDGE_TYPE, edgeType);
  }

  /** Convenience method to configure property filtering on an IteratorSetting. */
  public static void setPropertyFilter(IteratorSetting is, String propertyName,
      ComparisonOperator operator, String value) {
    is.addOption(PROPERTY_NAME, propertyName);
    is.addOption(OPERATOR, operator.name());
    is.addOption(PROPERTY_VALUE, value);
  }

  /** Convenience method to configure weight range filtering on an IteratorSetting. */
  public static void setWeightRange(IteratorSetting is, Double minWeight, Double maxWeight) {
    if (minWeight != null) {
      is.addOption(MIN_WEIGHT, String.valueOf(minWeight));
    }
    if (maxWeight != null) {
      is.addOption(MAX_WEIGHT, String.valueOf(maxWeight));
    }
  }

  /** Convenience method to configure event time range filtering on an IteratorSetting. */
  public static void setEventTimeRange(IteratorSetting is, Long start, Long end) {
    if (start != null) {
      is.addOption(EVENT_TIME_START, String.valueOf(start));
    }
    if (end != null) {
      is.addOption(EVENT_TIME_END, String.valueOf(end));
    }
  }
}
