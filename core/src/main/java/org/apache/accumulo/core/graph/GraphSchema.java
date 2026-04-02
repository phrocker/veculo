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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Constants and key layout helpers for the graph storage schema. Single source of truth for naming
 * conventions used in the adjacency-list graph model.
 *
 * <p>
 * Schema layout (single table, adjacency-list):
 *
 * <pre>
 * Row          | ColFam        | ColQual          | ColVis | Value
 * -------------|---------------|------------------|--------|------------------
 * &lt;vertexId&gt;   | V             | &lt;propertyName&gt;   | &lt;vis&gt;  | &lt;propertyValue&gt;
 * &lt;vertexId&gt;   | V             | _label           | &lt;vis&gt;  | &lt;vertexLabel&gt;
 * &lt;vertexId&gt;   | V             | _embedding       | &lt;vis&gt;  | VECTOR_FLOAT32
 * &lt;vertexId&gt;   | E_&lt;edgeType&gt;  | &lt;targetVertexId&gt; | &lt;vis&gt;  | &lt;edgePropsJSON&gt;
 * &lt;vertexId&gt;   | EI_&lt;edgeType&gt; | &lt;sourceVertexId&gt; | &lt;vis&gt;  | &lt;edgePropsJSON&gt;
 * </pre>
 */
public final class GraphSchema {

  public static final String VERTEX_COLFAM = "V";
  public static final String EDGE_COLFAM_PREFIX = "E_";
  public static final String EDGE_INVERSE_COLFAM_PREFIX = "EI_";

  public static final String LABEL_COLQUAL = "_label";
  public static final String EMBEDDING_COLQUAL = "_embedding";
  public static final String SUMMARY_COLQUAL = "_summary";
  public static final String EMBEDDING_MODEL_COLQUAL = "_embedding_model";
  public static final String CELL_ID_COLQUAL = "_cell_id";
  public static final String RANK_COLQUAL = "_rank";
  public static final String ANOMALY_SCORE_COLQUAL = "_anomaly_score";

  public static final String PENDING_COLFAM_PREFIX = "P_";
  public static final String SIMILAR_TO_EDGE_TYPE = "SIMILAR_TO";

  // File/multimodal properties
  public static final String FILE_URI_COLQUAL = "_file_uri";
  public static final String CONTENT_TYPE_COLQUAL = "_content_type";
  public static final String FILE_SIZE_COLQUAL = "_file_size";
  public static final String ORIGINAL_FILENAME_COLQUAL = "_original_filename";
  public static final String EXTRACTED_COLQUAL = "_extracted";
  public static final String MODALITY_COLQUAL = "_modality";
  public static final String EXTRACTED_TEXT_COLQUAL = "_extracted_text";
  public static final String PAGE_COUNT_COLQUAL = "_page_count";

  // Multimodal edge types
  public static final String EXTRACTED_FROM_EDGE_TYPE = "EXTRACTED_FROM";
  public static final String CITES_EDGE_TYPE = "CITES";
  public static final String CITED_BY_EDGE_TYPE = "CITED_BY";
  public static final String APPEARS_IN_EDGE_TYPE = "APPEARS_IN";
  public static final String HAS_KEYFRAME_EDGE_TYPE = "HAS_KEYFRAME";
  public static final String IMPLEMENTS_EDGE_TYPE = "IMPLEMENTS";
  public static final String REFERENCES_EDGE_TYPE = "REFERENCES";
  public static final String TRANSCRIBED_FROM_EDGE_TYPE = "TRANSCRIBED_FROM";
  public static final String DEFINES_EDGE_TYPE = "DEFINES";
  public static final String DEFINED_IN_EDGE_TYPE = "DEFINED_IN";
  public static final String IMPORTS_EDGE_TYPE = "IMPORTS";
  public static final String LANGUAGE_COLQUAL = "_language";

  public static final Text VERTEX_COLFAM_TEXT = new Text(VERTEX_COLFAM);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String,String>> MAP_TYPE_REF = new TypeReference<>() {};

  private GraphSchema() {}

  /** Returns the column family for outgoing edges of the given type. */
  public static String edgeColumnFamily(String edgeType) {
    return EDGE_COLFAM_PREFIX + edgeType;
  }

  /** Returns the column family for incoming (inverse) edges of the given type. */
  public static String inverseEdgeColumnFamily(String edgeType) {
    return EDGE_INVERSE_COLFAM_PREFIX + edgeType;
  }

  /** Extracts the edge type from an outgoing edge column family (e.g., "E_knows" -> "knows"). */
  public static String extractEdgeType(String colFam) {
    if (colFam.startsWith(EDGE_COLFAM_PREFIX)) {
      return colFam.substring(EDGE_COLFAM_PREFIX.length());
    }
    if (colFam.startsWith(EDGE_INVERSE_COLFAM_PREFIX)) {
      return colFam.substring(EDGE_INVERSE_COLFAM_PREFIX.length());
    }
    throw new IllegalArgumentException("Not an edge column family: " + colFam);
  }

  /** Returns true if the key represents a vertex property (ColFam = "V"). */
  public static boolean isVertexProperty(Key key) {
    return key.getColumnFamily().toString().equals(VERTEX_COLFAM);
  }

  /** Returns true if the key represents an outgoing edge (ColFam starts with "E_"). */
  public static boolean isOutgoingEdge(Key key) {
    return key.getColumnFamily().toString().startsWith(EDGE_COLFAM_PREFIX);
  }

  /** Returns true if the key represents an incoming (inverse) edge. */
  public static boolean isIncomingEdge(Key key) {
    return key.getColumnFamily().toString().startsWith(EDGE_INVERSE_COLFAM_PREFIX);
  }

  /** Returns a Range covering all entries for the given vertex (entire row). */
  public static Range vertexRange(String vertexId) {
    return Range.exact(vertexId);
  }

  /** Returns a Range covering outgoing edges of the given type for the vertex. */
  public static Range outgoingEdgesRange(String vertexId, String edgeType) {
    return Range.exact(new Text(vertexId), new Text(edgeColumnFamily(edgeType)));
  }

  /** Returns a Range covering incoming edges of the given type for the vertex. */
  public static Range incomingEdgesRange(String vertexId, String edgeType) {
    return Range.exact(new Text(vertexId), new Text(inverseEdgeColumnFamily(edgeType)));
  }

  /** Encodes a map of edge properties to JSON bytes. */
  public static byte[] encodeEdgeProperties(Map<String,String> properties) {
    if (properties == null || properties.isEmpty()) {
      return "{}".getBytes(UTF_8);
    }
    try {
      return MAPPER.writeValueAsBytes(properties);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Decodes JSON bytes to a map of edge properties. */
  public static Map<String,String> decodeEdgeProperties(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return new HashMap<>();
    }
    try {
      return MAPPER.readValue(bytes, MAP_TYPE_REF);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Combines two visibility expressions with AND, producing a compound visibility that requires
   * authorization for both sources. Empty visibilities are treated as public and skipped.
   *
   * @param a first visibility expression
   * @param b second visibility expression
   * @return compound visibility {@code (a)&(b)}, or whichever is non-empty if one is empty
   */
  public static ColumnVisibility compoundVisibility(ColumnVisibility a, ColumnVisibility b) {
    if (a == null || a.getExpression().length == 0) {
      return b == null ? new ColumnVisibility() : b;
    }
    if (b == null || b.getExpression().length == 0) {
      return a;
    }
    String exprA = new String(a.getExpression(), UTF_8);
    String exprB = new String(b.getExpression(), UTF_8);
    if (exprA.equals(exprB)) {
      return a;
    }
    return new ColumnVisibility("(" + exprA + ")&(" + exprB + ")");
  }

  /** Returns true if the key represents a file vertex (has a _file_uri vertex property). */
  public static boolean isFileVertex(Key key) {
    return isVertexProperty(key) && key.getColumnQualifier().toString().equals(FILE_URI_COLQUAL);
  }

  /** Returns true if the key represents a pending marker cell (ColFam starts with "P_"). */
  public static boolean isPendingMarker(Key key) {
    return key.getColumnFamily().toString().startsWith(PENDING_COLFAM_PREFIX);
  }
}
