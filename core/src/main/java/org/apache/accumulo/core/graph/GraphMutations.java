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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Builds Accumulo Mutations for graph write operations. Each method produces one or more Mutations
 * that encode graph structures into the adjacency-list schema defined by {@link GraphSchema}.
 */
public final class GraphMutations {

  private GraphMutations() {}

  /**
   * Creates a Mutation that adds a vertex with a label and properties.
   *
   * @param id vertex identifier (becomes the row ID)
   * @param label vertex label (stored under ColQual "_label")
   * @param properties vertex properties (each stored as a separate ColQual under ColFam "V")
   * @param visibility column visibility expression for all cells
   * @return a single Mutation containing all vertex data
   */
  public static Mutation addVertex(String id, String label, Map<String,String> properties,
      ColumnVisibility visibility) {
    requireNonNull(id, "Vertex ID must not be null");
    requireNonNull(label, "Vertex label must not be null");

    Mutation m = new Mutation(id);
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.LABEL_COLQUAL)
        .visibility(visibility).put(new Value(label));

    if (properties != null) {
      for (Map.Entry<String,String> entry : properties.entrySet()) {
        m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(entry.getKey()).visibility(visibility)
            .put(new Value(entry.getValue()));
      }
    }

    return m;
  }

  /**
   * Creates a mutation for a file vertex with metadata properties. The file itself is stored in
   * GCS; this mutation stores the URI and metadata.
   *
   * @param id vertex identifier (becomes the row ID)
   * @param label vertex label
   * @param fileUri GCS URI for the stored file
   * @param contentType MIME content type of the file
   * @param fileSize file size in bytes
   * @param originalFilename original filename at upload time
   * @param properties additional vertex properties
   * @param visibility column visibility expression for all cells
   * @return a single Mutation containing all file vertex data
   */
  public static Mutation addFileVertex(String id, String label, String fileUri, String contentType,
      long fileSize, String originalFilename, Map<String,String> properties,
      ColumnVisibility visibility) {
    Mutation m = addVertex(id, label, properties, visibility);
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.FILE_URI_COLQUAL)
        .visibility(visibility).put(new Value(fileUri));
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.CONTENT_TYPE_COLQUAL)
        .visibility(visibility).put(new Value(contentType));
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.FILE_SIZE_COLQUAL)
        .visibility(visibility).put(new Value(String.valueOf(fileSize)));
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.ORIGINAL_FILENAME_COLQUAL)
        .visibility(visibility).put(new Value(originalFilename));
    return m;
  }

  /**
   * Creates a Mutation that adds a vector embedding to a vertex.
   *
   * @param id vertex identifier
   * @param embedding float array containing the embedding vector
   * @param visibility column visibility expression
   * @return a Mutation storing the embedding under ColQual "_embedding"
   */
  public static Mutation addVertexEmbedding(String id, float[] embedding,
      ColumnVisibility visibility) {
    requireNonNull(id, "Vertex ID must not be null");
    requireNonNull(embedding, "Embedding must not be null");

    Mutation m = new Mutation(id);
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.EMBEDDING_COLQUAL)
        .visibility(visibility).put(Value.newVector(embedding));
    return m;
  }

  /**
   * Creates Mutations for both the graph table and the vidx table for a vertex embedding. Returns a
   * list of exactly 2 mutations: the first for the graph table, the second for the vidx table.
   *
   * @param id vertex identifier
   * @param embedding float array containing the embedding vector
   * @param visibility column visibility expression
   * @return list of [graphMutation, vidxMutation]
   */
  public static List<Mutation> addVertexEmbeddingWithIndex(String id, float[] embedding,
      ColumnVisibility visibility) {
    return VectorIndexWriter.dualWriteMutations(id, embedding, visibility);
  }

  /**
   * Creates Mutations for a directed edge (forward + inverse). The forward edge is stored in the
   * source row; the inverse edge is stored in the destination row for O(1) incoming edge lookup.
   *
   * @param src source vertex ID
   * @param dst destination vertex ID
   * @param edgeType edge type label (e.g., "knows", "references")
   * @param properties edge properties (stored as JSON)
   * @param visibility column visibility expression
   * @return list of two Mutations: [forward on src row, inverse on dst row]
   */
  public static List<Mutation> addEdge(String src, String dst, String edgeType,
      Map<String,String> properties, ColumnVisibility visibility) {
    requireNonNull(src, "Source vertex ID must not be null");
    requireNonNull(dst, "Destination vertex ID must not be null");
    requireNonNull(edgeType, "Edge type must not be null");

    byte[] propsJson = GraphSchema.encodeEdgeProperties(properties);

    // Forward edge: src row, E_<type> family, dst qualifier
    Mutation forward = new Mutation(src);
    forward.at().family(GraphSchema.edgeColumnFamily(edgeType)).qualifier(dst)
        .visibility(visibility).put(new Value(propsJson));

    // Inverse edge: dst row, EI_<type> family, src qualifier
    Mutation inverse = new Mutation(dst);
    inverse.at().family(GraphSchema.inverseEdgeColumnFamily(edgeType)).qualifier(src)
        .visibility(visibility).put(new Value(propsJson));

    List<Mutation> mutations = new ArrayList<>(2);
    mutations.add(forward);
    mutations.add(inverse);
    return mutations;
  }

  /**
   * Creates Mutations for a weighted edge. The weight is stored as a property in the edge's JSON.
   *
   * @param src source vertex ID
   * @param dst destination vertex ID
   * @param edgeType edge type label
   * @param weight edge weight
   * @param visibility column visibility expression
   * @return list of two Mutations: [forward on src row, inverse on dst row]
   */
  public static List<Mutation> addWeightedEdge(String src, String dst, String edgeType,
      double weight, ColumnVisibility visibility) {
    Map<String,String> properties = new HashMap<>();
    properties.put("weight", String.valueOf(weight));
    return addEdge(src, dst, edgeType, properties, visibility);
  }

  /**
   * Creates Mutations for a timestamped edge. The event time is stored as the {@code _event_time}
   * property in the edge's JSON, allowing temporal queries via {@link EdgeFilterIterator}.
   *
   * @param src source vertex ID
   * @param dst destination vertex ID
   * @param edgeType edge type label
   * @param eventTimeMillis event time in epoch milliseconds
   * @param properties additional edge properties (may be null)
   * @param visibility column visibility expression
   * @return list of two Mutations: [forward on src row, inverse on dst row]
   */
  public static List<Mutation> addTimestampedEdge(String src, String dst, String edgeType,
      long eventTimeMillis, Map<String,String> properties, ColumnVisibility visibility) {
    Map<String,String> props = new HashMap<>(properties != null ? properties : Map.of());
    props.put("_event_time", String.valueOf(eventTimeMillis));
    return addEdge(src, dst, edgeType, props, visibility);
  }

  /**
   * Creates delete Mutations for removing an edge (both forward and inverse).
   *
   * @param src source vertex ID
   * @param dst destination vertex ID
   * @param edgeType edge type label
   * @return list of two Mutations with delete markers
   */
  public static List<Mutation> removeEdge(String src, String dst, String edgeType) {
    requireNonNull(src, "Source vertex ID must not be null");
    requireNonNull(dst, "Destination vertex ID must not be null");
    requireNonNull(edgeType, "Edge type must not be null");

    Mutation forward = new Mutation(src);
    forward.putDelete(GraphSchema.edgeColumnFamily(edgeType), dst);

    Mutation inverse = new Mutation(dst);
    inverse.putDelete(GraphSchema.inverseEdgeColumnFamily(edgeType), src);

    List<Mutation> mutations = new ArrayList<>(2);
    mutations.add(forward);
    mutations.add(inverse);
    return mutations;
  }

  /**
   * Creates a Mutation that deletes an entire vertex row. Note: this only deletes known column
   * families. For a complete delete, the caller should scan the row first to discover all column
   * families, or use an appropriate Accumulo delete mechanism.
   *
   * @param id vertex identifier
   * @return a Mutation with a delete marker for the vertex label
   */
  public static Mutation removeVertex(String id) {
    requireNonNull(id, "Vertex ID must not be null");
    Mutation m = new Mutation(id);
    m.putDelete(GraphSchema.VERTEX_COLFAM, GraphSchema.LABEL_COLQUAL);
    return m;
  }
}
