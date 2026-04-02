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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds mutations for the {@code graph_vidx} secondary index table and provides dual-write helpers
 * that write to both graph and vidx tables atomically (at the batch level).
 */
public final class VectorIndexWriter {

  private static final Logger log = LoggerFactory.getLogger(VectorIndexWriter.class);

  /** Column family for latent edge links discovered at write time. */
  public static final String LINK_COLFAM = "link";

  /** Default similarity threshold for write-path latent edge discovery. */
  public static final float DEFAULT_LINK_THRESHOLD = 0.85f;

  /** Max embeddings to compare per cell during write-path discovery. */
  public static final int DEFAULT_MAX_CELL_SCAN = 200;

  private VectorIndexWriter() {}

  /**
   * Creates a single Mutation for the vidx table that stores an embedding indexed by its
   * tessellation cell ID.
   *
   * @param vertexId the vertex identifier
   * @param embedding the float array embedding
   * @param visibility column visibility to inherit from the source vertex
   * @return a Mutation with row key {@code <cellIdHex>:<vertexId>}, CF "V", CQ "_embedding"
   */
  public static Mutation vidxMutation(String vertexId, float[] embedding,
      ColumnVisibility visibility) {
    requireNonNull(vertexId, "Vertex ID must not be null");
    requireNonNull(embedding, "Embedding must not be null");

    long cellId = SphericalTessellation.assignCellId(embedding);
    String rowKey = VectorIndexTable.rowKey(cellId, vertexId);

    Mutation m = new Mutation(rowKey);
    m.at().family(GraphSchema.VERTEX_COLFAM).qualifier(GraphSchema.EMBEDDING_COLQUAL)
        .visibility(visibility).put(Value.newVector(embedding));
    return m;
  }

  /**
   * Creates mutations for both the graph table and the vidx table for a single vertex embedding.
   *
   * @param vertexId the vertex identifier
   * @param embedding the float array embedding
   * @param visibility column visibility expression
   * @return a list of exactly 2 mutations: [graphMutation, vidxMutation]
   */
  public static List<Mutation> dualWriteMutations(String vertexId, float[] embedding,
      ColumnVisibility visibility) {
    Mutation graphMut = GraphMutations.addVertexEmbedding(vertexId, embedding, visibility);
    Mutation vidxMut = vidxMutation(vertexId, embedding, visibility);

    List<Mutation> mutations = new ArrayList<>(2);
    mutations.add(graphMut);
    mutations.add(vidxMut);
    return mutations;
  }

  /**
   * Writes a vertex embedding to both the graph table and the vidx table using the provided batch
   * writers.
   *
   * @param graphWriter batch writer for the graph table
   * @param vidxWriter batch writer for the vidx table
   * @param vertexId the vertex identifier
   * @param embedding the float array embedding
   * @param visibility column visibility expression
   */
  public static void dualWrite(BatchWriter graphWriter, BatchWriter vidxWriter, String vertexId,
      float[] embedding, ColumnVisibility visibility) throws MutationsRejectedException {
    graphWriter.addMutation(GraphMutations.addVertexEmbedding(vertexId, embedding, visibility));
    vidxWriter.addMutation(vidxMutation(vertexId, embedding, visibility));
  }

  /**
   * Deterministic Write-Path Discovery: after writing an embedding to vidx, scan the tessellation
   * cell for existing embeddings and create {@code link} column family entries for any pair
   * exceeding the similarity threshold.
   *
   * <p>
   * Schema: Row={cellIdHex}:{vertexId}, CF="link", CQ={neighborVertexId}, Value={score as string}
   *
   * <p>
   * This provides real-time latent edge discovery — edges appear the millisecond the vertex is
   * ACKed, with zero write amplification to the graph table. The {@code link} entries live in vidx
   * and are merged at query time.
   *
   * @param vidxScanner scanner on the vidx table (caller manages lifecycle)
   * @param vidxWriter batch writer for the vidx table (caller manages lifecycle)
   * @param vertexId the newly written vertex ID
   * @param embedding the vertex's embedding
   * @param cellId the tessellation cell ID (from SphericalTessellation.assignCellId)
   * @param visibility column visibility
   * @param threshold minimum cosine similarity for link creation
   */
  public static void discoverLinksInCell(Scanner vidxScanner, BatchWriter vidxWriter,
      String vertexId, float[] embedding, long cellId, ColumnVisibility visibility,
      float threshold) {

    String cellHex = VectorIndexTable.formatCellId(cellId);
    String myRowKey = VectorIndexTable.rowKey(cellId, vertexId);

    // Scan the cell for existing embeddings
    vidxScanner.clearColumns();
    vidxScanner.clearScanIterators();
    vidxScanner.setRange(VectorIndexTable.cellRange(cellId, cellId));
    vidxScanner.fetchColumn(new Text(GraphSchema.VERTEX_COLFAM),
        new Text(GraphSchema.EMBEDDING_COLQUAL));

    int compared = 0;
    int linksCreated = 0;

    try {
      for (Map.Entry<Key,Value> entry : vidxScanner) {
        String row = entry.getKey().getRow().toString();

        // Skip self
        if (row.equals(myRowKey)) {
          continue;
        }

        // Skip link entries (shouldn't appear with fetchColumn, but guard)
        if (entry.getKey().getColumnFamily().toString().equals(LINK_COLFAM)) {
          continue;
        }

        // Parse neighbor embedding from raw bytes
        float[] neighborEmbedding = parseEmbedding(entry.getValue());
        if (neighborEmbedding == null || neighborEmbedding.length != embedding.length) {
          continue;
        }

        if (++compared > DEFAULT_MAX_CELL_SCAN) {
          break; // Safety cap for hot cells
        }

        float similarity = cosineSimilarity(embedding, neighborEmbedding);
        if (similarity >= threshold) {
          String neighborId = VectorIndexTable.extractVertexId(row);
          String scoreStr = String.format("%.4f", similarity);

          // Write bidirectional link entries:
          // On my row: link:<neighborId> = score
          Mutation myLink = new Mutation(myRowKey);
          myLink.at().family(LINK_COLFAM).qualifier(neighborId).visibility(visibility)
              .put(new Value(scoreStr));

          // On neighbor's row: link:<myId> = score
          Mutation neighborLink = new Mutation(row);
          neighborLink.at().family(LINK_COLFAM).qualifier(vertexId).visibility(visibility)
              .put(new Value(scoreStr));

          vidxWriter.addMutation(myLink);
          vidxWriter.addMutation(neighborLink);
          linksCreated++;
        }
      }

      if (linksCreated > 0) {
        log.info("Write-path discovery: vertex '{}' → {} links in cell {} ({} compared)", vertexId,
            linksCreated, cellHex, compared);
      }
    } catch (Exception e) {
      log.debug("Link discovery failed for vertex '{}': {}", vertexId, e.getMessage());
    }
  }

  /**
   * Combined dual-write + write-path link discovery. Writes the embedding to both tables, then
   * immediately scans the tessellation cell for similar existing embeddings and creates link
   * entries.
   */
  public static void dualWriteWithDiscovery(BatchWriter graphWriter, BatchWriter vidxWriter,
      Scanner vidxScanner, String vertexId, float[] embedding, ColumnVisibility visibility,
      float threshold) throws MutationsRejectedException {

    long cellId = SphericalTessellation.assignCellId(embedding);

    // Step 1: Write embedding to both tables + store cell ID on graph vertex for O(1) lookup
    graphWriter.addMutation(GraphMutations.addVertexEmbedding(vertexId, embedding, visibility));
    Mutation cellIdMut = new Mutation(vertexId);
    cellIdMut.at().family(GraphSchema.VERTEX_COLFAM).qualifier("_cell_id").visibility(visibility)
        .put(new Value(VectorIndexTable.formatCellId(cellId)));
    graphWriter.addMutation(cellIdMut);

    Mutation vidxMut = vidxMutation(vertexId, embedding, visibility);
    vidxWriter.addMutation(vidxMut);
    vidxWriter.flush(); // Flush so the cell scan can see this vertex

    // Step 2: Scan cell and create links
    discoverLinksInCell(vidxScanner, vidxWriter, vertexId, embedding, cellId, visibility,
        threshold);
  }

  private static float[] parseEmbedding(Value value) {
    try {
      byte[] bytes = value.get();
      if (bytes.length < 4 || bytes.length % 4 != 0) {
        return null;
      }
      float[] vector = new float[bytes.length / 4];
      ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(vector);
      return vector;
    } catch (Exception e) {
      return null;
    }
  }

  private static float cosineSimilarity(float[] a, float[] b) {
    float dot = 0f, normA = 0f, normB = 0f;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    if (normA == 0f || normB == 0f) {
      return 0f;
    }
    return dot / (float) (Math.sqrt(normA) * Math.sqrt(normB));
  }
}
