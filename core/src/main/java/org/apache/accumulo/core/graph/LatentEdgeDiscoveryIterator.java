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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * Major compaction iterator for the {@code graph_vidx} table that discovers latent edges between
 * vertices whose embeddings are highly similar within the same tessellation cell.
 *
 * <p>
 * During compaction, this iterator buffers embeddings per tessellation cell. When all entries for a
 * cell have been read, it performs pairwise cosine similarity comparisons. Pairs exceeding the
 * configured similarity threshold produce a LinkMarker entry in the vidx table with row key format
 * {@code <cellIdHex>:LM:<vertexA>:<vertexB>}.
 *
 * <p>
 * At query time, {@code listEdges} scans for LinkMarker entries and returns them as
 * {@code SIMILAR_TO} edges. This achieves zero write amplification to the graph table -- latent
 * edges live only in vidx.
 *
 * <p>
 * All original entries are passed through unchanged (required for compaction iterators). LinkMarker
 * entries are merged with original entries and emitted in sorted Key order.
 */
public class LatentEdgeDiscoveryIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String SIMILARITY_THRESHOLD = "similarityThreshold";
  public static final String MAX_PAIRS_PER_CELL = "maxPairsPerCell";
  public static final String MAX_CELL_BUFFER = "maxCellBuffer";

  private static final String LINK_MARKER = "LM";
  private static final String VERTEX_COLFAM = "V";
  private static final String EMBEDDING_COLQUAL = "_embedding";
  private static final String SIMILAR_COLQUAL = "_similar";

  private SortedKeyValueIterator<Key,Value> source;

  private float similarityThreshold = 0.85f;
  private int maxPairsPerCell = 500;
  private int maxCellBuffer = 200;

  // Merged output buffer: original entries + generated LinkMarker entries, sorted by Key
  private List<Map.Entry<Key,Value>> outputBuffer;
  private int outputIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;

    if (options.containsKey(SIMILARITY_THRESHOLD)) {
      similarityThreshold = Float.parseFloat(options.get(SIMILARITY_THRESHOLD));
    }
    if (options.containsKey(MAX_PAIRS_PER_CELL)) {
      maxPairsPerCell = Integer.parseInt(options.get(MAX_PAIRS_PER_CELL));
    }
    if (options.containsKey(MAX_CELL_BUFFER)) {
      maxCellBuffer = Integer.parseInt(options.get(MAX_CELL_BUFFER));
    }

    outputBuffer = new ArrayList<>();
    outputIndex = 0;
  }

  @Override
  public boolean hasTop() {
    return outputIndex < outputBuffer.size();
  }

  @Override
  public void next() throws IOException {
    outputIndex++;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    outputBuffer.clear();
    outputIndex = 0;

    source.seek(range, columnFamilies, inclusive);

    // Use a TreeMap to merge original entries and LinkMarker entries in sorted Key order
    TreeMap<Key,Value> merged = new TreeMap<>();

    // Buffer embeddings per cell for pairwise comparison
    String currentCellId = null;
    Map<String,float[]> cellEmbeddings = new HashMap<>();

    while (source.hasTop()) {
      Key key = new Key(source.getTopKey());
      Value value = new Value(source.getTopValue());

      // Always pass through original entries
      merged.put(key, value);

      String row = key.getRow().toString();
      String cf = key.getColumnFamily().toString();
      String cq = key.getColumnQualifier().toString();

      // Skip existing link entries (CF="link" — already discovered edges)
      if (cf.equals(VectorIndexWriter.LINK_COLFAM)) {
        source.next();
        continue;
      }

      // Only process embedding entries: CF=V, CQ=_embedding
      if (!VERTEX_COLFAM.equals(cf) || !EMBEDDING_COLQUAL.equals(cq)) {
        source.next();
        continue;
      }

      // Parse cell ID and vertex ID from row key: <cellIdHex>:<vertexId>
      int sepIdx = row.indexOf(':');
      if (sepIdx < 0) {
        source.next();
        continue;
      }
      String cellId = row.substring(0, sepIdx);
      String vertexId = row.substring(sepIdx + 1);

      // If we moved to a new cell, process the previous cell's buffer
      if (currentCellId != null && !currentCellId.equals(cellId)) {
        processCell(currentCellId, cellEmbeddings, merged);
        cellEmbeddings.clear();
      }
      currentCellId = cellId;

      // Parse embedding from value bytes (BIG_ENDIAN float array)
      float[] embedding = parseEmbedding(value);
      if (embedding != null && cellEmbeddings.size() < maxCellBuffer) {
        cellEmbeddings.put(vertexId, embedding);
      }

      source.next();
    }

    // Process the last cell
    if (currentCellId != null && !cellEmbeddings.isEmpty()) {
      processCell(currentCellId, cellEmbeddings, merged);
    }

    // Convert sorted map to list for indexed access
    for (Map.Entry<Key,Value> entry : merged.entrySet()) {
      outputBuffer.add(entry);
    }
  }

  /**
   * Performs pairwise cosine similarity comparison for all embeddings in a cell. Generates
   * LinkMarker entries for pairs exceeding the similarity threshold.
   */
  private void processCell(String cellId, Map<String,float[]> embeddings,
      TreeMap<Key,Value> merged) {
    List<String> vertexIds = new ArrayList<>(embeddings.keySet());
    int pairsChecked = 0;

    for (int i = 0; i < vertexIds.size() && pairsChecked < maxPairsPerCell; i++) {
      for (int j = i + 1; j < vertexIds.size() && pairsChecked < maxPairsPerCell; j++) {
        String vertexA = vertexIds.get(i);
        String vertexB = vertexIds.get(j);
        float[] embA = embeddings.get(vertexA);
        float[] embB = embeddings.get(vertexB);

        if (embA.length != embB.length) {
          pairsChecked++;
          continue;
        }

        float similarity = cosineSimilarity(embA, embB);
        pairsChecked++;

        if (similarity >= similarityThreshold) {
          String scoreStr = String.format("%.4f", similarity);
          Value linkValue = new Value(scoreStr);

          // Write bidirectional link entries using "link" column family
          // Row: <cellIdHex>:<vertexA>, CF: link, CQ: <vertexB>
          String rowA = cellId + ":" + vertexA;
          Key linkKeyA = new Key(new Text(rowA), new Text(VectorIndexWriter.LINK_COLFAM),
              new Text(vertexB), System.currentTimeMillis());
          merged.put(linkKeyA, linkValue);

          // Row: <cellIdHex>:<vertexB>, CF: link, CQ: <vertexA>
          String rowB = cellId + ":" + vertexB;
          Key linkKeyB = new Key(new Text(rowB), new Text(VectorIndexWriter.LINK_COLFAM),
              new Text(vertexA), System.currentTimeMillis());
          merged.put(linkKeyB, linkValue);
        }
      }
    }
  }

  /**
   * Parses an embedding from raw value bytes using BIG_ENDIAN float encoding, matching the format
   * used by VectorIterator.
   */
  private static float[] parseEmbedding(Value value) {
    byte[] bytes = value.get();
    if (bytes == null || bytes.length < 4 || bytes.length % 4 != 0) {
      return null;
    }
    float[] vector = new float[bytes.length / 4];
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(vector);
    return vector;
  }

  /**
   * Computes cosine similarity between two vectors: dot(a,b) / (||a|| * ||b||).
   */
  private static float cosineSimilarity(float[] a, float[] b) {
    float dotProduct = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;

    for (int i = 0; i < a.length; i++) {
      dotProduct += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }

    if (normA == 0.0f || normB == 0.0f) {
      return 0.0f;
    }

    return dotProduct / (float) (Math.sqrt(normA) * Math.sqrt(normB));
  }

  @Override
  public Key getTopKey() {
    if (!hasTop()) {
      return null;
    }
    return outputBuffer.get(outputIndex).getKey();
  }

  @Override
  public Value getTopValue() {
    if (!hasTop()) {
      return null;
    }
    return outputBuffer.get(outputIndex).getValue();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    LatentEdgeDiscoveryIterator copy = new LatentEdgeDiscoveryIterator();
    try {
      Map<String,String> options = new HashMap<>();
      options.put(SIMILARITY_THRESHOLD, String.valueOf(similarityThreshold));
      options.put(MAX_PAIRS_PER_CELL, String.valueOf(maxPairsPerCell));
      options.put(MAX_CELL_BUFFER, String.valueOf(maxCellBuffer));
      copy.init(source.deepCopy(env), options, env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy LatentEdgeDiscoveryIterator", e);
    }
    return copy;
  }
}
