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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Major compaction iterator that computes spherical tessellation cell IDs for vertices with
 * embeddings and emits a {@code _cell_id} property for each. Implements
 * {@link SortedKeyValueIterator SortedKeyValueIterator&lt;Key,Value&gt;} directly (same pattern as
 * {@link GraphTraversalIterator}).
 *
 * <p>
 * During major compaction, this iterator scans all entries. For each vertex row that contains an
 * {@code _embedding} column qualifier (ColFam="V", ColQual="_embedding"), it:
 * <ol>
 * <li>Extracts the vector via {@link Value#asVector()}</li>
 * <li>Computes the cell ID via {@link SphericalTessellation#assignCellId(float[], int)}</li>
 * <li>Emits an additional entry: Key(row, "V", "_cell_id", sourceVisibility) with the cell ID as
 * string value</li>
 * </ol>
 *
 * <p>
 * All original entries are passed through unmodified. The cell ID entry inherits the visibility of
 * the embedding entry. Malformed embedding values are skipped gracefully.
 *
 * <p>
 * Supported options:
 * <ul>
 * <li>{@code depth} — tessellation depth (default 6, producing 262,144 cells)</li>
 * </ul>
 */
public class SphericalTessellationIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String DEPTH = "depth";

  private SortedKeyValueIterator<Key,Value> source;
  private IteratorEnvironment env;

  private int depth = SphericalTessellation.DEFAULT_DEPTH;

  private List<Map.Entry<Key,Value>> results;
  private int currentIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.env = env;
    this.results = new ArrayList<>();
    this.currentIndex = 0;

    if (options.containsKey(DEPTH)) {
      depth = Integer.parseInt(options.get(DEPTH));
    }
  }

  @Override
  public boolean hasTop() {
    return currentIndex < results.size();
  }

  @Override
  public void next() throws IOException {
    currentIndex++;
  }

  @Override
  public Key getTopKey() {
    if (!hasTop()) {
      return null;
    }
    return results.get(currentIndex).getKey();
  }

  @Override
  public Value getTopValue() {
    if (!hasTop()) {
      return null;
    }
    return results.get(currentIndex).getValue();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    results.clear();
    currentIndex = 0;

    source.seek(range, columnFamilies, inclusive);

    // Row-aware buffering: collect all entries per row to detect embeddings
    List<Map.Entry<Key,Value>> rowBuffer = new ArrayList<>();
    String currentRow = null;

    while (source.hasTop()) {
      Key key = new Key(source.getTopKey());
      // Preserve the original Value reference for embedding entries so asVector() works
      Value value = source.getTopValue();
      String row = key.getRow().toString();

      if (currentRow != null && !row.equals(currentRow)) {
        // Process the completed row
        processRow(rowBuffer, results);
        rowBuffer.clear();
      }

      currentRow = row;
      rowBuffer.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
      source.next();
    }

    // Process the last row
    if (!rowBuffer.isEmpty()) {
      processRow(rowBuffer, results);
    }

    // Sort all results by Key for correct iterator ordering
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  /**
   * Processes a single row (vertex). Passes through all original entries and, if an embedding is
   * found, emits an additional {@code _cell_id} entry.
   */
  private void processRow(List<Map.Entry<Key,Value>> rowEntries,
      List<Map.Entry<Key,Value>> output) {
    // Track embedding entries found in this row
    Map.Entry<Key,Value> embeddingEntry = null;
    boolean hasCellId = false;

    for (Map.Entry<Key,Value> entry : rowEntries) {
      // Always pass through the original entry
      output.add(entry);

      Key key = entry.getKey();
      String colFam = key.getColumnFamily().toString();
      String colQual = key.getColumnQualifier().toString();

      if (colFam.equals(GraphSchema.VERTEX_COLFAM)) {
        if (colQual.equals(GraphSchema.EMBEDDING_COLQUAL)) {
          embeddingEntry = entry;
        } else if (colQual.equals(GraphSchema.CELL_ID_COLQUAL)) {
          hasCellId = true;
        }
      }
    }

    // If we found an embedding, compute and emit a cell ID
    if (embeddingEntry != null) {
      try {
        Value embeddingValue = embeddingEntry.getValue();
        float[] vector = embeddingValue.asVector();

        long cellId = SphericalTessellation.assignCellId(vector, depth);

        // Inherit the visibility from the embedding entry
        Key embeddingKey = embeddingEntry.getKey();
        String visStr = embeddingKey.getColumnVisibility().toString();

        Key cellIdKey = new Key(embeddingKey.getRow().toString(), GraphSchema.VERTEX_COLFAM,
            GraphSchema.CELL_ID_COLQUAL, visStr);
        Value cellIdValue = new Value(Long.toString(cellId).getBytes(UTF_8));

        // If the row already had a _cell_id, our new one will replace it after sorting
        // (same key = last writer wins during compaction). If not, this is a fresh addition.
        if (!hasCellId) {
          output.add(new AbstractMap.SimpleImmutableEntry<>(cellIdKey, cellIdValue));
        } else {
          // Replace the existing cell ID: find it in output and update
          for (int i = 0; i < output.size(); i++) {
            Key existingKey = output.get(i).getKey();
            if (existingKey.getRow().equals(embeddingKey.getRow())
                && existingKey.getColumnFamily().toString().equals(GraphSchema.VERTEX_COLFAM)
                && existingKey.getColumnQualifier().toString()
                    .equals(GraphSchema.CELL_ID_COLQUAL)) {
              output.set(i, new AbstractMap.SimpleImmutableEntry<>(cellIdKey, cellIdValue));
              break;
            }
          }
        }
      } catch (IllegalStateException | IllegalArgumentException e) {
        // Malformed embedding value — skip cell ID computation silently.
        // The original entries (including the malformed embedding) are still passed through.
      }
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    SphericalTessellationIterator copy = new SphericalTessellationIterator();
    try {
      copy.init(source.deepCopy(env), getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deep copy SphericalTessellationIterator", e);
    }
    return copy;
  }

  private Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    options.put(DEPTH, String.valueOf(depth));
    return options;
  }
}
