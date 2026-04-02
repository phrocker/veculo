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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Abstract base class for minor/major compaction iterators that detect conditions in the data and
 * emit lightweight pending marker cells. A separate worker service polls for these markers and
 * processes them asynchronously (e.g., calling LLM/embedding APIs).
 *
 * <p>
 * On {@link #seek}, scans through all source entries, buffers them along with any generated marker
 * cells, sorts the combined list by Key, and serves them through the standard iterator interface.
 *
 * <p>
 * Subclasses implement:
 * <ul>
 * <li>{@link #shouldMark(Key, Value)} — returns true if a marker should be emitted for this
 * entry</li>
 * <li>{@link #createMarker(Key, Value)} — creates the marker key-value pair. The marker value
 * should include the serialized source visibility for worker propagation.</li>
 * </ul>
 */
public abstract class PendingMarkerIterator implements SortedKeyValueIterator<Key,Value> {

  private SortedKeyValueIterator<Key,Value> source;
  private IteratorEnvironment env;

  private List<Map.Entry<Key,Value>> results;
  private int currentIndex;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.env = env;
    this.results = new ArrayList<>();
    this.currentIndex = 0;
    initOptions(options);
  }

  /**
   * Called during init to allow subclasses to parse their specific options. Default implementation
   * does nothing.
   */
  protected void initOptions(Map<String,String> options) {
    // subclasses override as needed
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

    while (source.hasTop()) {
      Key key = new Key(source.getTopKey());
      Value value = new Value(source.getTopValue());

      // Always pass through the original entry
      results.add(new AbstractMap.SimpleImmutableEntry<>(key, value));

      // Check if this entry should generate a marker
      if (shouldMark(key, value)) {
        Map.Entry<Key,Value> marker = createMarker(key, value);
        if (marker != null) {
          results.add(marker);
        }
      }

      source.next();
    }

    // Sort by Key so the iterator serves entries in correct sorted order
    results.sort(Comparator.comparing(Map.Entry::getKey));
  }

  /**
   * Returns true if the given key-value pair should trigger a pending marker emission.
   *
   * @param key the current entry's key
   * @param value the current entry's value
   * @return true to emit a marker for this entry
   */
  protected abstract boolean shouldMark(Key key, Value value);

  /**
   * Creates a pending marker key-value pair for the given source entry. The marker's column family
   * should start with {@link GraphSchema#PENDING_COLFAM_PREFIX} and its value should include the
   * serialized source column visibility so the worker can propagate ABAC.
   *
   * @param key the source entry's key
   * @param value the source entry's value
   * @return a marker key-value pair, or null to skip
   */
  protected abstract Map.Entry<Key,Value> createMarker(Key key, Value value);

  protected SortedKeyValueIterator<Key,Value> getSource() {
    return source;
  }

  protected IteratorEnvironment getEnvironment() {
    return env;
  }

  /**
   * Allows subclasses that override {@link #seek} to replace the results buffer and reset the
   * index. This supports row-aware buffering patterns where the subclass needs to collect entries
   * across an entire row before deciding whether to emit markers.
   */
  protected void setResults(List<Map.Entry<Key,Value>> newResults) {
    this.results = newResults;
    this.currentIndex = 0;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    try {
      PendingMarkerIterator copy = this.getClass().getDeclaredConstructor().newInstance();
      copy.init(source.deepCopy(env), getOptionsForCopy(), env);
      return copy;
    } catch (Exception e) {
      throw new RuntimeException("Failed to deep copy " + this.getClass().getSimpleName(), e);
    }
  }

  /**
   * Returns the options map for use in deep copy. Subclasses should override to include their
   * specific options.
   */
  protected Map<String,String> getOptionsForCopy() {
    return Map.of();
  }
}
