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

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pure math utility for spherical tessellation-based locality-sensitive hashing (LSH). Maps
 * high-dimensional vectors to 64-bit cell IDs such that nearby vectors on the unit sphere receive
 * nearby cell IDs, enabling efficient range scans over Accumulo's sorted key space.
 *
 * <p>
 * For high-dimensional embedding vectors, geometric icosahedral tessellation is impractical.
 * Instead, this class uses an LSH scheme inspired by spherical tessellation:
 * <ol>
 * <li>Generate a fixed set of random hyperplanes (seeded for reproducibility)</li>
 * <li>For each vector, compute the sign of the dot product with each hyperplane to produce a bit
 * string</li>
 * <li>Interleave the bits to produce a cell ID with spatial locality (nearby vectors on the sphere
 * tend to share bit prefixes)</li>
 * </ol>
 *
 * <p>
 * The {@code depth} parameter controls the number of hyperplanes: {@code 3 * depth} hyperplanes
 * yield {@code 2^(3*depth)} possible cells. The default depth of 6 produces 18 hyperplanes and
 * 262,144 cells.
 *
 * <p>
 * All methods are static and thread-safe. Hyperplane sets are lazily generated and cached per
 * (dimension, depth) pair using a {@link ConcurrentHashMap}.
 */
public final class SphericalTessellation {

  /** Default tessellation depth. Produces 3*6 = 18 hyperplanes and 2^18 = 262,144 cells. */
  public static final int DEFAULT_DEPTH = 6;

  /** Fixed random seed for reproducible hyperplane generation across all nodes. */
  private static final long HYPERPLANE_SEED = 42L;

  /**
   * Cache of generated hyperplanes keyed by "dimension:depth". Each value is a float[numPlanes][]
   * array of unit-length hyperplane normal vectors.
   */
  private static final ConcurrentHashMap<String,float[][]> HYPERPLANE_CACHE =
      new ConcurrentHashMap<>();

  private SphericalTessellation() {}

  /**
   * Assigns a cell ID to a vector using the default depth.
   *
   * @param vector the input vector (any dimensionality)
   * @return a 64-bit cell ID with spatial locality
   */
  public static long assignCellId(float[] vector) {
    return assignCellId(vector, DEFAULT_DEPTH);
  }

  /**
   * Assigns a cell ID to a vector at the given tessellation depth.
   *
   * <p>
   * The vector is normalized to unit length, then hashed against {@code 3 * depth} random
   * hyperplanes. The resulting bit pattern is interleaved to maximize locality: bits from groups of
   * 3 hyperplanes are interleaved so that the cell ID changes slowly for nearby vectors on the unit
   * sphere.
   *
   * @param vector the input vector (any dimensionality, must have length &gt; 0)
   * @param depth tessellation depth controlling resolution (number of hyperplanes = 3 * depth)
   * @return a 64-bit cell ID with spatial locality
   * @throws IllegalArgumentException if vector is null or empty, or depth is not positive
   */
  public static long assignCellId(float[] vector, int depth) {
    validateVector(vector);
    validateDepth(depth);

    float[] normalized = normalize(vector);
    int numPlanes = 3 * depth;
    float[][] hyperplanes = getHyperplanes(vector.length, depth);

    // Compute sign bits: one bit per hyperplane
    int[] bits = new int[numPlanes];
    for (int i = 0; i < numPlanes; i++) {
      bits[i] = dotProductSign(normalized, hyperplanes[i]);
    }

    // Interleave bits for spatial locality.
    // Group hyperplanes in triples and interleave: bit[0], bit[depth], bit[2*depth],
    // bit[1], bit[depth+1], bit[2*depth+1], ...
    // This ensures that the most significant bits of the cell ID capture coarse spatial
    // structure (one bit from each of three orthogonal-ish directions).
    long cellId = 0L;
    for (int i = 0; i < depth; i++) {
      int b0 = bits[i];
      int b1 = bits[i + depth];
      int b2 = bits[i + 2 * depth];

      int bitPos = (depth - 1 - i) * 3;
      cellId |= ((long) b0 << (bitPos + 2));
      cellId |= ((long) b1 << (bitPos + 1));
      cellId |= ((long) b2 << bitPos);
    }

    return cellId;
  }

  /**
   * Returns a range of cell IDs for a range scan that covers the neighborhood of the query vector
   * at the default depth.
   *
   * @param queryVector the query vector
   * @param searchRadius how many bit-positions of slack to allow (controls range width)
   * @return a two-element array {@code [startCellId, endCellId]} (inclusive)
   */
  public static long[] getCellRange(float[] queryVector, int searchRadius) {
    return getCellRange(queryVector, DEFAULT_DEPTH, searchRadius);
  }

  /**
   * Returns a range of cell IDs for a range scan that covers the neighborhood of the query vector.
   *
   * <p>
   * Because the cell ID is constructed via bit interleaving for spatial locality, numerically
   * adjacent cell IDs tend to correspond to spatially adjacent cells. The range is computed as
   * {@code [cellId - margin, cellId + margin]} where {@code margin = (1L << searchRadius) - 1}.
   * This is an approximation that works well in practice because the interleaved ordering preserves
   * locality.
   *
   * @param queryVector the query vector
   * @param depth tessellation depth
   * @param searchRadius controls range width; larger values include more cells (and more false
   *        positives)
   * @return a two-element array {@code [startCellId, endCellId]} (inclusive), clamped to
   *         non-negative
   * @throws IllegalArgumentException if searchRadius is negative
   */
  public static long[] getCellRange(float[] queryVector, int depth, int searchRadius) {
    validateVector(queryVector);
    validateDepth(depth);
    if (searchRadius < 0) {
      throw new IllegalArgumentException("searchRadius must be non-negative, got: " + searchRadius);
    }

    long cellId = assignCellId(queryVector, depth);
    long margin = (1L << searchRadius) - 1;

    long startCellId = Math.max(0L, cellId - margin);
    long maxCellId = (1L << (3 * depth)) - 1;
    long endCellId = Math.min(maxCellId, cellId + margin);

    return new long[] {startCellId, endCellId};
  }

  /**
   * Normalizes a vector to unit length. If the vector has zero magnitude, returns a copy of the
   * original (avoids division by zero).
   *
   * @param vector the input vector
   * @return a new array containing the unit-length vector
   */
  static float[] normalize(float[] vector) {
    float magnitude = 0.0f;
    for (float v : vector) {
      magnitude += v * v;
    }
    magnitude = (float) Math.sqrt(magnitude);

    float[] result = new float[vector.length];
    if (magnitude == 0.0f) {
      System.arraycopy(vector, 0, result, 0, vector.length);
      return result;
    }

    for (int i = 0; i < vector.length; i++) {
      result[i] = vector[i] / magnitude;
    }
    return result;
  }

  /**
   * Returns 1 if the dot product of the vector with the hyperplane normal is non-negative, 0
   * otherwise.
   *
   * @param vector the input vector (must have same dimensionality as hyperplane)
   * @param hyperplane the hyperplane normal vector
   * @return 0 or 1
   */
  static int dotProductSign(float[] vector, float[] hyperplane) {
    float dot = 0.0f;
    int len = Math.min(vector.length, hyperplane.length);
    for (int i = 0; i < len; i++) {
      dot += vector[i] * hyperplane[i];
    }
    return dot >= 0.0f ? 1 : 0;
  }

  /**
   * Gets or lazily generates the hyperplane set for the given dimension and depth. Hyperplanes are
   * random unit vectors generated from a fixed seed for reproducibility. The same (dimension,
   * depth) pair always produces the same hyperplanes, regardless of which node in the cluster
   * generates them.
   *
   * @param dimension the vector dimensionality
   * @param depth tessellation depth
   * @return array of {@code 3 * depth} unit-length hyperplane normal vectors
   */
  private static float[][] getHyperplanes(int dimension, int depth) {
    String cacheKey = dimension + ":" + depth;
    return HYPERPLANE_CACHE.computeIfAbsent(cacheKey, k -> generateHyperplanes(dimension, depth));
  }

  /**
   * Generates random unit hyperplanes from a deterministic seed. Uses the seed combined with
   * dimension and depth to ensure unique but reproducible hyperplane sets.
   */
  private static float[][] generateHyperplanes(int dimension, int depth) {
    int numPlanes = 3 * depth;
    float[][] hyperplanes = new float[numPlanes][];

    // Combine the fixed seed with dimension and depth so different configurations
    // get different (but still deterministic) hyperplanes
    Random rng = new Random(HYPERPLANE_SEED ^ ((long) dimension << 32 | depth));

    for (int p = 0; p < numPlanes; p++) {
      float[] plane = new float[dimension];
      float magnitude = 0.0f;
      for (int d = 0; d < dimension; d++) {
        // Sample from standard normal distribution via Box-Muller would be ideal,
        // but Random.nextGaussian() suffices for LSH hyperplanes
        plane[d] = (float) rng.nextGaussian();
        magnitude += plane[d] * plane[d];
      }
      // Normalize to unit length
      magnitude = (float) Math.sqrt(magnitude);
      if (magnitude > 0.0f) {
        for (int d = 0; d < dimension; d++) {
          plane[d] /= magnitude;
        }
      }
      hyperplanes[p] = plane;
    }

    return hyperplanes;
  }

  private static void validateVector(float[] vector) {
    if (vector == null || vector.length == 0) {
      throw new IllegalArgumentException("Vector must be non-null and non-empty");
    }
  }

  private static void validateDepth(int depth) {
    if (depth < 1) {
      throw new IllegalArgumentException("Depth must be positive, got: " + depth);
    }
    // Cap at 21 to avoid exceeding 63 bits (3 * 21 = 63)
    if (depth > 21) {
      throw new IllegalArgumentException(
          "Depth must be at most 21 (63 bits in a long), got: " + depth);
    }
  }
}
