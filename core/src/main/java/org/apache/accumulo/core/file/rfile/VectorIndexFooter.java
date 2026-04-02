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
package org.apache.accumulo.core.file.rfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Advanced indexing structure stored in RFile footer for hierarchical vector search. Supports
 * multi-level centroids and cluster assignments for efficient block filtering.
 */
public class VectorIndexFooter implements Writable {

  private int vectorDimension;
  private float[][] globalCentroids; // Top-level cluster centers
  private int[][] clusterAssignments; // Block to cluster mappings
  private byte[] quantizationCodebook; // For product quantization
  private IndexingType indexingType;

  // Tessellation-specific: maps block index → [minCellId, maxCellId] range
  private long[][] tessellationCellRanges;
  private int tessellationDepth;

  public enum IndexingType {
    FLAT((byte) 0), // Simple centroid-based
    IVF((byte) 1), // Inverted File Index
    HIERARCHICAL((byte) 2), // Multi-level centroids
    PQ((byte) 3), // Product Quantization
    TESSELLATION((byte) 4); // Spherical tessellation (LSH) — data-independent, no drift

    private final byte typeId;

    IndexingType(byte typeId) {
      this.typeId = typeId;
    }

    public byte getTypeId() {
      return typeId;
    }

    public static IndexingType fromTypeId(byte typeId) {
      for (IndexingType type : values()) {
        if (type.typeId == typeId) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unknown IndexingType id: " + typeId);
    }
  }

  public VectorIndexFooter() {
    this.globalCentroids = new float[0][];
    this.clusterAssignments = new int[0][];
    this.quantizationCodebook = new byte[0];
    this.indexingType = IndexingType.FLAT;
    this.tessellationCellRanges = new long[0][];
    this.tessellationDepth = org.apache.accumulo.core.graph.SphericalTessellation.DEFAULT_DEPTH;
  }

  public VectorIndexFooter(int vectorDimension, IndexingType indexingType) {
    this.vectorDimension = vectorDimension;
    this.indexingType = indexingType;
    this.globalCentroids = new float[0][];
    this.clusterAssignments = new int[0][];
    this.quantizationCodebook = new byte[0];
    this.tessellationCellRanges = new long[0][];
    this.tessellationDepth = org.apache.accumulo.core.graph.SphericalTessellation.DEFAULT_DEPTH;
  }

  /**
   * Builds a hierarchical index from vector block centroids using K-means clustering.
   *
   * @param blockCentroids centroids from all vector blocks
   * @param clustersPerLevel number of clusters per hierarchical level
   */
  public void buildHierarchicalIndex(List<float[]> blockCentroids, int clustersPerLevel) {
    if (blockCentroids.isEmpty()) {
      return;
    }

    this.indexingType = IndexingType.HIERARCHICAL;

    // Build top-level clusters using K-means
    this.globalCentroids = performKMeansClustering(blockCentroids, clustersPerLevel);

    // Assign each block to nearest top-level cluster
    this.clusterAssignments = new int[blockCentroids.size()][];
    for (int blockIdx = 0; blockIdx < blockCentroids.size(); blockIdx++) {
      float[] blockCentroid = blockCentroids.get(blockIdx);
      int nearestCluster = findNearestCluster(blockCentroid, globalCentroids);
      this.clusterAssignments[blockIdx] = new int[] {nearestCluster};
    }
  }

  /**
   * Builds an Inverted File Index (IVF) for approximate nearest neighbor search.
   *
   * @param blockCentroids centroids from all vector blocks
   * @param numClusters number of IVF clusters to create
   */
  public void buildIVFIndex(List<float[]> blockCentroids, int numClusters) {
    if (blockCentroids.isEmpty()) {
      return;
    }

    this.indexingType = IndexingType.IVF;

    // Create IVF clusters
    this.globalCentroids = performKMeansClustering(blockCentroids, numClusters);

    // Build inverted file structure - each block maps to multiple clusters
    this.clusterAssignments = new int[blockCentroids.size()][];
    for (int blockIdx = 0; blockIdx < blockCentroids.size(); blockIdx++) {
      float[] blockCentroid = blockCentroids.get(blockIdx);
      // Find top-3 nearest clusters for better recall
      int[] nearestClusters = findTopKNearestClusters(blockCentroid, globalCentroids, 3);
      this.clusterAssignments[blockIdx] = nearestClusters;
    }
  }

  /**
   * Builds a tessellation index from vector block centroids. Unlike IVF/hierarchical approaches,
   * tessellation uses fixed random hyperplanes (LSH) to assign cell IDs. This means the index never
   * goes stale as data distribution changes — no retraining needed.
   *
   * <p>
   * Each block is assigned a cell ID range [min, max] based on the cell IDs of all vectors it
   * contains. At query time, the query vector's cell ID is computed and only blocks whose range
   * overlaps are scanned.
   *
   * @param blockVectors list of vector lists, one per block — all vectors in each block
   * @param depth tessellation depth (default 6 = 262,144 cells)
   */
  public void buildTessellationIndex(List<List<float[]>> blockVectors, int depth) {
    if (blockVectors.isEmpty()) {
      return;
    }

    this.indexingType = IndexingType.TESSELLATION;
    this.tessellationDepth = depth;
    this.tessellationCellRanges = new long[blockVectors.size()][];

    for (int blockIdx = 0; blockIdx < blockVectors.size(); blockIdx++) {
      List<float[]> vectors = blockVectors.get(blockIdx);
      if (vectors.isEmpty()) {
        tessellationCellRanges[blockIdx] = new long[] {Long.MAX_VALUE, Long.MIN_VALUE};
        continue;
      }

      long minCell = Long.MAX_VALUE;
      long maxCell = Long.MIN_VALUE;
      for (float[] vec : vectors) {
        long cellId = org.apache.accumulo.core.graph.SphericalTessellation.assignCellId(vec, depth);
        minCell = Math.min(minCell, cellId);
        maxCell = Math.max(maxCell, cellId);
      }
      tessellationCellRanges[blockIdx] = new long[] {minCell, maxCell};
    }

    // Also store centroids for fallback similarity computation
    this.globalCentroids = new float[blockVectors.size()][];
    for (int i = 0; i < blockVectors.size(); i++) {
      this.globalCentroids[i] = calculateCentroid(blockVectors.get(i));
    }
    this.clusterAssignments = new int[0][];
  }

  /**
   * Simplified tessellation index builder using block centroids only (when individual vectors are
   * not available). Each block gets a single cell ID from its centroid.
   */
  public void buildTessellationIndex(List<float[]> blockCentroids) {
    buildTessellationIndexFromCentroids(blockCentroids,
        org.apache.accumulo.core.graph.SphericalTessellation.DEFAULT_DEPTH);
  }

  private void buildTessellationIndexFromCentroids(List<float[]> blockCentroids, int depth) {
    if (blockCentroids.isEmpty()) {
      return;
    }

    this.indexingType = IndexingType.TESSELLATION;
    this.tessellationDepth = depth;
    this.tessellationCellRanges = new long[blockCentroids.size()][];

    for (int i = 0; i < blockCentroids.size(); i++) {
      long cellId = org.apache.accumulo.core.graph.SphericalTessellation
          .assignCellId(blockCentroids.get(i), depth);
      // Single centroid per block — range is just [cellId, cellId]
      tessellationCellRanges[i] = new long[] {cellId, cellId};
    }

    this.globalCentroids = blockCentroids.toArray(new float[0][]);
    this.clusterAssignments = new int[0][];
  }

  public int getTessellationDepth() {
    return tessellationDepth;
  }

  public long[][] getTessellationCellRanges() {
    return tessellationCellRanges;
  }

  private static float[] calculateCentroid(List<float[]> vectors) {
    if (vectors.isEmpty()) {
      return new float[0];
    }
    int dim = vectors.get(0).length;
    float[] centroid = new float[dim];
    for (float[] vec : vectors) {
      for (int d = 0; d < dim; d++) {
        centroid[d] += vec[d];
      }
    }
    for (int d = 0; d < dim; d++) {
      centroid[d] /= vectors.size();
    }
    return centroid;
  }

  /**
   * Finds candidate blocks for a query vector using the index structure.
   *
   * @param queryVector the query vector
   * @param maxCandidateBlocks maximum number of candidate blocks to return
   * @return list of candidate block indices
   */
  public List<Integer> findCandidateBlocks(float[] queryVector, int maxCandidateBlocks) {
    List<Integer> candidates = new ArrayList<>();

    switch (indexingType) {
      case TESSELLATION:
        candidates = findCandidatesTessellation(queryVector, maxCandidateBlocks);
        break;
      case HIERARCHICAL:
        candidates = findCandidatesHierarchical(queryVector, maxCandidateBlocks);
        break;
      case IVF:
        candidates = findCandidatesIVF(queryVector, maxCandidateBlocks);
        break;
      case FLAT:
      default:
        // For flat indexing, return all blocks (no filtering)
        int totalBlocks = tessellationCellRanges != null ? tessellationCellRanges.length
            : clusterAssignments.length;
        for (int i = 0; i < totalBlocks; i++) {
          candidates.add(i);
        }
        break;
    }

    return candidates.subList(0, Math.min(candidates.size(), maxCandidateBlocks));
  }

  private List<Integer> findCandidatesHierarchical(float[] queryVector, int maxCandidates) {
    List<Integer> candidates = new ArrayList<>();

    if (globalCentroids.length == 0) {
      return candidates;
    }

    // Find nearest top-level clusters
    int[] nearestClusters =
        findTopKNearestClusters(queryVector, globalCentroids, Math.min(3, globalCentroids.length));

    // Collect all blocks assigned to these clusters
    for (int blockIdx = 0; blockIdx < clusterAssignments.length; blockIdx++) {
      if (clusterAssignments[blockIdx].length > 0) {
        int blockCluster = clusterAssignments[blockIdx][0];
        for (int nearestCluster : nearestClusters) {
          if (blockCluster == nearestCluster) {
            candidates.add(blockIdx);
            break;
          }
        }
      }
    }

    return candidates;
  }

  private List<Integer> findCandidatesIVF(float[] queryVector, int maxCandidates) {
    List<Integer> candidates = new ArrayList<>();

    if (globalCentroids.length == 0) {
      return candidates;
    }

    // Find nearest IVF clusters
    int[] nearestClusters =
        findTopKNearestClusters(queryVector, globalCentroids, Math.min(5, globalCentroids.length));

    // Use inverted file to find candidate blocks
    for (int blockIdx = 0; blockIdx < clusterAssignments.length; blockIdx++) {
      for (int blockCluster : clusterAssignments[blockIdx]) {
        for (int nearestCluster : nearestClusters) {
          if (blockCluster == nearestCluster) {
            candidates.add(blockIdx);
            break;
          }
        }
      }
    }

    return candidates;
  }

  /**
   * Finds candidate blocks using tessellation cell ID ranges. Computes the query vector's cell ID
   * and returns all blocks whose cell range overlaps. This is O(blocks) but with very fast constant
   * — just two long comparisons per block. No clustering, no centroids, no data drift.
   */
  private List<Integer> findCandidatesTessellation(float[] queryVector, int maxCandidates) {
    List<Integer> candidates = new ArrayList<>();

    if (tessellationCellRanges == null || tessellationCellRanges.length == 0) {
      return candidates;
    }

    long[] queryRange = org.apache.accumulo.core.graph.SphericalTessellation
        .getCellRange(queryVector, tessellationDepth, 2);
    long queryMin = queryRange[0];
    long queryMax = queryRange[1];

    for (int blockIdx = 0; blockIdx < tessellationCellRanges.length; blockIdx++) {
      long blockMin = tessellationCellRanges[blockIdx][0];
      long blockMax = tessellationCellRanges[blockIdx][1];

      // Check if block's cell range overlaps with query's cell range
      if (blockMax >= queryMin && blockMin <= queryMax) {
        candidates.add(blockIdx);
      }
    }

    return candidates;
  }

  private static final int MAX_KMEANS_ITERATIONS = 50;
  private static final float CONVERGENCE_THRESHOLD = 1e-6f;

  private float[][] performKMeansClustering(List<float[]> points, int k) {
    if (points.isEmpty() || k <= 0) {
      return new float[0][];
    }

    k = Math.min(k, points.size());
    int dimension = points.get(0).length;

    for (float[] point : points) {
      if (point.length != dimension) {
        throw new IllegalArgumentException("All points must have the same dimension: expected "
            + dimension + ", got " + point.length);
      }
    }

    // Initialize centroids using evenly-spaced points
    float[][] centroids = new float[k][dimension];
    for (int i = 0; i < k; i++) {
      int pointIndex = Math.min((i * points.size()) / k, points.size() - 1);
      System.arraycopy(points.get(pointIndex), 0, centroids[i], 0, dimension);
    }

    int[] assignments = new int[points.size()];

    for (int iteration = 0; iteration < MAX_KMEANS_ITERATIONS; iteration++) {
      // Assign points to nearest centroids
      boolean changed = false;
      for (int pointIdx = 0; pointIdx < points.size(); pointIdx++) {
        int nearest = findNearestCluster(points.get(pointIdx), centroids);
        if (nearest != assignments[pointIdx]) {
          assignments[pointIdx] = nearest;
          changed = true;
        }
      }

      // Update centroids
      float maxDrift = 0.0f;
      for (int clusterIdx = 0; clusterIdx < k; clusterIdx++) {
        float[] newCentroid = new float[dimension];
        int count = 0;

        for (int pointIdx = 0; pointIdx < points.size(); pointIdx++) {
          if (assignments[pointIdx] == clusterIdx) {
            float[] point = points.get(pointIdx);
            for (int d = 0; d < dimension; d++) {
              newCentroid[d] += point[d];
            }
            count++;
          }
        }

        if (count > 0) {
          for (int d = 0; d < dimension; d++) {
            newCentroid[d] /= count;
          }
          float drift = euclideanDistance(centroids[clusterIdx], newCentroid);
          maxDrift = Math.max(maxDrift, drift);
          centroids[clusterIdx] = newCentroid;
        }
      }

      // Converged if no assignments changed or centroids barely moved
      if (!changed || maxDrift < CONVERGENCE_THRESHOLD) {
        break;
      }
    }

    return centroids;
  }

  private int findNearestCluster(float[] point, float[][] centroids) {
    int nearest = 0;
    float minDistance = Float.MAX_VALUE;

    for (int i = 0; i < centroids.length; i++) {
      float distance = euclideanDistance(point, centroids[i]);
      if (distance < minDistance) {
        minDistance = distance;
        nearest = i;
      }
    }

    return nearest;
  }

  private int[] findTopKNearestClusters(float[] point, float[][] centroids, int k) {
    k = Math.min(k, centroids.length);
    float[] distances = new float[centroids.length];

    for (int i = 0; i < centroids.length; i++) {
      distances[i] = euclideanDistance(point, centroids[i]);
    }

    // Find indices of k smallest distances
    Integer[] indices = new Integer[centroids.length];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = i;
    }

    Arrays.sort(indices, (a, b) -> Float.compare(distances[a], distances[b]));

    int[] result = new int[k];
    for (int i = 0; i < k; i++) {
      result[i] = indices[i];
    }

    return result;
  }

  private float euclideanDistance(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimensions must match: " + a.length + " != " + b.length);
    }
    float sum = 0.0f;
    for (int i = 0; i < a.length; i++) {
      float diff = a[i] - b[i];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  // Getters and setters
  public int getVectorDimension() {
    return vectorDimension;
  }

  public float[][] getGlobalCentroids() {
    return globalCentroids;
  }

  public int[][] getClusterAssignments() {
    return clusterAssignments;
  }

  public byte[] getQuantizationCodebook() {
    return quantizationCodebook;
  }

  public IndexingType getIndexingType() {
    return indexingType;
  }

  public void setGlobalCentroids(float[][] globalCentroids) {
    this.globalCentroids = globalCentroids;
  }

  public void setClusterAssignments(int[][] clusterAssignments) {
    this.clusterAssignments = clusterAssignments;
  }

  public void setQuantizationCodebook(byte[] quantizationCodebook) {
    this.quantizationCodebook = quantizationCodebook;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(vectorDimension);
    out.writeByte(indexingType.getTypeId());

    // Write global centroids
    out.writeInt(globalCentroids.length);
    for (float[] centroid : globalCentroids) {
      out.writeInt(centroid.length);
      for (float value : centroid) {
        out.writeFloat(value);
      }
    }

    // Write cluster assignments
    out.writeInt(clusterAssignments.length);
    for (int[] assignment : clusterAssignments) {
      out.writeInt(assignment.length);
      for (int cluster : assignment) {
        out.writeInt(cluster);
      }
    }

    // Write quantization codebook
    out.writeInt(quantizationCodebook.length);
    if (quantizationCodebook.length > 0) {
      out.write(quantizationCodebook);
    }

    // Write tessellation data (for TESSELLATION index type)
    if (tessellationCellRanges != null && tessellationCellRanges.length > 0) {
      out.writeInt(tessellationDepth);
      out.writeInt(tessellationCellRanges.length);
      for (long[] range : tessellationCellRanges) {
        out.writeLong(range[0]);
        out.writeLong(range[1]);
      }
    } else {
      out.writeInt(0); // depth=0 signals no tessellation data
      out.writeInt(0);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vectorDimension = in.readInt();
    indexingType = IndexingType.fromTypeId(in.readByte());

    // Read global centroids
    int numCentroids = in.readInt();
    globalCentroids = new float[numCentroids][];
    for (int i = 0; i < numCentroids; i++) {
      int centroidLength = in.readInt();
      globalCentroids[i] = new float[centroidLength];
      for (int j = 0; j < centroidLength; j++) {
        globalCentroids[i][j] = in.readFloat();
      }
    }

    // Read cluster assignments
    int numAssignments = in.readInt();
    clusterAssignments = new int[numAssignments][];
    for (int i = 0; i < numAssignments; i++) {
      int assignmentLength = in.readInt();
      clusterAssignments[i] = new int[assignmentLength];
      for (int j = 0; j < assignmentLength; j++) {
        clusterAssignments[i][j] = in.readInt();
      }
    }

    // Read quantization codebook
    int codebookLength = in.readInt();
    quantizationCodebook = new byte[codebookLength];
    if (codebookLength > 0) {
      in.readFully(quantizationCodebook);
    }

    // Read tessellation data
    try {
      tessellationDepth = in.readInt();
      int numRanges = in.readInt();
      tessellationCellRanges = new long[numRanges][];
      for (int i = 0; i < numRanges; i++) {
        tessellationCellRanges[i] = new long[] {in.readLong(), in.readLong()};
      }
    } catch (java.io.EOFException e) {
      // Older RFiles without tessellation data — backwards compatible
      tessellationDepth = org.apache.accumulo.core.graph.SphericalTessellation.DEFAULT_DEPTH;
      tessellationCellRanges = new long[0][];
    }
  }
}
