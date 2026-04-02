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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Comprehensive example demonstrating production-ready vector store features including: - Metadata
 * integration for per-vector categories - Compression for storage efficiency - Batching/staging for
 * performance - Advanced indexing for scalability - Vector chunking for large embeddings
 */
@SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
    justification = "This class is an example/demo, not security-sensitive production code.")
public class ProductionVectorStoreExampleTest {

  static Random rand = new Random(1234);

  public static void main(String[] args) {
    System.out.println("=== Production Vector Store Capabilities ===\n");

    demonstrateCategoryIntegration();
    demonstrateCompression();
    demonstrateBatchingAndStaging();
    demonstrateAdvancedIndexing();
    demonstrateVectorChunking();

    System.out.println("=== Production Features Complete ===");
  }

  /**
   * Demonstrates per-vector category metadata.
   */
  public static void demonstrateCategoryIntegration() {
    System.out.println("1. CATEGORY INTEGRATION - Example Metadata");
    System.out.println("-------------------------------------------");

    // Create vectors with different category markings
    float[] publicVector = {0.1f, 0.2f, 0.3f};
    float[] internalVector = {0.8f, 0.9f, 1.0f};
    float[] restrictedVector = {0.4f, 0.5f, 0.6f};

    System.out.println("Created vectors with category tags:");
    System.out.println(String.format("  Public: %s (tag=public)", Arrays.toString(publicVector)));
    System.out
        .println(String.format("  Internal: %s (tag=internal)", Arrays.toString(internalVector)));
    System.out.println(
        String.format("  Restricted: %s (tag=restricted)", Arrays.toString(restrictedVector)));

    // Demonstrate filtering by category
    Map<String,String> iteratorOptions = new HashMap<>();
    iteratorOptions.put(VectorIterator.QUERY_VECTOR_OPTION, "0.5,0.6,0.7");
    iteratorOptions.put(VectorIterator.AUTHORIZATIONS_OPTION, "internal");
    iteratorOptions.put(VectorIterator.TOP_K_OPTION, "5");

    System.out.println("User with category filter = internal can access:");
    System.out.println("  + Public vectors (always available)");
    System.out.println("  + Internal vectors (category matches)");
    System.out.println("  - Restricted vectors (not included in filter)");

    System.out.println();
  }

  /**
   * Demonstrates vector compression for storage efficiency.
   */
  public static void demonstrateCompression() {
    System.out.println("2. COMPRESSION - High Impact on Storage Efficiency");
    System.out.println("--------------------------------------------------");

    float[] embedding = new float[128];
    for (int i = 0; i < embedding.length; i++) {
      embedding[i] = (float) (Math.sin(i * 0.01) * Math.cos(i * 0.02));
    }

    Value uncompressed = Value.newVector(embedding);
    Value compressed8bit =
        Value.newCompressedVector(embedding, VectorCompression.COMPRESSION_QUANTIZED_8BIT);
    Value compressed16bit =
        Value.newCompressedVector(embedding, VectorCompression.COMPRESSION_QUANTIZED_16BIT);

    System.out.println("Original 128-d vector:");
    System.out.println("  Uncompressed: " + uncompressed.getSize() + " bytes");
    System.out.println("  8-bit quantized: " + compressed8bit.getSize() + " bytes");
    System.out.println("  16-bit quantized: " + compressed16bit.getSize() + " bytes");

    float[] d8 = compressed8bit.asCompressedVector();
    float[] d16 = compressed16bit.asCompressedVector();

    double error8 = calculateMeanSquaredError(embedding, d8);
    double error16 = calculateMeanSquaredError(embedding, d16);

    System.out.println("Reconstruction accuracy:");
    System.out.println("  8-bit MSE: " + error8);
    System.out.println("  16-bit MSE: " + error16);

    System.out.println();
  }

  /**
   * Demonstrates batching and staging for performance improvement.
   */
  public static void demonstrateBatchingAndStaging() {
    System.out.println("3. BATCHING/STAGING - Significant Performance Improvement");
    System.out.println("---------------------------------------------------------");

    VectorBuffer buffer = new VectorBuffer(256, 4);

    List<VectorBuffer.VectorBlock.VectorEntry> block1Vectors =
        createSampleVectorBlock("block1", 50);
    VectorIndex.VectorBlockMetadata metadata1 =
        new VectorIndex.VectorBlockMetadata(computeCentroid(block1Vectors), 50, 0L, 2000);
    buffer.loadBlock(0L, metadata1, block1Vectors);

    float[] queryVector = {0.3f, 0.4f, 0.5f};
    List<VectorIterator.SimilarityResult> results =
        buffer.computeSimilarities(queryVector, VectorIterator.SimilarityType.COSINE, 10, 0.5f);

    System.out.println("Parallel similarity search results: " + results.size());
    buffer.shutdown();
    System.out.println();
  }

  /**
   * Demonstrates advanced indexing.
   */
  public static void demonstrateAdvancedIndexing() {
    System.out.println("4. ADVANCED INDEXING - For Large-Scale Deployments");
    System.out.println("---------------------------------------------------");

    List<float[]> blockCentroids = Arrays.asList(new float[] {1.0f, 0.0f, 0.0f},
        new float[] {0.0f, 1.0f, 0.0f}, new float[] {0.0f, 0.0f, 1.0f});

    VectorIndexFooter hierarchicalIndex =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.HIERARCHICAL);
    hierarchicalIndex.buildHierarchicalIndex(blockCentroids, 2);

    VectorIndexFooter ivfIndex = new VectorIndexFooter(3, VectorIndexFooter.IndexingType.IVF);
    ivfIndex.buildIVFIndex(blockCentroids, 2);

    float[] queryVector = {0.8f, 0.2f, 0.0f};
    List<Integer> candidates = hierarchicalIndex.findCandidateBlocks(queryVector, 2);

    System.out.println("Candidate blocks: " + candidates);
    System.out.println();
  }

  /**
   * Demonstrates vector chunking for very large embeddings.
   */
  public static void demonstrateVectorChunking() {
    System.out.println("5. VECTOR CHUNKING - For Very Large Embeddings");
    System.out.println("-----------------------------------------------");

    float[] largeEmbedding = new float[1024];
    for (int i = 0; i < largeEmbedding.length; i++) {
      largeEmbedding[i] = (float) (rand.nextFloat() * 2.0 - 1.0);
    }

    int chunkSize = 256;
    Value[] chunks = Value.chunkVector(largeEmbedding, chunkSize);

    System.out.println("Chunked into " + chunks.length + " pieces");
    float[] reassembled = Value.reassembleVector(chunks);
    System.out.println("Reassembled size: " + reassembled.length);
    System.out.println();
  }

  // ==== Helpers ====

  private static List<VectorBuffer.VectorBlock.VectorEntry> createSampleVectorBlock(String prefix,
      int count) {
    List<VectorBuffer.VectorBlock.VectorEntry> entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Key key = new Key(prefix + "_" + i, "embedding", "vector", System.currentTimeMillis());
      float[] vector = {rand.nextFloat(), rand.nextFloat(), rand.nextFloat()};
      byte[] category = "public".getBytes();
      entries.add(new VectorBuffer.VectorBlock.VectorEntry(key, vector, category));
    }
    return entries;
  }

  private static float[] computeCentroid(List<VectorBuffer.VectorBlock.VectorEntry> vectors) {
    int dimension = vectors.get(0).getVector().length;
    float[] centroid = new float[dimension];
    for (VectorBuffer.VectorBlock.VectorEntry entry : vectors) {
      for (int i = 0; i < dimension; i++) {
        centroid[i] += entry.getVector()[i];
      }
    }
    for (int i = 0; i < dimension; i++) {
      centroid[i] /= vectors.size();
    }
    return centroid;
  }

  private static double calculateMeanSquaredError(float[] a, float[] b) {
    double sum = 0.0;
    for (int i = 0; i < a.length; i++) {
      double d = a[i] - b[i];
      sum += d * d;
    }
    return sum / a.length;
  }

}
