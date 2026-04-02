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
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;

/**
 * Example demonstrating how to use the vector store functionality. This class shows the complete
 * workflow from creating vector values to writing them with RFile.Writer and performing similarity
 * searches.
 */
public class VectorStoreExample {

  /**
   * Demonstrates creating vector values and using vector operations.
   */
  public static void demonstrateVectorValues() {
    System.out.println("=== Vector Value Operations ===");

    // Create a vector value
    float[] embedding = {0.1f, 0.2f, -0.5f, 1.0f, 0.8f};
    Value vectorValue = Value.newVector(embedding);

    System.out.println("Created vector value:");
    System.out.println("Type: " + vectorValue.getValueType());
    System.out.println("Size: " + vectorValue.getSize() + " bytes");
    System.out.println("Vector: " + Arrays.toString(vectorValue.asVector()));

    // Demonstrate type checking
    Value textValue = new Value("hello world".getBytes());
    System.out.println("\nRegular value type: " + textValue.getValueType());

    System.out.println();
  }

  /**
   * Demonstrates vector index operations.
   */
  public static void demonstrateVectorIndex() {
    System.out.println("=== Vector Index Operations ===");

    VectorIndex index = new VectorIndex(3); // 3-dimensional vectors
    System.out.println("Created vector index for dimension: " + index.getVectorDimension());

    // Add some block metadata
    float[] centroid1 = {1.0f, 0.0f, 0.0f};
    float[] centroid2 = {0.0f, 1.0f, 0.0f};
    float[] centroid3 = {0.0f, 0.0f, 1.0f};

    VectorIndex.VectorBlockMetadata block1 =
        new VectorIndex.VectorBlockMetadata(centroid1, 100, 0L, 1024);
    VectorIndex.VectorBlockMetadata block2 =
        new VectorIndex.VectorBlockMetadata(centroid2, 150, 1024L, 1536);
    VectorIndex.VectorBlockMetadata block3 =
        new VectorIndex.VectorBlockMetadata(centroid3, 75, 2560L, 768);

    index.addBlock(block1);
    index.addBlock(block2);
    index.addBlock(block3);

    System.out.println("Added " + index.getBlocks().size() + " blocks to index");
    for (int i = 0; i < index.getBlocks().size(); i++) {
      VectorIndex.VectorBlockMetadata block = index.getBlocks().get(i);
      var blockCount = "Block " + i + ": " + block.getVectorCount() + " vectors ,";
      System.out.println(blockCount + "centroid=" + Arrays.toString(block.getCentroid()));
    }

    System.out.println();
  }

  /**
   * Demonstrates creating vector data for RFile storage.
   */
  public static List<KeyValue> createSampleVectorData() {
    System.out.println("=== Creating Sample Vector Data ===");

    List<KeyValue> vectorData = new ArrayList<>();

    // Create some sample document embeddings
    String[] documents = {"machine learning artificial intelligence",
        "natural language processing text analysis", "computer vision image recognition",
        "deep learning neural networks", "data science analytics"};

    // Simulate document embeddings (in real use case, these would come from ML models)
    float[][] embeddings = {{0.8f, 0.2f, 0.1f, 0.9f}, // ML/AI focused
        {0.1f, 0.9f, 0.2f, 0.7f}, // NLP focused
        {0.2f, 0.1f, 0.9f, 0.8f}, // Computer vision focused
        {0.9f, 0.3f, 0.4f, 0.95f}, // Deep learning focused
        {0.4f, 0.8f, 0.3f, 0.6f} // Data science focused
    };

    for (int i = 0; i < documents.length; i++) {
      Key key = new Key("doc" + i, "embedding", "v1");
      Value value = Value.newVector(embeddings[i]);
      vectorData.add(new KeyValue(key, value));

      System.out.println("Created vector for '" + documents[i] + "':");
      System.out.println("  Key: " + key);
      System.out.println("  Vector: " + Arrays.toString(embeddings[i]));
    }

    System.out.println("Created " + vectorData.size() + " vector entries");
    System.out.println();

    return vectorData;
  }

  /**
   * Demonstrates vector similarity calculations.
   */
  public static void demonstrateSimilarityCalculations() {
    System.out.println("=== Vector Similarity Calculations ===");

    // Sample vectors
    float[] queryVector = {0.7f, 0.3f, 0.2f, 0.8f};
    float[] doc1Vector = {0.8f, 0.2f, 0.1f, 0.9f}; // Should be similar
    float[] doc2Vector = {0.1f, 0.9f, 0.8f, 0.2f}; // Should be less similar

    System.out.println("Query vector: " + Arrays.toString(queryVector));
    System.out.println("Document 1 vector: " + Arrays.toString(doc1Vector));
    System.out.println("Document 2 vector: " + Arrays.toString(doc2Vector));

    // Calculate cosine similarity manually for demonstration
    float cosineSim1 = calculateCosineSimilarity(queryVector, doc1Vector);
    float cosineSim2 = calculateCosineSimilarity(queryVector, doc2Vector);

    System.out.println("\nCosine similarities:");
    System.out.println("Query vs Doc1: " + cosineSim1);
    System.out.println("Query vs Doc2: " + cosineSim2);
    System.out.println(
        "Doc1 is " + (cosineSim1 > cosineSim2 ? "more" : "less") + " similar to query than Doc2");

    // Calculate dot product similarity
    float dotProd1 = calculateDotProduct(queryVector, doc1Vector);
    float dotProd2 = calculateDotProduct(queryVector, doc2Vector);

    System.out.println("\nDot product similarities:");
    System.out.println("Query vs Doc1: " + dotProd1);
    System.out.println("Query vs Doc2: " + dotProd2);

    System.out.println();
  }

  /**
   * Demonstrates how VectorIterator would be used.
   */
  public static void demonstrateVectorIteratorUsage() {
    System.out.println("=== Vector Iterator Usage Example ===");

    // This demonstrates the API - actual usage would require RFile setup
    System.out.println("Usage pattern for VectorIterator:");
    System.out.println("1. Create RFile.Reader with vector data");
    System.out.println("2. Get vector index from reader");
    System.out.println("3. Create VectorIterator with query parameters");
    System.out.println("4. Perform similarity search");
    System.out.println("5. Iterate through ranked results");

    System.out.println("\nExample configuration:");
    System.out.println("Query vector: [0.5, 0.3, 0.8, 0.2]");
    System.out.println("Similarity type: COSINE");
    System.out.println("Top K: 10");
    System.out.println("Threshold: 0.7");

    System.out.println("\nPseudo-code:");
    System.out.println("RFile.Reader reader = ...;");
    System.out.println("VectorIterator iter = reader.createVectorIterator(");
    System.out.println("    queryVector, SimilarityType.COSINE, 10, 0.7f);");
    System.out.println("iter.seek(range, columnFamilies, inclusive);");
    System.out.println("while (iter.hasTop()) {");
    System.out.println("    Key key = iter.getTopKey();");
    System.out.println("    Value value = iter.getTopValue();");
    System.out.println("    // Process result");
    System.out.println("    iter.next();");
    System.out.println("}");

    System.out.println();
  }

  // Helper methods for similarity calculations

  private static float calculateCosineSimilarity(float[] v1, float[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Vectors must have same length");
    }

    float dotProduct = 0.0f;
    float norm1 = 0.0f;
    float norm2 = 0.0f;

    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }

    if (norm1 == 0.0f || norm2 == 0.0f) {
      return 0.0f;
    }

    return dotProduct / (float) (Math.sqrt(norm1) * Math.sqrt(norm2));
  }

  private static float calculateDotProduct(float[] v1, float[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Vectors must have same length");
    }

    float dotProduct = 0.0f;
    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
    }
    return dotProduct;
  }

  /**
   * Main method to run all examples.
   */
  public static void main(String[] args) {
    System.out.println("Accumulo Vector Store Example");
    System.out.println("=============================");
    System.out.println();

    demonstrateVectorValues();
    demonstrateVectorIndex();
    createSampleVectorData();
    demonstrateSimilarityCalculations();
    demonstrateVectorIteratorUsage();

    System.out.println("Vector store example completed successfully!");
  }
}
