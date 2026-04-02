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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.file.rfile.VectorCompression;
import org.junit.jupiter.api.Test;

/**
 * Tests for enhanced vector functionality including chunking and compression.
 */
public class ValueVectorEnhancedTest {

  @Test
  public void testVectorChunking() {
    // Create a large vector that needs chunking
    float[] largeVector = new float[1000];
    for (int i = 0; i < largeVector.length; i++) {
      largeVector[i] = i * 0.001f;
    }

    // Chunk into smaller pieces
    Value[] chunks = Value.chunkVector(largeVector, 250);

    assertEquals(4, chunks.length); // 1000 / 250 = 4 chunks

    // Verify each chunk is a vector type
    for (Value chunk : chunks) {
      assertEquals(ValueType.VECTOR_FLOAT32, chunk.getValueType());
    }

    // Reassemble and verify
    float[] reassembled = Value.reassembleVector(chunks);
    assertArrayEquals(largeVector, reassembled, 0.001f);
  }

  @Test
  public void testVectorChunkingUneven() {
    float[] vector = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f};

    Value[] chunks = Value.chunkVector(vector, 3);

    assertEquals(3, chunks.length); // 7 elements, chunk size 3 = 3 chunks

    // First two chunks should have 3 elements each, last chunk should have 1
    assertEquals(3, chunks[0].asVector().length);
    assertEquals(3, chunks[1].asVector().length);
    assertEquals(1, chunks[2].asVector().length);

    float[] reassembled = Value.reassembleVector(chunks);
    assertArrayEquals(vector, reassembled, 0.001f);
  }

  @Test
  public void testCompressedVectorCreation() {
    float[] original = {0.1f, -0.5f, 1.0f, 0.8f, -0.2f};

    // Create compressed vector with 8-bit quantization
    Value compressedValue =
        Value.newCompressedVector(original, VectorCompression.COMPRESSION_QUANTIZED_8BIT);

    assertEquals(ValueType.VECTOR_FLOAT32, compressedValue.getValueType());

    // Decompress and verify
    float[] decompressed = compressedValue.asCompressedVector();
    assertEquals(original.length, decompressed.length);

    // Should be close but not exact due to quantization
    for (int i = 0; i < original.length; i++) {
      assertEquals(original[i], decompressed[i], 0.1f);
    }
  }

  @Test
  public void testCompressedVectorFallback() {
    float[] original = {0.1f, -0.5f, 1.0f};

    // Create with no compression
    Value uncompressedValue =
        Value.newCompressedVector(original, VectorCompression.COMPRESSION_NONE);

    // Should be able to read as regular vector
    float[] asVector = uncompressedValue.asVector();
    assertArrayEquals(original, asVector, 0.001f);
  }

  @Test
  public void testEmptyVectorChunking() {
    float[] empty = new float[0];

    Value[] chunks = Value.chunkVector(empty, 10);

    assertEquals(0, chunks.length);

    float[] reassembled = Value.reassembleVector(new Value[0]);
    assertEquals(0, reassembled.length);
  }

  @Test
  public void testInvalidChunkSize() {
    float[] vector = {1.0f, 2.0f, 3.0f};

    assertThrows(IllegalArgumentException.class, () -> {
      Value.chunkVector(vector, 0);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      Value.chunkVector(vector, -1);
    });
  }

  @Test
  public void testInvalidReassembly() {
    Value regularValue = new Value("not a vector".getBytes());
    Value[] invalidChunks = {regularValue};

    assertThrows(IllegalArgumentException.class, () -> {
      Value.reassembleVector(invalidChunks);
    });
  }

  @Test
  public void testSingleChunk() {
    float[] smallVector = {1.0f, 2.0f};

    Value[] chunks = Value.chunkVector(smallVector, 10); // Chunk size larger than vector

    assertEquals(1, chunks.length);
    assertArrayEquals(smallVector, chunks[0].asVector(), 0.001f);

    float[] reassembled = Value.reassembleVector(chunks);
    assertArrayEquals(smallVector, reassembled, 0.001f);
  }
}
