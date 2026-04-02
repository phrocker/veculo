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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for vector compression functionality.
 */
public class VectorCompressionTest {

  @Test
  public void testCompress8Bit() {
    float[] original = {0.1f, -0.5f, 1.0f, 0.8f, -0.2f};

    VectorCompression.CompressedVector compressed = VectorCompression.compress8Bit(original);
    float[] decompressed = VectorCompression.decompress(compressed);

    assertEquals(original.length, decompressed.length);
    assertEquals(4.0f, compressed.getCompressionRatio(), 0.001f);

    // Check that decompressed values are close to originals (within quantization error)
    for (int i = 0; i < original.length; i++) {
      assertEquals(original[i], decompressed[i], 0.1f,
          "Decompressed value should be close to original");
    }
  }

  @Test
  public void testCompress16Bit() {
    float[] original = {0.1f, -0.5f, 1.0f, 0.8f, -0.2f};

    VectorCompression.CompressedVector compressed = VectorCompression.compress16Bit(original);
    float[] decompressed = VectorCompression.decompress(compressed);

    assertEquals(original.length, decompressed.length);
    assertEquals(2.0f, compressed.getCompressionRatio(), 0.001f);

    // 16-bit compression should be more accurate than 8-bit
    for (int i = 0; i < original.length; i++) {
      assertEquals(original[i], decompressed[i], 0.01f,
          "16-bit compression should be more accurate");
    }
  }

  @Test
  public void testEmptyVector() {
    float[] empty = new float[0];

    VectorCompression.CompressedVector compressed = VectorCompression.compress8Bit(empty);
    float[] decompressed = VectorCompression.decompress(compressed);

    assertEquals(0, decompressed.length);
  }

  @Test
  public void testConstantVector() {
    float[] constant = {5.0f, 5.0f, 5.0f, 5.0f};

    VectorCompression.CompressedVector compressed = VectorCompression.compress8Bit(constant);
    float[] decompressed = VectorCompression.decompress(compressed);

    for (int i = 0; i < constant.length; i++) {
      assertEquals(constant[i], decompressed[i], 0.001f);
    }
  }

  @Test
  public void testLargeRangeVector() {
    float[] largeRange = {-1000.0f, 0.0f, 1000.0f};

    VectorCompression.CompressedVector compressed = VectorCompression.compress8Bit(largeRange);
    float[] decompressed = VectorCompression.decompress(compressed);

    // With large ranges, expect some quantization error but relative ordering preserved
    assertTrue(decompressed[0] < decompressed[1]);
    assertTrue(decompressed[1] < decompressed[2]);
  }
}
