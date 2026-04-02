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

import java.nio.ByteBuffer;

/**
 * Compression utilities for vector data to reduce storage footprint while maintaining similarity
 * computation capabilities.
 */
public class VectorCompression {

  public static final byte COMPRESSION_NONE = 0;
  public static final byte COMPRESSION_QUANTIZED_8BIT = 1;
  public static final byte COMPRESSION_QUANTIZED_16BIT = 2;

  /**
   * Compresses a float32 vector using 8-bit quantization. Maps float values to byte range [-128,
   * 127] while preserving relative magnitudes.
   *
   * @param vector the input vector to compress
   * @return compressed vector data with quantization parameters
   */
  public static CompressedVector compress8Bit(float[] vector) {
    if (vector == null || vector.length == 0) {
      return new CompressedVector(new byte[0], 0.0f, 0.0f, COMPRESSION_QUANTIZED_8BIT);
    }

    // Find min and max values for quantization range
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;
    for (float v : vector) {
      if (v < min) {
        min = v;
      }

      if (v > max) {
        max = v;
      }
    }

    // Avoid division by zero
    float range = max - min;
    if (range == 0.0f) {
      byte[] quantized = new byte[vector.length];
      return new CompressedVector(quantized, min, max, COMPRESSION_QUANTIZED_8BIT);
    }

    // Quantize to 8-bit range
    byte[] quantized = new byte[vector.length];
    float scale = 255.0f / range;
    for (int i = 0; i < vector.length; i++) {
      int quantizedValue = Math.round((vector[i] - min) * scale);
      quantized[i] = (byte) Math.max(0, Math.min(255, quantizedValue));
    }

    return new CompressedVector(quantized, min, max, COMPRESSION_QUANTIZED_8BIT);
  }

  /**
   * Compresses a float32 vector using 16-bit quantization. Higher precision than 8-bit but still 2x
   * compression ratio.
   *
   * @param vector the input vector to compress
   * @return compressed vector data with quantization parameters
   */
  public static CompressedVector compress16Bit(float[] vector) {
    if (vector == null || vector.length == 0) {
      return new CompressedVector(new byte[0], 0.0f, 0.0f, COMPRESSION_QUANTIZED_16BIT);
    }

    // Find min and max values
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;
    for (float v : vector) {
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }

    float range = max - min;
    if (range == 0.0f) {
      byte[] quantized = new byte[vector.length * 2];
      return new CompressedVector(quantized, min, max, COMPRESSION_QUANTIZED_16BIT);
    }

    // Quantize to 16-bit range
    ByteBuffer buffer = ByteBuffer.allocate(vector.length * 2);
    float scale = 65535.0f / range;
    for (float v : vector) {
      int quantizedValue = Math.round((v - min) * scale);
      short shortValue = (short) Math.max(0, Math.min(65535, quantizedValue));
      buffer.putShort(shortValue);
    }

    return new CompressedVector(buffer.array(), min, max, COMPRESSION_QUANTIZED_16BIT);
  }

  /**
   * Decompresses a vector back to float32 representation.
   *
   * @param compressed the compressed vector data
   * @return decompressed float32 vector
   */
  public static float[] decompress(CompressedVector compressed) {
    if (compressed.getData().length == 0) {
      return new float[0];
    }

    switch (compressed.getCompressionType()) {
      case COMPRESSION_QUANTIZED_8BIT:
        return decompress8Bit(compressed);
      case COMPRESSION_QUANTIZED_16BIT:
        return decompress16Bit(compressed);
      case COMPRESSION_NONE:
      default:
        // Convert bytes back to floats (raw storage)
        ByteBuffer buffer = ByteBuffer.wrap(compressed.getData());
        float[] result = new float[compressed.getData().length / 4];
        for (int i = 0; i < result.length; i++) {
          result[i] = buffer.getFloat();
        }
        return result;
    }
  }

  private static float[] decompress8Bit(CompressedVector compressed) {
    byte[] data = compressed.getData();
    float[] result = new float[data.length];
    float min = compressed.getMin();
    float max = compressed.getMax();
    float range = max - min;

    if (range == 0.0f) {
      // All values were the same
      for (int i = 0; i < result.length; i++) {
        result[i] = min;
      }
      return result;
    }

    float scale = range / 255.0f;
    for (int i = 0; i < data.length; i++) {
      int unsignedByte = data[i] & 0xFF;
      result[i] = min + (unsignedByte * scale);
    }

    return result;
  }

  private static float[] decompress16Bit(CompressedVector compressed) {
    byte[] data = compressed.getData();
    ByteBuffer buffer = ByteBuffer.wrap(data);
    float[] result = new float[data.length / 2];
    float min = compressed.getMin();
    float max = compressed.getMax();
    float range = max - min;

    if (range == 0.0f) {
      for (int i = 0; i < result.length; i++) {
        result[i] = min;
      }
      return result;
    }

    float scale = range / 65535.0f;
    for (int i = 0; i < result.length; i++) {
      int unsignedShort = buffer.getShort() & 0xFFFF;
      result[i] = min + (unsignedShort * scale);
    }

    return result;
  }

  /**
   * Container for compressed vector data and metadata.
   */
  public static class CompressedVector {
    private final byte[] data;
    private final float min;
    private final float max;
    private final byte compressionType;

    public CompressedVector(byte[] data, float min, float max, byte compressionType) {
      this.data = data;
      this.min = min;
      this.max = max;
      this.compressionType = compressionType;
    }

    public byte[] getData() {
      return data;
    }

    public float getMin() {
      return min;
    }

    public float getMax() {
      return max;
    }

    public byte getCompressionType() {
      return compressionType;
    }

    /**
     * Returns the compression ratio achieved (original size / compressed size).
     */
    public float getCompressionRatio() {
      switch (compressionType) {
        case COMPRESSION_QUANTIZED_8BIT:
          return 4.0f; // 32-bit -> 8-bit
        case COMPRESSION_QUANTIZED_16BIT:
          return 2.0f; // 32-bit -> 16-bit
        case COMPRESSION_NONE:
        default:
          return 1.0f;
      }
    }
  }
}
