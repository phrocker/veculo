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

import org.apache.accumulo.core.file.rfile.VectorIndex.VectorBlockMetadata;
import org.junit.jupiter.api.Test;

/**
 * Tests for VectorIndex functionality.
 */
public class VectorIndexTest {

  @Test
  public void testVectorIndexCreation() {
    VectorIndex index = new VectorIndex(3);
    assertEquals(3, index.getVectorDimension());
    assertTrue(index.getBlocks().isEmpty());
  }

  @Test
  public void testAddBlock() {
    VectorIndex index = new VectorIndex(3);
    float[] centroid = {1.0f, 2.0f, 3.0f};
    VectorBlockMetadata block = new VectorBlockMetadata(centroid, 10, 1000L, 256);

    index.addBlock(block);

    assertEquals(1, index.getBlocks().size());
    VectorBlockMetadata retrieved = index.getBlocks().get(0);
    assertEquals(10, retrieved.getVectorCount());
    assertEquals(1000L, retrieved.getBlockOffset());
    assertEquals(256, retrieved.getBlockSize());
  }

  @Test
  public void testMultipleBlocks() {
    VectorIndex index = new VectorIndex(2);

    VectorBlockMetadata block1 = new VectorBlockMetadata(new float[] {1.0f, 2.0f}, 5, 0L, 128);
    VectorBlockMetadata block2 = new VectorBlockMetadata(new float[] {3.0f, 4.0f}, 8, 128L, 192);

    index.addBlock(block1);
    index.addBlock(block2);

    assertEquals(2, index.getBlocks().size());
    assertEquals(5, index.getBlocks().get(0).getVectorCount());
    assertEquals(8, index.getBlocks().get(1).getVectorCount());
  }

  @Test
  public void testVectorBlockMetadata() {
    float[] centroid = {0.5f, -1.2f, 2.8f};
    VectorBlockMetadata block = new VectorBlockMetadata(centroid, 15, 2048L, 512);

    assertEquals(3, block.getCentroid().length);
    assertEquals(0.5f, block.getCentroid()[0], 0.001f);
    assertEquals(-1.2f, block.getCentroid()[1], 0.001f);
    assertEquals(2.8f, block.getCentroid()[2], 0.001f);
    assertEquals(15, block.getVectorCount());
    assertEquals(2048L, block.getBlockOffset());
    assertEquals(512, block.getBlockSize());
  }
}
