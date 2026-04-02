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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests for advanced vector indexing functionality.
 */
public class VectorIndexFooterTest {

  @Test
  public void testHierarchicalIndexBuilding() {
    VectorIndexFooter footer =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.HIERARCHICAL);

    // Create some sample centroids
    List<float[]> centroids =
        Arrays.asList(new float[] {1.0f, 0.0f, 0.0f}, new float[] {0.0f, 1.0f, 0.0f},
            new float[] {0.0f, 0.0f, 1.0f}, new float[] {0.5f, 0.5f, 0.0f});

    footer.buildHierarchicalIndex(centroids, 2);

    assertEquals(VectorIndexFooter.IndexingType.HIERARCHICAL, footer.getIndexingType());
    assertEquals(2, footer.getGlobalCentroids().length);
    assertEquals(4, footer.getClusterAssignments().length);
  }

  @Test
  public void testIVFIndexBuilding() {
    VectorIndexFooter footer = new VectorIndexFooter(2, VectorIndexFooter.IndexingType.IVF);

    List<float[]> centroids = Arrays.asList(new float[] {1.0f, 0.0f}, new float[] {0.0f, 1.0f},
        new float[] {-1.0f, 0.0f}, new float[] {0.0f, -1.0f});

    footer.buildIVFIndex(centroids, 2);

    assertEquals(VectorIndexFooter.IndexingType.IVF, footer.getIndexingType());
    assertEquals(2, footer.getGlobalCentroids().length);

    // Each block should be assigned to multiple clusters for better recall
    for (int[] assignment : footer.getClusterAssignments()) {
      assertTrue(assignment.length > 0);
    }
  }

  @Test
  public void testCandidateBlockSelection() {
    VectorIndexFooter footer =
        new VectorIndexFooter(2, VectorIndexFooter.IndexingType.HIERARCHICAL);

    List<float[]> centroids = Arrays.asList(new float[] {1.0f, 0.0f}, new float[] {0.0f, 1.0f},
        new float[] {-1.0f, 0.0f});

    footer.buildHierarchicalIndex(centroids, 2);

    // Query vector close to first centroid
    float[] queryVector = {0.9f, 0.1f};
    List<Integer> candidates = footer.findCandidateBlocks(queryVector, 5);

    assertFalse(candidates.isEmpty());
    assertTrue(candidates.size() <= 5);
  }

  @Test
  public void testFlatIndexing() {
    VectorIndexFooter footer = new VectorIndexFooter(2, VectorIndexFooter.IndexingType.FLAT);

    // For flat indexing, should return all blocks
    float[] queryVector = {0.5f, 0.5f};
    List<Integer> candidates = footer.findCandidateBlocks(queryVector, 10);

    assertEquals(0, candidates.size()); // No blocks configured in this test
  }

  @Test
  public void testIndexTypeEnumeration() {
    assertEquals(0, VectorIndexFooter.IndexingType.FLAT.getTypeId());
    assertEquals(1, VectorIndexFooter.IndexingType.IVF.getTypeId());
    assertEquals(2, VectorIndexFooter.IndexingType.HIERARCHICAL.getTypeId());
    assertEquals(3, VectorIndexFooter.IndexingType.PQ.getTypeId());
    assertEquals(4, VectorIndexFooter.IndexingType.TESSELLATION.getTypeId());

    assertEquals(VectorIndexFooter.IndexingType.FLAT,
        VectorIndexFooter.IndexingType.fromTypeId((byte) 0));
    assertEquals(VectorIndexFooter.IndexingType.IVF,
        VectorIndexFooter.IndexingType.fromTypeId((byte) 1));
    assertEquals(VectorIndexFooter.IndexingType.TESSELLATION,
        VectorIndexFooter.IndexingType.fromTypeId((byte) 4));
  }

  @Test
  public void testTessellationIndexBuilding() {
    VectorIndexFooter footer =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.TESSELLATION);

    // Build tessellation index from block vectors
    List<List<float[]>> blockVectors = new ArrayList<>();
    blockVectors.add(Arrays.asList(new float[] {1.0f, 0.0f, 0.0f}, new float[] {0.9f, 0.1f, 0.0f}));
    blockVectors.add(Arrays.asList(new float[] {0.0f, 1.0f, 0.0f}, new float[] {0.1f, 0.9f, 0.0f}));
    blockVectors.add(Arrays.asList(new float[] {0.0f, 0.0f, 1.0f}, new float[] {0.0f, 0.1f, 0.9f}));

    footer.buildTessellationIndex(blockVectors, 4);

    assertEquals(VectorIndexFooter.IndexingType.TESSELLATION, footer.getIndexingType());
    assertEquals(4, footer.getTessellationDepth());
    assertEquals(3, footer.getTessellationCellRanges().length);

    // Each block should have a valid cell range
    for (long[] range : footer.getTessellationCellRanges()) {
      assertTrue(range[0] <= range[1], "min should be <= max");
      assertTrue(range[0] >= 0, "cell ID should be non-negative");
    }
  }

  @Test
  public void testTessellationCandidateSelection() {
    VectorIndexFooter footer =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.TESSELLATION);

    List<List<float[]>> blockVectors = new ArrayList<>();
    // Block 0: vectors near [1,0,0]
    blockVectors
        .add(Arrays.asList(new float[] {1.0f, 0.0f, 0.0f}, new float[] {0.95f, 0.05f, 0.0f}));
    // Block 1: vectors near [0,0,1] — very different direction
    blockVectors
        .add(Arrays.asList(new float[] {0.0f, 0.0f, 1.0f}, new float[] {0.0f, 0.05f, 0.95f}));

    footer.buildTessellationIndex(blockVectors, 4);

    // Query near [1,0,0] — should find block 0, may or may not find block 1
    float[] queryVector = {0.98f, 0.02f, 0.0f};
    List<Integer> candidates = footer.findCandidateBlocks(queryVector, 10);

    assertFalse(candidates.isEmpty(), "Should find at least one candidate block");
    assertTrue(candidates.contains(0), "Block 0 (similar vectors) should be a candidate");
  }

  @Test
  public void testTessellationNoDrift() {
    // Key property: tessellation cell IDs are deterministic and data-independent.
    // Building the index at different times with different data should produce
    // consistent cell IDs for the same vectors.
    VectorIndexFooter footer1 =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.TESSELLATION);
    VectorIndexFooter footer2 =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.TESSELLATION);

    float[] testVector = {0.5f, 0.3f, 0.8f};

    // Build with different surrounding data
    List<List<float[]>> blockVectors1 = new ArrayList<>();
    blockVectors1.add(Arrays.asList(testVector, new float[] {1.0f, 0.0f, 0.0f}));

    List<List<float[]>> blockVectors2 = new ArrayList<>();
    blockVectors2.add(Arrays.asList(testVector, new float[] {0.0f, 0.0f, 1.0f}));

    footer1.buildTessellationIndex(blockVectors1, 4);
    footer2.buildTessellationIndex(blockVectors2, 4);

    // Both should include the same cell ID for testVector in the range
    long cellId = org.apache.accumulo.core.graph.SphericalTessellation.assignCellId(testVector, 4);

    long[] range1 = footer1.getTessellationCellRanges()[0];
    long[] range2 = footer2.getTessellationCellRanges()[0];

    assertTrue(cellId >= range1[0] && cellId <= range1[1],
        "Cell ID should be in range for footer1");
    assertTrue(cellId >= range2[0] && cellId <= range2[1],
        "Cell ID should be in range for footer2");
  }

  @Test
  public void testTessellationSerialization() throws IOException {
    VectorIndexFooter footer =
        new VectorIndexFooter(3, VectorIndexFooter.IndexingType.TESSELLATION);

    List<List<float[]>> blockVectors = new ArrayList<>();
    blockVectors.add(Arrays.asList(new float[] {1.0f, 0.0f, 0.0f}, new float[] {0.9f, 0.1f, 0.0f}));
    blockVectors.add(Arrays.asList(new float[] {0.0f, 1.0f, 0.0f}));

    footer.buildTessellationIndex(blockVectors, 5);

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    footer.write(new DataOutputStream(baos));

    // Deserialize
    VectorIndexFooter restored = new VectorIndexFooter();
    restored.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

    assertEquals(footer.getIndexingType(), restored.getIndexingType());
    assertEquals(footer.getTessellationDepth(), restored.getTessellationDepth());
    assertEquals(footer.getTessellationCellRanges().length,
        restored.getTessellationCellRanges().length);

    for (int i = 0; i < footer.getTessellationCellRanges().length; i++) {
      assertEquals(footer.getTessellationCellRanges()[i][0],
          restored.getTessellationCellRanges()[i][0]);
      assertEquals(footer.getTessellationCellRanges()[i][1],
          restored.getTessellationCellRanges()[i][1]);
    }
  }

  @Test
  public void testEmptyIndexBehavior() {
    VectorIndexFooter footer = new VectorIndexFooter();

    float[] queryVector = {1.0f, 0.0f};
    List<Integer> candidates = footer.findCandidateBlocks(queryVector, 5);

    assertTrue(candidates.isEmpty());
  }

  @Test
  public void testDimensionValidation() {
    VectorIndexFooter footer =
        new VectorIndexFooter(2, VectorIndexFooter.IndexingType.HIERARCHICAL);

    // Create centroids with mismatched dimensions
    List<float[]> centroids = Arrays.asList(new float[] {1.0f, 0.0f}, // 2D
        new float[] {0.0f, 1.0f, 0.0f}); // 3D - this should cause an exception

    try {
      footer.buildHierarchicalIndex(centroids, 2);
      assertTrue(false, "Expected IllegalArgumentException for mismatched dimensions");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("All points must have the same dimension"));
    }
  }
}
