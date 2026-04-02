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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SphericalTessellationTest {

  @Test
  public void testAssignCellIdReturnsSameIdForSameVector() {
    float[] vector = new float[] {1.0f, 0.0f, 0.0f};
    long cellId1 = SphericalTessellation.assignCellId(vector);
    long cellId2 = SphericalTessellation.assignCellId(vector);
    assertEquals(cellId1, cellId2, "Same vector should always produce the same cell ID");
  }

  @Test
  public void testAssignCellIdReturnsDifferentIdsForDifferentVectors() {
    float[] vectorA = new float[] {1.0f, 0.0f, 0.0f};
    float[] vectorB = new float[] {0.0f, 0.0f, 1.0f};

    long cellIdA = SphericalTessellation.assignCellId(vectorA);
    long cellIdB = SphericalTessellation.assignCellId(vectorB);

    assertNotEquals(cellIdA, cellIdB, "Very different vectors should produce different cell IDs");
  }

  @Test
  public void testSimilarVectorsGetNearbyCellIds() {
    float[] vectorA = new float[] {1.0f, 0.0f, 0.0f};
    float[] vectorB = new float[] {0.99f, 0.1f, 0.0f};

    long cellIdA = SphericalTessellation.assignCellId(vectorA);
    long cellIdB = SphericalTessellation.assignCellId(vectorB);

    // Similar vectors should produce cell IDs that are close together
    long diff = Math.abs(cellIdA - cellIdB);

    // The very different vector should produce a larger difference
    float[] vectorC = new float[] {0.0f, 0.0f, 1.0f};
    long cellIdC = SphericalTessellation.assignCellId(vectorC);
    long diffFar = Math.abs(cellIdA - cellIdC);

    // The nearby cell ID difference should be smaller (or equal) to the far one
    // This is probabilistic due to LSH but should hold for these clearly separated vectors
    assertTrue(diff <= diffFar, "Similar vectors' cell ID difference (" + diff
        + ") should be <= different vectors' difference (" + diffFar + ")");
  }

  @Test
  public void testNormalizeProducesUnitLengthVector() {
    float[] vector = new float[] {3.0f, 4.0f, 0.0f};
    float[] normalized = SphericalTessellation.normalize(vector);

    assertNotNull(normalized);
    assertEquals(vector.length, normalized.length);

    // Compute magnitude of normalized vector
    float magnitude = 0.0f;
    for (float v : normalized) {
      magnitude += v * v;
    }
    magnitude = (float) Math.sqrt(magnitude);

    assertEquals(1.0f, magnitude, 0.0001f, "Normalized vector should have unit length");
  }

  @Test
  public void testNormalizePreservesDirection() {
    float[] vector = new float[] {3.0f, 4.0f, 0.0f};
    float[] normalized = SphericalTessellation.normalize(vector);

    // The ratio between components should be preserved
    float ratio = vector[0] / vector[1];
    float normalizedRatio = normalized[0] / normalized[1];
    assertEquals(ratio, normalizedRatio, 0.0001f,
        "Normalization should preserve the direction of the vector");
  }

  @Test
  public void testGetCellRangeReturnsValidRange() {
    float[] queryVector = new float[] {1.0f, 0.0f, 0.0f};
    int searchRadius = 2;

    long[] range = SphericalTessellation.getCellRange(queryVector, searchRadius);

    assertNotNull(range);
    assertEquals(2, range.length);
    assertTrue(range[0] >= 0, "Start of range should be non-negative");
    assertTrue(range[1] >= range[0], "End of range should be >= start");

    // The query vector's own cell ID should fall within the range
    long cellId = SphericalTessellation.assignCellId(queryVector);
    assertTrue(cellId >= range[0] && cellId <= range[1], "Query vector's cell ID (" + cellId
        + ") should fall within range [" + range[0] + ", " + range[1] + "]");
  }

  @Test
  public void testGetCellRangeWithDifferentRadii() {
    float[] queryVector = new float[] {0.5f, 0.5f, 0.5f};

    long[] narrowRange = SphericalTessellation.getCellRange(queryVector, 1);
    long[] wideRange = SphericalTessellation.getCellRange(queryVector, 4);

    long narrowWidth = narrowRange[1] - narrowRange[0];
    long wideWidth = wideRange[1] - wideRange[0];

    assertTrue(wideWidth >= narrowWidth, "Wider search radius should produce wider range");
  }

  @Test
  public void testDeterministicAcrossCalls() {
    // Multiple calls should produce identical results (deterministic hyperplanes)
    float[] vector = new float[] {0.3f, 0.7f, 0.1f, 0.5f};

    long cellId1 = SphericalTessellation.assignCellId(vector, 4);
    long cellId2 = SphericalTessellation.assignCellId(vector, 4);
    long cellId3 = SphericalTessellation.assignCellId(vector, 4);

    assertEquals(cellId1, cellId2);
    assertEquals(cellId2, cellId3);
  }

  @Test
  public void testScaledVectorProducesSameCellId() {
    float[] vector = new float[] {1.0f, 2.0f, 3.0f};
    float[] scaled = new float[] {2.0f, 4.0f, 6.0f};

    long cellId1 = SphericalTessellation.assignCellId(vector);
    long cellId2 = SphericalTessellation.assignCellId(scaled);

    assertEquals(cellId1, cellId2,
        "Scaled vector (same direction) should produce same cell ID after normalization");
  }
}
