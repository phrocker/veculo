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

import org.junit.jupiter.api.Test;

/**
 * Tests for Value vector functionality.
 */
public class ValueVectorTest {

  @Test
  public void testNewVector() {
    float[] vector = {1.0f, 2.0f, 3.0f, 4.5f};
    Value value = Value.newVector(vector);

    assertEquals(ValueType.VECTOR_FLOAT32, value.getValueType());
    assertArrayEquals(vector, value.asVector(), 0.0001f);
  }

  @Test
  public void testAsVectorWithWrongType() {
    Value value = new Value("hello".getBytes());
    value.setValueType(ValueType.BYTES);

    assertThrows(IllegalStateException.class, () -> {
      value.asVector();
    });
  }

  @Test
  public void testAsVectorWithInvalidLength() {
    Value value = new Value(new byte[] {1, 2, 3}); // 3 bytes, not divisible by 4
    value.setValueType(ValueType.VECTOR_FLOAT32);

    assertThrows(IllegalArgumentException.class, () -> {
      value.asVector();
    });
  }

  @Test
  public void testEmptyVector() {
    float[] vector = {};
    Value value = Value.newVector(vector);

    assertEquals(ValueType.VECTOR_FLOAT32, value.getValueType());
    assertArrayEquals(vector, value.asVector(), 0.0001f);
    assertEquals(0, value.getSize());
  }

  @Test
  public void testDefaultValueType() {
    Value value = new Value();
    assertEquals(ValueType.BYTES, value.getValueType());
  }

  @Test
  public void testSetValueType() {
    Value value = new Value();
    assertEquals(ValueType.BYTES, value.getValueType());

    value.setValueType(ValueType.VECTOR_FLOAT32);
    assertEquals(ValueType.VECTOR_FLOAT32, value.getValueType());
  }
}
