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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for ValueType enumeration.
 */
public class ValueTypeTest {

  @Test
  public void testValueTypeConstants() {
    assertEquals((byte) 0, ValueType.BYTES.getTypeId());
    assertEquals((byte) 1, ValueType.VECTOR_FLOAT32.getTypeId());
  }

  @Test
  public void testFromTypeId() {
    assertEquals(ValueType.BYTES, ValueType.fromTypeId((byte) 0));
    assertEquals(ValueType.VECTOR_FLOAT32, ValueType.fromTypeId((byte) 1));
  }

  @Test
  public void testFromTypeIdInvalid() {
    assertThrows(IllegalArgumentException.class, () -> {
      ValueType.fromTypeId((byte) 99);
    });
  }
}
