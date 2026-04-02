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

/**
 * Enumeration of supported value types for specialized value handling in Accumulo.
 */
public enum ValueType {

  /**
   * Standard byte array value type - the default for all existing values.
   */
  BYTES((byte) 0),

  /**
   * 32-bit floating point vector value type for vector similarity operations. Values of this type
   * contain a sequence of IEEE 754 single-precision floating point numbers.
   */
  VECTOR_FLOAT32((byte) 1);

  private final byte typeId;

  ValueType(byte typeId) {
    this.typeId = typeId;
  }

  /**
   * Gets the byte identifier for this value type.
   *
   * @return the byte identifier
   */
  public byte getTypeId() {
    return typeId;
  }

  /**
   * Gets the ValueType for the given type identifier.
   *
   * @param typeId the type identifier
   * @return the corresponding ValueType
   * @throws IllegalArgumentException if the typeId is not recognized
   */
  public static ValueType fromTypeId(byte typeId) {
    for (ValueType type : values()) {
      if (type.typeId == typeId) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown ValueType id: " + typeId);
  }
}
