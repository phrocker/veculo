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
package org.apache.accumulo.core.spi.fs;

import java.util.Optional;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;

/**
 * @since 2.1.0
 */
public interface VolumeChooserEnvironment {
  /**
   * A scope for the volume chooser environment; a TABLE scope should be accompanied by a tableId.
   *
   * @since 2.1.0
   */
  public static enum Scope {
    DEFAULT, TABLE, INIT, LOGGER
  }

  /**
   * Hint about the context in which the volume choice is being made. Enables tiered storage where
   * hot data stays on fast local volumes and cold data migrates to durable cloud volumes during
   * compaction.
   *
   * @since 4.0.0
   */
  enum ChooserHint {
    /** Minor compaction output — prefer fast local storage */
    MINOR_COMPACTION,
    /** Partial major compaction output — prefer fast local storage (merges subset of files) */
    PARTIAL_MAJOR_COMPACTION,
    /** Full major compaction output — use durable cloud storage (merges all files) */
    MAJOR_COMPACTION,
    /** Write-ahead log — must be fast and support sync operations */
    WAL,
    /** General/default — no specific hint */
    GENERAL
  }

  /**
   * Returns a hint about the context of this volume choice. Implementations can use this to route
   * data to different storage tiers.
   *
   * @return the chooser hint, defaults to {@link ChooserHint#GENERAL}
   * @since 4.0.0
   */
  default ChooserHint getChooserHint() {
    return ChooserHint.GENERAL;
  }

  /**
   * The end row of the tablet for which a volume is being chosen. Only call this when the scope is
   * TABLE
   *
   * @since 2.0.0
   */
  public Text getEndRow();

  public Optional<TableId> getTable();

  public Scope getChooserScope();

  public ServiceEnvironment getServiceEnv();
}
