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

import java.util.Set;

import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.ChooserHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link VolumeChooser} that implements tiered storage by routing data to different volumes based
 * on the compaction context. Minor compactions and WALs go to fast local storage. Major compactions
 * migrate data to durable cloud storage.
 *
 * <p>
 * Configure via properties:
 * <ul>
 * <li>{@code general.custom.volume.tiered.local} — URI prefix for local volume (e.g.,
 * {@code file:///mnt/nvme/accumulo})</li>
 * <li>{@code general.custom.volume.tiered.cloud} — URI prefix for cloud volume (e.g.,
 * {@code gs://bucket/accumulo})</li>
 * </ul>
 *
 * <p>
 * Per-table override:
 * <ul>
 * <li>{@code table.custom.volume.tiered.local}</li>
 * <li>{@code table.custom.volume.tiered.cloud}</li>
 * </ul>
 *
 * <p>
 * Behavior:
 * <ul>
 * <li>{@link ChooserHint#MINOR_COMPACTION} — local volume (hot data, fast writes)</li>
 * <li>{@link ChooserHint#MAJOR_COMPACTION} — cloud volume (cold data, durable storage)</li>
 * <li>{@link ChooserHint#WAL} — local volume (must be fast + syncable)</li>
 * <li>{@link ChooserHint#GENERAL} — local volume (default to fast path)</li>
 * </ul>
 *
 * <p>
 * If no cloud volume is configured or available, falls back to local for all operations. If no
 * local volume matches, falls back to any available volume.
 *
 * @since 4.0.0
 */
public class TieredVolumeChooser implements VolumeChooser {

  private static final Logger log = LoggerFactory.getLogger(TieredVolumeChooser.class);

  private static final String LOCAL_PROP = "volume.tiered.local";
  private static final String CLOUD_PROP = "volume.tiered.cloud";

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options) {
    // During INIT scope, config may not be available. Use the cloud/shared volume
    // so that instance_id and system tables are accessible to all nodes.
    // The local volume (file://) is per-node and not shared.
    if (env.getChooserScope() == VolumeChooserEnvironment.Scope.INIT) {
      // Prefer non-local (cloud) volume for init — it's shared across all nodes
      for (String option : options) {
        if (!option.startsWith("file:")) {
          log.trace("INIT scope — using shared volume: {}", option);
          return option;
        }
      }
      // Fallback to first if all are local
      String fallback = options.iterator().next();
      log.trace("INIT scope — no shared volume found, using: {}", fallback);
      return fallback;
    }

    String localPrefix = getProperty(env, LOCAL_PROP);
    String cloudPrefix = getProperty(env, CLOUD_PROP);

    String preferred;
    switch (env.getChooserHint()) {
      case MINOR_COMPACTION:
      case PARTIAL_MAJOR_COMPACTION:
      case WAL:
        preferred = localPrefix;
        break;
      case MAJOR_COMPACTION:
        preferred = cloudPrefix != null ? cloudPrefix : localPrefix;
        break;
      default:
        preferred = localPrefix;
        break;
    }

    if (preferred != null) {
      for (String option : options) {
        if (option.startsWith(preferred)) {
          log.trace("Chose {} for hint {} (preferred={})", option, env.getChooserHint(), preferred);
          return option;
        }
      }
      log.debug("Preferred volume prefix {} not found in options {} for hint {}", preferred,
          options, env.getChooserHint());
    }

    // Fallback: if preferred volume not found in options, pick first local, then any
    if (localPrefix != null) {
      for (String option : options) {
        if (option.startsWith(localPrefix)) {
          log.trace("Falling back to local volume {} for hint {}", option, env.getChooserHint());
          return option;
        }
      }
    }

    // Last resort: pick any option
    String fallback = options.iterator().next();
    log.debug("No preferred or local volume found; falling back to {}", fallback);
    return fallback;
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options) {
    return options;
  }

  private String getProperty(VolumeChooserEnvironment env, String suffix) {
    try {
      var serviceEnv = env.getServiceEnv();
      // Try table-level property first, then general
      if (env.getTable().isPresent()) {
        try {
          String tableValue =
              serviceEnv.getConfiguration(env.getTable().orElseThrow()).getTableCustom(suffix);
          if (tableValue != null && !tableValue.isEmpty()) {
            return tableValue;
          }
        } catch (Exception e) {
          log.trace("Could not read table property {}: {}", suffix, e.getMessage());
        }
      }
      String generalValue = serviceEnv.getConfiguration().getCustom(suffix);
      return (generalValue != null && !generalValue.isEmpty()) ? generalValue : null;
    } catch (Exception e) {
      log.trace("Could not read property {}: {}", suffix, e.getMessage());
      return null;
    }
  }
}
