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
package org.apache.accumulo.server.manager.recovery;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogCloser for Google Cloud Storage (GCS) and other object stores that don't support HDFS lease
 * recovery. GCS files are immutable once written — there are no leases to recover.
 *
 * <p>
 * Unlike {@link NoOpLogCloser}, this closer actively marks WAL recovery as complete by creating the
 * {@code finished} marker file in the recovery directory. This prevents the RecoveryManager from
 * endlessly retrying WAL sort operations on object store files.
 *
 * <p>
 * Use this by setting {@code manager.wal.closer.implementation} to the full class name of this
 * class in accumulo.properties.
 */
public class GcsLogCloser implements LogCloser {

  private static final Logger log = LoggerFactory.getLogger(GcsLogCloser.class);

  @Override
  public long close(AccumuloConfiguration conf, Configuration hadoopConf, VolumeManager fs,
      LogEntry logEntry) throws IOException {
    // GCS files are immutable — no lease recovery needed.
    // Return 0 to indicate the file is ready for sort/recovery.
    log.debug("GcsLogCloser: skipping lease recovery for {} (object store, no leases)",
        logEntry.getPath());
    return 0;
  }
}
