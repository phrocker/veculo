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
package org.apache.accumulo.server.fs;

import java.util.List;

/**
 * Interface for Hadoop FileSystem implementations that track WAL replication peers. When a WAL
 * segment is opened via the sidecar, the FileSystem records which peer addresses hold replicas.
 * This information is stored in the metadata table so the recovery LogCloser can contact the right
 * peers without relying on DNS pattern guessing.
 */
public interface WalPeerProvider {

  /**
   * Returns the peer sidecar addresses that were configured for replication when the given WAL
   * segment was opened. Returns an empty list if the segment is unknown or the FileSystem does not
   * support peer tracking.
   *
   * @param segmentUuid the UUID of the WAL segment
   * @return list of peer addresses (e.g., "host:port")
   */
  List<String> getSegmentPeers(String segmentUuid);
}
