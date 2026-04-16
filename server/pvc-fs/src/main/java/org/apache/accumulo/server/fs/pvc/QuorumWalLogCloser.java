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
package org.apache.accumulo.server.fs.pvc;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.pvc.proto.ListSegmentsRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListSegmentsResponse;
import org.apache.accumulo.server.fs.pvc.proto.SealSegmentRequest;
import org.apache.accumulo.server.fs.pvc.proto.SealSegmentResponse;
import org.apache.accumulo.server.fs.pvc.proto.SegmentId;
import org.apache.accumulo.server.fs.pvc.proto.SegmentInfo;
import org.apache.accumulo.server.fs.pvc.proto.WalQuorumPeerGrpc;
import org.apache.accumulo.server.manager.recovery.LogCloser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * {@link LogCloser} implementation for {@code qwal://} WAL segments. Ensures that a WAL segment is
 * sealed and uploaded to GCS so that WAL sort/recovery can proceed.
 *
 * <p>
 * The close procedure:
 * <ol>
 * <li>Check GCS for the sealed segment. If found, the WAL is ready for sort — return 0.</li>
 * <li>If not in GCS, derive peer sidecar hostnames from the StatefulSet DNS pattern.</li>
 * <li>Connect to each peer and call {@code ListSegments} to find who holds a replica.</li>
 * <li>Call {@code SealSegment} on the peer with the largest replica. The peer will finalize the
 * segment and upload it to GCS.</li>
 * <li>Return 0 once the seal is confirmed. If no peer has the segment, return a retry delay
 * (10 seconds) so the RecoveryManager will try again.</li>
 * </ol>
 *
 * <p>
 * Configure by setting {@code manager.wal.closer.implementation} to the fully qualified class name
 * of this class in accumulo.properties.
 */
public class QuorumWalLogCloser implements LogCloser {

  private static final Logger log = LoggerFactory.getLogger(QuorumWalLogCloser.class);

  /** Retry delay in milliseconds when no peer holds the segment. */
  private static final long RETRY_DELAY_MS = 10_000;

  @Override
  public long close(AccumuloConfiguration conf, Configuration hadoopConf, VolumeManager fs,
      LogEntry logEntry) throws IOException {
    Path path = new Path(logEntry.getPath());
    String walPath = path.toUri().getPath();
    String uuid = path.getName();

    log.debug("QuorumWalLogCloser: attempting to close WAL segment {}", path);

    // Step 1: Check GCS for the sealed segment
    String gcsBucket = hadoopConf.get(
        QuorumWalFileSystem.CONFIG_GCS_BUCKET, QuorumWalFileSystem.DEFAULT_GCS_BUCKET);
    String gcsPrefix = hadoopConf.get(
        QuorumWalFileSystem.CONFIG_GCS_PREFIX, QuorumWalFileSystem.DEFAULT_GCS_PREFIX);
    String gcsPath = toGcsPath(walPath, gcsBucket, gcsPrefix);

    try {
      FileSystem gcsFs = FileSystem.get(URI.create("gs://" + gcsBucket), hadoopConf);
      Path gcsFilePath = new Path(gcsPath);
      if (gcsFs.exists(gcsFilePath)) {
        log.debug("WAL segment {} already sealed in GCS at {}, ready for sort", path, gcsPath);
        return 0;
      }
    } catch (IOException e) {
      log.debug("GCS check for {} failed: {}", gcsPath, e.getMessage());
    }

    // Step 2: Determine which peers to query.
    // Prefer peers from the LogEntry metadata (stored when the WAL was created).
    // Fall back to DNS pattern if no peers are recorded (legacy entries).
    int peerPort = hadoopConf.getInt(
        QuorumWalFileSystem.CONFIG_PEER_PORT, QuorumWalFileSystem.DEFAULT_PEER_PORT);

    List<String> peerAddresses = logEntry.getPeers();
    if (peerAddresses != null && !peerAddresses.isEmpty()) {
      log.info("Using {} peers from log entry for segment {}: {}",
          peerAddresses.size(), uuid, peerAddresses);
    } else {
      // Legacy fallback: derive peers from DNS pattern
      String peerDnsPattern = hadoopConf.get(
          QuorumWalFileSystem.CONFIG_PEER_DNS_PATTERN,
          QuorumWalFileSystem.DEFAULT_PEER_DNS_PATTERN);
      int peerCount = hadoopConf.getInt(
          QuorumWalFileSystem.CONFIG_PEER_COUNT, QuorumWalFileSystem.DEFAULT_PEER_COUNT);

      peerAddresses = new ArrayList<>();
      for (int i = 0; i < peerCount; i++) {
        peerAddresses.add(peerDnsPattern.replace("{}", String.valueOf(i)) + ":" + peerPort);
      }
      log.info("No peers in log entry for segment {}, using DNS pattern: {}",
          uuid, peerAddresses);
    }

    SegmentId segmentId =
        SegmentId.newBuilder().setUuid(uuid).setWalPath(walPath).build();

    // Find the peer with the largest replica
    String bestPeer = null;
    int bestPort = peerPort;
    long bestSize = -1;

    for (String peerAddr : peerAddresses) {
      String hostname;
      int port;
      // Parse "host:port" format from LogEntry peers
      int colonIdx = peerAddr.lastIndexOf(':');
      if (colonIdx > 0) {
        hostname = peerAddr.substring(0, colonIdx);
        try {
          port = Integer.parseInt(peerAddr.substring(colonIdx + 1));
        } catch (NumberFormatException e) {
          hostname = peerAddr;
          port = peerPort;
        }
      } else {
        hostname = peerAddr;
        port = peerPort;
      }

      ManagedChannel channel = null;
      try {
        channel = ManagedChannelBuilder.forAddress(hostname, port)
            .usePlaintext().build();
        WalQuorumPeerGrpc.WalQuorumPeerBlockingStub stub =
            WalQuorumPeerGrpc.newBlockingStub(channel);

        ListSegmentsResponse response = stub.listSegments(
            ListSegmentsRequest.newBuilder().build());

        for (SegmentInfo info : response.getSegmentsList()) {
          if (info.getSegmentId().getUuid().equals(uuid)) {
            log.info("Found segment {} on peer {} (size={}, sealed={})",
                uuid, hostname, info.getSize(), info.getSealed());
            if (info.getSealed()) {
              // Segment is sealed on the peer — the LogSorter can read it via
              // QuorumWalFileSystem.open() which falls back to peer reads.
              // No need to wait for GCS upload.
              log.info("Segment {} is already sealed on peer {}, ready for sort",
                  uuid, hostname);
              return 0;
            }
            if (info.getSize() > bestSize) {
              bestPeer = hostname;
              bestPort = port;
              bestSize = info.getSize();
            }
            break;
          }
        }
      } catch (StatusRuntimeException e) {
        log.warn("Failed to query peer {}:{}: {}", hostname, port, e.getMessage());
      } finally {
        if (channel != null) {
          shutdownChannel(channel);
        }
      }
    }

    if (bestPeer == null) {
      log.warn("WAL segment {} not found on any peer sidecar (tried {}), will retry",
          path, peerAddresses);
      return RETRY_DELAY_MS;
    }

    // Step 3: Seal the segment on the best peer
    log.info("Sealing WAL segment {} on peer {}:{} (size={})", uuid, bestPeer, bestPort, bestSize);

    ManagedChannel sealChannel = null;
    try {
      sealChannel = ManagedChannelBuilder.forAddress(bestPeer, bestPort)
          .usePlaintext().build();
      WalQuorumPeerGrpc.WalQuorumPeerBlockingStub sealStub =
          WalQuorumPeerGrpc.newBlockingStub(sealChannel)
              .withDeadlineAfter(60, TimeUnit.SECONDS);

      SealSegmentResponse sealResponse = sealStub.sealSegment(
          SealSegmentRequest.newBuilder()
              .setSegmentId(segmentId)
              .setExpectedSize(bestSize)
              .build());

      if (sealResponse.getSuccess()) {
        log.info("Successfully sealed WAL segment {} on peer {}:{} (actual size={})",
            uuid, bestPeer, bestPort, sealResponse.getActualSize());
        return 0;
      } else {
        log.warn("Seal failed for segment {} on peer {}:{}: {}",
            uuid, bestPeer, bestPort, sealResponse.getError());
        return RETRY_DELAY_MS;
      }
    } catch (StatusRuntimeException e) {
      log.warn("gRPC error sealing segment {} on peer {}:{}: {}",
          uuid, bestPeer, bestPort, e.getMessage());
      return RETRY_DELAY_MS;
    } finally {
      if (sealChannel != null) {
        shutdownChannel(sealChannel);
      }
    }
  }

  /**
   * Converts a WAL path to a GCS object path.
   */
  private static String toGcsPath(String walPath, String bucket, String prefix) {
    String key = walPath.startsWith("/") ? walPath.substring(1) : walPath;
    return "gs://" + bucket + "/" + prefix + key;
  }

  /**
   * Gracefully shuts down a gRPC channel.
   */
  private static void shutdownChannel(ManagedChannel channel) {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
