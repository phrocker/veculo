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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.fs.WalPeerProvider;
import org.apache.accumulo.server.fs.pvc.proto.CloseSegmentRequest;
import org.apache.accumulo.server.fs.pvc.proto.HealthCheckRequest;
import org.apache.accumulo.server.fs.pvc.proto.HealthCheckResponse;
import org.apache.accumulo.server.fs.pvc.proto.ListSegmentsRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListSegmentsResponse;
import org.apache.accumulo.server.fs.pvc.proto.OpenSegmentRequest;
import org.apache.accumulo.server.fs.pvc.proto.OpenSegmentResponse;
import org.apache.accumulo.server.fs.pvc.proto.SegmentId;
import org.apache.accumulo.server.fs.pvc.proto.SegmentInfo;
import org.apache.accumulo.server.fs.pvc.proto.WalQuorumLocalGrpc;
import org.apache.accumulo.server.fs.pvc.proto.WalQuorumPeerGrpc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;

/**
 * Hadoop {@link FileSystem} implementation for the {@code qwal://} URI scheme. Provides quorum-
 * replicated WAL writes via a local sidecar process communicating over Unix domain sockets.
 *
 * <p>
 * URI format: {@code qwal://local/wal/<address>/<uuid>}
 *
 * <p>
 * The write path sends WAL data to the local sidecar over a Unix domain socket. The sidecar
 * replicates entries to peer sidecars for quorum durability, then seals and uploads the segment to
 * GCS on close.
 *
 * <p>
 * The read path (used during WAL recovery) first checks GCS for a sealed segment. If not found, it
 * queries peer sidecars via TCP gRPC to locate the replica with the most data and streams from it.
 *
 * <p>
 * Configuration properties:
 * <ul>
 * <li>{@code fs.qwal.socket.path} - Unix domain socket path to local sidecar (default:
 * /var/run/qwal/qwal.sock)</li>
 * <li>{@code fs.qwal.gcs.bucket} - GCS bucket for sealed WAL segments (default:
 * accumulo-wal)</li>
 * <li>{@code fs.qwal.gcs.prefix} - GCS path prefix within bucket (default: wal/)</li>
 * <li>{@code fs.qwal.peer.port} - TCP port for peer sidecar gRPC (default: 9710)</li>
 * <li>{@code fs.qwal.peer.dns.pattern} - StatefulSet DNS pattern for peer discovery (default:
 * accumulo-tserver-{}.accumulo-tserver-hl)</li>
 * <li>{@code fs.qwal.peer.count} - Number of peer sidecars in the StatefulSet (default: 3)</li>
 * <li>{@code fs.qwal.replication.factor} - Replication factor for quorum writes (default: 3)</li>
 * </ul>
 */
public class QuorumWalFileSystem extends FileSystem implements WalPeerProvider {

  private static final Logger log = LoggerFactory.getLogger(QuorumWalFileSystem.class);

  public static final String SCHEME = "qwal";

  public static final String CONFIG_SOCKET_PATH = "fs.qwal.socket.path";
  public static final String CONFIG_GCS_BUCKET = "fs.qwal.gcs.bucket";
  public static final String CONFIG_GCS_PREFIX = "fs.qwal.gcs.prefix";
  public static final String CONFIG_PEER_PORT = "fs.qwal.peer.port";
  public static final String CONFIG_PEER_DNS_PATTERN = "fs.qwal.peer.dns.pattern";
  public static final String CONFIG_PEER_COUNT = "fs.qwal.peer.count";
  public static final String CONFIG_REPLICATION_FACTOR = "fs.qwal.replication.factor";

  public static final String DEFAULT_SOCKET_PATH = "/var/run/qwal/qwal.sock";
  public static final String DEFAULT_GCS_BUCKET = "accumulo-wal";
  public static final String DEFAULT_GCS_PREFIX = "wal/";
  public static final int DEFAULT_PEER_PORT = 9710;
  public static final String DEFAULT_PEER_DNS_PATTERN =
      "accumulo-tserver-{}.accumulo-tserver-hl";
  public static final int DEFAULT_PEER_COUNT = 3;
  public static final int DEFAULT_REPLICATION_FACTOR = 3;

  private URI uri;
  private Configuration conf;
  private String socketPath;
  private String gcsBucket;
  private String gcsPrefix;
  private int peerPort;
  private String peerDnsPattern;
  private int peerCount;
  private int replicationFactor;
  private Path workingDirectory;

  private ManagedChannel localChannel;
  private EventLoopGroup eventLoopGroup;
  private WalQuorumLocalGrpc.WalQuorumLocalBlockingStub localBlockingStub;
  private WalQuorumLocalGrpc.WalQuorumLocalStub localAsyncStub;

  /** Peers reported by the sidecar for each segment UUID, used by DfsLogger to populate LogEntry */
  private final ConcurrentHashMap<String,List<String>> segmentPeers = new ConcurrentHashMap<>();

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = URI.create(SCHEME + "://local");
    this.conf = conf;
    this.workingDirectory = new Path("/");

    this.socketPath = conf.get(CONFIG_SOCKET_PATH, DEFAULT_SOCKET_PATH);
    this.gcsBucket = conf.get(CONFIG_GCS_BUCKET, DEFAULT_GCS_BUCKET);
    this.gcsPrefix = conf.get(CONFIG_GCS_PREFIX, DEFAULT_GCS_PREFIX);
    this.peerPort = conf.getInt(CONFIG_PEER_PORT, DEFAULT_PEER_PORT);
    this.peerDnsPattern = conf.get(CONFIG_PEER_DNS_PATTERN, DEFAULT_PEER_DNS_PATTERN);
    this.peerCount = conf.getInt(CONFIG_PEER_COUNT, DEFAULT_PEER_COUNT);
    this.replicationFactor = conf.getInt(CONFIG_REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR);

    // Lazy gRPC connection — only connect to sidecar when a WAL write happens.
    // The manager doesn't have a sidecar (no UDS socket), so it runs in
    // pure GCS-delegation mode. The tserver connects on first WAL write.
    java.io.File socketFile = new java.io.File(socketPath);
    if (socketFile.exists()) {
      initSidecarConnection();
      log.info("Initialized QuorumWalFileSystem with sidecar: socket={}, gcs={}/{}, repl={}",
          socketPath, gcsBucket, gcsPrefix, replicationFactor);
    } else {
      log.info("Initialized QuorumWalFileSystem in delegation mode (no sidecar socket at {}). "
          + "All operations delegate to GCS: {}/{}", socketPath, gcsBucket, gcsPrefix);
    }
  }

  private synchronized void initSidecarConnection() {
    if (localChannel != null) {
      return;
    }
    this.eventLoopGroup = new EpollEventLoopGroup(1);
    this.localChannel = NettyChannelBuilder
        .forAddress(new DomainSocketAddress(socketPath))
        .eventLoopGroup(eventLoopGroup)
        .channelType(EpollDomainSocketChannel.class)
        .usePlaintext()
        .build();
    this.localBlockingStub = WalQuorumLocalGrpc.newBlockingStub(localChannel);
    this.localAsyncStub = WalQuorumLocalGrpc.newStub(localChannel);
  }

  private boolean hasSidecar() {
    return localChannel != null;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    Path absPath = makeQualified(f);
    String walPath = absPath.toUri().getPath();

    // Non-WAL paths or no sidecar available — delegate to GCS.
    // Only actual WAL writes (/wal/...) on tserver pods go through the sidecar.
    if (!walPath.contains("/wal/") || !hasSidecar()) {
      String gcsBase = conf.get("fs.qwal.gcs.volume", "");
      if (!gcsBase.isEmpty()) {
        Path gcsPath = toGcsMetadataPath(walPath);
        log.debug("Delegating qwal:// create to GCS: {}", gcsPath);
        return getGcsFileSystem().create(gcsPath, permission, overwrite,
            bufferSize, replication, blockSize, progress);
      }
      throw new IOException("No GCS volume configured for qwal:// non-WAL create: " + walPath);
    }

    log.debug("Opening WAL segment for quorum write: {}", walPath);

    // Derive a segment UUID from the path (last component is typically the UUID)
    String uuid = absPath.getName();
    SegmentId segmentId =
        SegmentId.newBuilder().setUuid(uuid).setWalPath(walPath).build();

    // Determine the local pod name for the originator field
    String originatorPod = System.getenv("POD_NAME");
    if (originatorPod == null) {
      originatorPod = System.getenv("HOSTNAME");
    }
    if (originatorPod == null) {
      originatorPod = "unknown";
    }

    try {
      OpenSegmentResponse response = localBlockingStub.openSegment(
          OpenSegmentRequest.newBuilder()
              .setSegmentId(segmentId)
              .setOriginatorPod(originatorPod)
              .setReplicationFactor(replicationFactor)
              .build());

      if (!response.getSuccess()) {
        throw new IOException(
            "Failed to open WAL segment " + walPath + ": " + response.getError());
      }

      List<String> peers = response.getPreparedPeersList();
      log.debug("Opened WAL segment {} with {} peers: {}", walPath, peers.size(), peers);

      // Store peers so DfsLogger can retrieve them for the LogEntry metadata value.
      segmentPeers.put(uuid, List.copyOf(peers));

      QuorumWalOutputStream out =
          new QuorumWalOutputStream(localAsyncStub, localBlockingStub, segmentId);
      return new FSDataOutputStream(out, statistics);

    } catch (StatusRuntimeException e) {
      throw new IOException("gRPC error opening WAL segment " + walPath, e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException("QuorumWalFileSystem does not support append");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // WALs are not renamed through this filesystem
    log.debug("Rename not supported for qwal:// paths: {} -> {}", src, dst);
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path absPath = makeQualified(f);
    String walPath = absPath.toUri().getPath();

    log.debug("Delete requested for qwal path: {} (recursive={})", walPath, recursive);

    // Recovery paths delegate entirely to GCS
    if (isMetadataPath(walPath)) {
      String gcsBase = conf.get("fs.qwal.gcs.volume", "");
      if (!gcsBase.isEmpty()) {
        return getGcsFileSystem().delete(toGcsMetadataPath(walPath), recursive);
      }
      return false;
    }

    String uuid = absPath.getName();

    // Delegate to sidecar to close/purge the segment if it exists
    if (hasSidecar()) {
      try {
        SegmentId segmentId =
            SegmentId.newBuilder().setUuid(uuid).setWalPath(walPath).build();
        localBlockingStub.closeSegment(
            CloseSegmentRequest.newBuilder().setSegmentId(segmentId).build());
        return true;
      } catch (StatusRuntimeException e) {
        log.debug("Sidecar close/delete for {} failed (may already be closed): {}",
            walPath, e.getMessage());
      }
    }

    // Also try to delete from GCS
    try {
      String gcsPath = toGcsPath(walPath);
      FileSystem gcsFs = getGcsFileSystem();
      return gcsFs.delete(new Path(gcsPath), recursive);
    } catch (IOException e) {
      log.debug("GCS delete for {} failed: {}", walPath, e.getMessage());
      return false;
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    // Metadata paths — delegate to GCS
    if (isMetadataPath(pathStr)) {
      try {
        String gcsBase = conf.get("fs.qwal.gcs.volume", "");
        if (!gcsBase.isEmpty()) {
          FileSystem gcsFs = getGcsFileSystem();
          Path gcsPath = toGcsMetadataPath(pathStr);
          FileStatus[] gcsResults = gcsFs.listStatus(gcsPath);
          // Remap paths back to qwal:// scheme
          FileStatus[] remapped = new FileStatus[gcsResults.length];
          for (int i = 0; i < gcsResults.length; i++) {
            FileStatus gs = gcsResults[i];
            String relativeName = gs.getPath().getName();
            Path qwalPath = new Path(absPath, relativeName);
            remapped[i] = new FileStatus(gs.getLen(), gs.isDirectory(), replicationFactor,
                gs.getBlockSize(), gs.getModificationTime(), qwalPath);
          }
          return remapped;
        }
      } catch (IOException e) {
        log.debug("GCS listStatus failed for {}: {}", pathStr, e.getMessage());
      }
    }

    return new FileStatus[0];
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    this.workingDirectory = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  /**
   * Check if a path is an Accumulo metadata path that should be delegated to GCS.
   * These paths contain cluster identity and structure, not WAL data.
   */
  private boolean isMetadataPath(String pathStr) {
    return pathStr.equals("/") || pathStr.equals("/instance_id")
        || pathStr.endsWith("/instance_id") || pathStr.equals("/version")
        || pathStr.endsWith("/version") || pathStr.endsWith("/tables")
        || pathStr.endsWith("/recovery") || pathStr.contains("/recovery/")
        || pathStr.startsWith("/tables/");
  }

  private Path toGcsMetadataPath(String pathStr) {
    String gcsBase = conf.get("fs.qwal.gcs.volume", "");
    // Strip the qwal volume base path (/accumulo) to avoid doubling it.
    // qwal://local/accumulo/version/12 → pathStr=/accumulo/version/12
    // gcsBase=gs://bucket/tenants/xxx/accumulo
    // Result should be gs://bucket/tenants/xxx/accumulo/version/12 (not .../accumulo/accumulo/...)
    String qwalBase = uri.getPath(); // e.g., "/accumulo"
    if (qwalBase != null && !qwalBase.isEmpty() && pathStr.startsWith(qwalBase)) {
      pathStr = pathStr.substring(qwalBase.length());
    }
    return new Path(gcsBase + pathStr);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    String pathStr = makeQualified(f).toUri().getPath();
    if (isMetadataPath(pathStr)) {
      String gcsBase = conf.get("fs.qwal.gcs.volume", "");
      if (!gcsBase.isEmpty()) {
        return getGcsFileSystem().mkdirs(toGcsMetadataPath(pathStr), permission);
      }
    }
    return true;
  }

  @Override
  public boolean exists(Path f) throws IOException {
    String pathStr = makeQualified(f).toUri().getPath();
    if (isMetadataPath(pathStr)) {
      String gcsBase = conf.get("fs.qwal.gcs.volume", "");
      if (!gcsBase.isEmpty()) {
        try {
          return getGcsFileSystem().exists(toGcsMetadataPath(pathStr));
        } catch (IOException e) {
          log.debug("GCS exists check failed for {}: {}", pathStr, e.getMessage());
        }
      }
      return false;
    }
    try {
      getFileStatus(f);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    // Metadata paths — delegate to GCS
    if (isMetadataPath(pathStr)) {
      String gcsBase = conf.get("fs.qwal.gcs.volume", "");
      if (gcsBase.isEmpty()) {
        throw new IOException("No GCS volume configured for qwal:// metadata delegation");
      }
      Path gcsPath = toGcsMetadataPath(pathStr);
      log.debug("Delegating qwal:// metadata read to GCS: {}", gcsPath);
      return getGcsFileSystem().open(gcsPath, bufferSize);
    }

    return openRecoveryRead(absPath, bufferSize);
  }

  private FSDataInputStream openRecoveryRead(Path absPath, int bufferSize) throws IOException {
    // ... existing recovery read logic from the original open() method
    String walPath = absPath.toUri().getPath();
    // Try GCS first, then peers
    try {
      String gcsPath = toGcsPath(walPath);
      FileSystem gcsFs = getGcsFileSystem();
      Path gcsFilePath = new Path(gcsPath);
      if (gcsFs.exists(gcsFilePath)) {
        return gcsFs.open(gcsFilePath, bufferSize);
      }
    } catch (IOException e) {
      log.debug("GCS open failed for {}: {}", walPath, e.getMessage());
    }

    // Fall back to peer sidecars for unsealed segments
    String uuid = absPath.getName();
    SegmentId segmentId = SegmentId.newBuilder()
        .setUuid(uuid).setWalPath(absPath.toUri().getPath()).build();
    QuorumWalRecoveryReader reader = new QuorumWalRecoveryReader(
        getPeerHostnames(), peerPort, segmentId);
    return new FSDataInputStream(reader);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absPath = makeQualified(f);
    String walPath = absPath.toUri().getPath();

    // Metadata paths — delegate to GCS
    if (isMetadataPath(walPath)) {
      String gcsBase = conf.get("fs.qwal.gcs.volume", "");
      if (!gcsBase.isEmpty()) {
        try {
          FileStatus gcsStatus = getGcsFileSystem().getFileStatus(toGcsMetadataPath(walPath));
          return new FileStatus(gcsStatus.getLen(), gcsStatus.isDirectory(), replicationFactor,
              gcsStatus.getBlockSize(), gcsStatus.getModificationTime(), absPath);
        } catch (IOException e) {
          log.debug("GCS metadata check failed for {}: {}", walPath, e.getMessage());
        }
      }
      throw new java.io.FileNotFoundException("Metadata not found at " + absPath);
    }

    // Check 1: Ask local sidecar via health check (if available)
    if (hasSidecar()) {
      try {
        HealthCheckResponse health =
            localBlockingStub.healthCheck(HealthCheckRequest.getDefaultInstance());
        if (health.getStatus() == HealthCheckResponse.Status.SERVING) {
          return new FileStatus(0, false, replicationFactor, 64 * 1024 * 1024,
              System.currentTimeMillis(), absPath);
        }
      } catch (StatusRuntimeException e) {
        log.debug("Local sidecar health check failed: {}", e.getMessage());
      }
    }

    // Check 2: Try GCS for sealed segment
    try {
      String gcsPath = toGcsPath(walPath);
      FileSystem gcsFs = getGcsFileSystem();
      Path gcsFilePath = new Path(gcsPath);
      if (gcsFs.exists(gcsFilePath)) {
        FileStatus gcsStatus = gcsFs.getFileStatus(gcsFilePath);
        return new FileStatus(gcsStatus.getLen(), false, replicationFactor,
            gcsStatus.getBlockSize(), gcsStatus.getModificationTime(), absPath);
      }
    } catch (IOException e) {
      log.debug("GCS status check failed for {}: {}", walPath, e.getMessage());
    }

    // Check 3: Query peers
    String uuid = absPath.getName();
    SegmentId segmentId =
        SegmentId.newBuilder().setUuid(uuid).setWalPath(walPath).build();
    List<String> peers = getPeerHostnames();

    for (String peer : peers) {
      ManagedChannel peerChannel = null;
      try {
        peerChannel = ManagedChannelBuilder.forAddress(peer, peerPort)
            .usePlaintext().build();
        WalQuorumPeerGrpc.WalQuorumPeerBlockingStub peerStub =
            WalQuorumPeerGrpc.newBlockingStub(peerChannel);
        ListSegmentsResponse listResponse = peerStub.listSegments(
            ListSegmentsRequest.newBuilder().build());

        for (SegmentInfo info : listResponse.getSegmentsList()) {
          if (info.getSegmentId().getUuid().equals(uuid)) {
            return new FileStatus(info.getSize(), false, replicationFactor,
                64 * 1024 * 1024, info.getCreatedAt(), absPath);
          }
        }
      } catch (StatusRuntimeException e) {
        log.debug("Peer {} status check failed: {}", peer, e.getMessage());
      } finally {
        if (peerChannel != null) {
          peerChannel.shutdown();
        }
      }
    }

    throw new FileNotFoundException("WAL segment not found: " + absPath);
  }

  @Override
  public List<String> getSegmentPeers(String segmentUuid) {
    return segmentPeers.getOrDefault(segmentUuid, Collections.emptyList());
  }

  @Override
  public void close() throws IOException {
    if (localChannel != null) {
      try {
        localChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Interrupted while shutting down local gRPC channel", e);
      }
    }
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    }
    super.close();
  }

  /**
   * Converts a WAL path to a GCS object path. WAL paths are like
   * {@code /accumulo/wal/<address>/<uuid>}; the GCS path is
   * {@code gs://<bucket>/<prefix><address>/<uuid>}.
   */
  String toGcsPath(String walPath) {
    // Strip leading slash for GCS key construction
    String key = walPath.startsWith("/") ? walPath.substring(1) : walPath;
    return "gs://" + gcsBucket + "/" + gcsPrefix + key;
  }

  /**
   * Returns a Hadoop FileSystem for GCS, obtained from the configuration. The GCS connector
   * ({@code com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem}) must be on the classpath.
   */
  private FileSystem getGcsFileSystem() throws IOException {
    return FileSystem.get(URI.create("gs://" + gcsBucket), conf);
  }

  /**
   * Derives peer sidecar hostnames from the StatefulSet DNS pattern. The pattern uses {@code {}} as
   * a placeholder for the ordinal index (e.g.,
   * {@code accumulo-tserver-{}.accumulo-tserver-hl} produces
   * {@code accumulo-tserver-0.accumulo-tserver-hl}, etc.).
   */
  List<String> getPeerHostnames() {
    List<String> peers = new ArrayList<>();
    for (int i = 0; i < peerCount; i++) {
      peers.add(peerDnsPattern.replace("{}", String.valueOf(i)));
    }
    return peers;
  }
}
