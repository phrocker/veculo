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
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.fs.pvc.proto.DeleteFileRequest;
import org.apache.accumulo.server.fs.pvc.proto.DeleteFileResponse;
import org.apache.accumulo.server.fs.pvc.proto.FileStatusProto;
import org.apache.accumulo.server.fs.pvc.proto.FileStatusResponse;
import org.apache.accumulo.server.fs.pvc.proto.GetFileStatusRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListFilesRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListFilesResponse;
import org.apache.accumulo.server.fs.pvc.proto.MkdirsRequest;
import org.apache.accumulo.server.fs.pvc.proto.MkdirsResponse;
import org.apache.accumulo.server.fs.pvc.proto.PvcFileServiceGrpc;
import org.apache.accumulo.server.fs.pvc.proto.RenameRequest;
import org.apache.accumulo.server.fs.pvc.proto.RenameResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * Hadoop {@link FileSystem} implementation for the {@code pvc://} URI scheme.
 *
 * <p>
 * URI format: {@code pvc://<hostname>/<path>}
 *
 * <p>
 * When the target hostname matches the local host, file operations bypass gRPC and use direct local
 * I/O through the Hadoop {@link LocalFileSystem}. For remote hosts, operations are dispatched via
 * gRPC to the {@link PvcFileServer} running on the target tserver.
 *
 * <p>
 * Configuration properties:
 * <ul>
 * <li>{@code fs.pvc.grpc.port} - gRPC server port (default: 9701)</li>
 * <li>{@code fs.pvc.local.root} - Local PVC mount point (default: /data/accumulo)</li>
 * <li>{@code fs.pvc.chunk.size} - Read/write chunk size in bytes (default: 1048576)</li>
 * </ul>
 */
public class PvcFileSystem extends FileSystem {

  private static final Logger log = LoggerFactory.getLogger(PvcFileSystem.class);

  public static final String SCHEME = "pvc";
  public static final String CONFIG_GRPC_PORT = "fs.pvc.grpc.port";
  public static final String CONFIG_LOCAL_ROOT = "fs.pvc.local.root";
  public static final String CONFIG_CHUNK_SIZE = "fs.pvc.chunk.size";

  public static final int DEFAULT_GRPC_PORT = 9701;
  public static final String DEFAULT_LOCAL_ROOT = "/data/accumulo";
  public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

  private URI uri;
  private String authority;
  private int grpcPort;
  private String localRoot;
  private String localHostname;
  private Path workingDirectory;
  private LocalFileSystem localFs;
  private final ConcurrentHashMap<String,ManagedChannel> channelCache =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,CircuitState> circuits = new ConcurrentHashMap<>();

  // ─── Circuit Breaker ────────────────────────────────────────────────────────

  /**
   * Per-host circuit breaker state. When a gRPC call fails {@code FAILURE_THRESHOLD} times
   * consecutively, the circuit opens and all requests to that host fail fast with an
   * {@link IOException} for {@code COOLDOWN_MS} milliseconds. After cooldown, one probe request is
   * allowed through (half-open state).
   */
  static class CircuitState {
    volatile int consecutiveFailures = 0;
    volatile long lastFailureTime = 0;
    volatile boolean open = false;

    static final int FAILURE_THRESHOLD = 3;
    static final long COOLDOWN_MS = 30_000;

    void recordFailure() {
      consecutiveFailures++;
      lastFailureTime = System.currentTimeMillis();
      if (consecutiveFailures >= FAILURE_THRESHOLD) {
        open = true;
      }
    }

    void recordSuccess() {
      consecutiveFailures = 0;
      open = false;
    }

    boolean isOpen() {
      if (open && System.currentTimeMillis() - lastFailureTime > COOLDOWN_MS) {
        // Half-open: allow one retry after cooldown
        open = false;
        consecutiveFailures = 0;
      }
      return open;
    }
  }

  private void checkCircuit(String hostname) throws IOException {
    CircuitState state = circuits.computeIfAbsent(hostname, k -> new CircuitState());
    if (state.isOpen()) {
      throw new IOException(
          "PVC file server at " + hostname + " is unreachable (circuit breaker open)");
    }
  }

  private void recordSuccess(String hostname) {
    circuits.computeIfAbsent(hostname, k -> new CircuitState()).recordSuccess();
  }

  private void recordFailure(String hostname) {
    circuits.computeIfAbsent(hostname, k -> new CircuitState()).recordFailure();
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = URI.create(SCHEME + "://" + name.getAuthority());
    this.authority = name.getAuthority();
    this.grpcPort = conf.getInt(CONFIG_GRPC_PORT, DEFAULT_GRPC_PORT);
    this.localRoot = conf.get(CONFIG_LOCAL_ROOT, DEFAULT_LOCAL_ROOT);
    this.workingDirectory = new Path("/");

    try {
      this.localHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      this.localHostname = "";
      log.warn("Could not determine local hostname", e);
    }

    this.localFs = FileSystem.getLocal(conf);
    log.info("Initialized PvcFileSystem for authority={}, localRoot={}, grpcPort={}",
        authority, localRoot, grpcPort);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Determines whether the given URI authority refers to the local host, enabling direct local I/O.
   */
  private boolean isLocal() {
    return localHostname.startsWith(authority) || authority.startsWith(localHostname);
  }

  private Path toLocalPath(Path path) {
    return new Path(localRoot + path.toUri().getPath());
  }

  private ManagedChannel getChannel(String hostname) {
    return channelCache.computeIfAbsent(hostname,
        h -> ManagedChannelBuilder.forAddress(h, grpcPort).usePlaintext()
            .keepAliveTime(30, TimeUnit.SECONDS)
            .idleTimeout(5, TimeUnit.MINUTES).build());
  }

  private PvcFileServiceGrpc.PvcFileServiceBlockingStub getBlockingStub() {
    return PvcFileServiceGrpc.newBlockingStub(getChannel(authority));
  }

  private PvcFileServiceGrpc.PvcFileServiceStub getAsyncStub() {
    return PvcFileServiceGrpc.newStub(getChannel(authority));
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    if (isLocal()) {
      log.debug("Local open: {}", pathStr);
      return localFs.open(toLocalPath(absPath), bufferSize);
    }

    log.debug("Remote open: {} on {}", pathStr, authority);
    checkCircuit(authority);
    try {
      FSDataInputStream in =
          new FSDataInputStream(new PvcInputStream(getBlockingStub(), pathStr));
      recordSuccess(authority);
      return in;
    } catch (StatusRuntimeException e) {
      recordFailure(authority);
      throw new IOException("PVC open failed for " + authority + ": " + pathStr, e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    if (isLocal()) {
      log.debug("Local create: {}", pathStr);
      return localFs.create(toLocalPath(absPath), permission, overwrite, bufferSize, replication,
          blockSize, progress);
    }

    log.debug("Remote create: {} on {}", pathStr, authority);
    try {
      checkCircuit(authority);
      PvcOutputStream out =
          new PvcOutputStream(getAsyncStub(), pathStr, overwrite, true, bufferSize, blockSize);
      recordSuccess(authority);
      return new FSDataOutputStream(out, statistics);
    } catch (StatusRuntimeException e) {
      recordFailure(authority);
      throw new IOException("PVC create failed for " + authority + ": " + pathStr, e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException("PvcFileSystem does not support append");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absSrc = makeQualified(src);
    Path absDst = makeQualified(dst);
    String srcPath = absSrc.toUri().getPath();
    String dstPath = absDst.toUri().getPath();

    if (isLocal()) {
      log.debug("Local rename: {} -> {}", srcPath, dstPath);
      return localFs.rename(toLocalPath(absSrc), toLocalPath(absDst));
    }

    log.debug("Remote rename: {} -> {} on {}", srcPath, dstPath, authority);
    try {
      checkCircuit(authority);
      RenameResponse response = getBlockingStub()
          .rename(RenameRequest.newBuilder().setSource(srcPath).setDestination(dstPath).build());
      recordSuccess(authority);
      return response.getSuccess();
    } catch (StatusRuntimeException e) {
      recordFailure(authority);
      throw new IOException("PVC rename failed for " + authority, e);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    if (isLocal()) {
      log.debug("Local delete: {}", pathStr);
      return localFs.delete(toLocalPath(absPath), recursive);
    }

    log.debug("Remote delete: {} on {}", pathStr, authority);
    try {
      checkCircuit(authority);
      DeleteFileResponse response = getBlockingStub().deleteFile(
          DeleteFileRequest.newBuilder().setPath(pathStr).setRecursive(recursive).build());
      recordSuccess(authority);
      return response.getSuccess();
    } catch (StatusRuntimeException e) {
      recordFailure(authority);
      throw new IOException("PVC delete failed for " + authority + ": " + pathStr, e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    if (isLocal()) {
      log.debug("Local listStatus: {}", pathStr);
      FileStatus[] localStatuses = localFs.listStatus(toLocalPath(absPath));
      // Rewrite paths from local filesystem paths back to pvc:// paths
      FileStatus[] result = new FileStatus[localStatuses.length];
      for (int i = 0; i < localStatuses.length; i++) {
        FileStatus ls = localStatuses[i];
        String localPathStr = ls.getPath().toUri().getPath();
        String relativePath = localPathStr;
        if (localPathStr.startsWith(localRoot)) {
          relativePath = localPathStr.substring(localRoot.length());
        }
        result[i] = new FileStatus(ls.getLen(), ls.isDirectory(), ls.getReplication(),
            ls.getBlockSize(), ls.getModificationTime(), ls.getAccessTime(),
            ls.getPermission(), ls.getOwner(), ls.getGroup(),
            new Path(uri.toString() + relativePath));
      }
      return result;
    }

    log.debug("Remote listStatus: {} on {}", pathStr, authority);
    try {
      checkCircuit(authority);
      ListFilesResponse response = getBlockingStub()
          .listFiles(ListFilesRequest.newBuilder().setPath(pathStr).build());
      recordSuccess(authority);
      return response.getFilesList().stream().map(this::toFileStatus).toArray(FileStatus[]::new);
    } catch (StatusRuntimeException | IOException e) {
      recordFailure(authority);
      throw new FileNotFoundException(
          "PVC volume unavailable at " + authority + ": " + pathStr);
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    this.workingDirectory = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    if (isLocal()) {
      log.debug("Local mkdirs: {}", pathStr);
      return localFs.mkdirs(toLocalPath(absPath), permission);
    }

    log.debug("Remote mkdirs: {} on {}", pathStr, authority);
    try {
      checkCircuit(authority);
      MkdirsResponse response = getBlockingStub().mkdirs(
          MkdirsRequest.newBuilder().setPath(pathStr).setPermission(permission.toShort()).build());
      recordSuccess(authority);
      return response.getSuccess();
    } catch (StatusRuntimeException e) {
      recordFailure(authority);
      throw new IOException("PVC mkdirs failed for " + authority + ": " + pathStr, e);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absPath = makeQualified(f);
    String pathStr = absPath.toUri().getPath();

    if (isLocal()) {
      log.debug("Local getFileStatus: {}", pathStr);
      FileStatus ls = localFs.getFileStatus(toLocalPath(absPath));
      String localPathStr = ls.getPath().toUri().getPath();
      String relativePath = localPathStr;
      if (localPathStr.startsWith(localRoot)) {
        relativePath = localPathStr.substring(localRoot.length());
      }
      return new FileStatus(ls.getLen(), ls.isDirectory(), ls.getReplication(), ls.getBlockSize(),
          ls.getModificationTime(), ls.getAccessTime(), ls.getPermission(), ls.getOwner(),
          ls.getGroup(), new Path(uri.toString() + relativePath));
    }

    log.debug("Remote getFileStatus: {} on {}", pathStr, authority);
    try {
      checkCircuit(authority);
      FileStatusResponse response = getBlockingStub()
          .getFileStatus(GetFileStatusRequest.newBuilder().setPath(pathStr).build());
      recordSuccess(authority);

      if (!response.getExists()) {
        throw new FileNotFoundException("File not found: " + absPath);
      }

      return toFileStatus(response.getStatus());
    } catch (FileNotFoundException e) {
      throw e;
    } catch (StatusRuntimeException | IOException e) {
      recordFailure(authority);
      // Throw FileNotFoundException so callers treat unavailable pvc:// volumes as
      // "file doesn't exist" rather than a fatal error. This is correct during init
      // (tservers not started) and during tablet migration (temporary unavailability).
      throw new FileNotFoundException(
          "PVC volume unavailable at " + authority + ": " + pathStr);
    }
  }

  private FileStatus toFileStatus(FileStatusProto proto) {
    return new FileStatus(proto.getLength(), proto.getIsDirectory(), 1,
        DEFAULT_CHUNK_SIZE, proto.getModificationTime(), proto.getAccessTime(),
        new FsPermission((short) proto.getPermission()), "", "",
        new Path(uri.toString() + proto.getPath()));
  }

  @Override
  public void close() throws IOException {
    for (ManagedChannel channel : channelCache.values()) {
      try {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Interrupted while shutting down gRPC channel", e);
      }
    }
    channelCache.clear();
    super.close();
  }
}
