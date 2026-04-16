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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.fs.pvc.proto.FileChunk;
import org.apache.accumulo.server.fs.pvc.proto.PvcFileServiceGrpc;
import org.apache.accumulo.server.fs.pvc.proto.ReadFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Lightweight gRPC server that serves WAL files from the local PVC to other tservers that need them
 * for recovery. Runs on each tserver pod (default port 9701).
 *
 * <p>
 * Only serves files under the WAL directory ({@code <localRoot>/accumulo/wal/}). Requests for any
 * other path are rejected with {@code PERMISSION_DENIED} for security.
 *
 * <p>
 * This is the minimal gRPC component needed for Option B of the tiered storage architecture: WALs
 * are written to local PVC and this service enables cross-pod WAL reads during tablet migration
 * recovery.
 */
public class WalRecoveryServer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(WalRecoveryServer.class);

  /** Default chunk size for streaming WAL data: 1 MB. */
  static final int CHUNK_SIZE = 1024 * 1024;

  private final Server server;
  private final String localRoot;
  private final Path walDir;
  private final int port;

  /**
   * Creates a WAL recovery server.
   *
   * @param localRoot the PVC mount point (e.g., {@code /mnt/data}). WALs are expected under
   *        {@code <localRoot>/accumulo/wal/}.
   * @param port the gRPC listen port
   */
  public WalRecoveryServer(String localRoot, int port) {
    this.localRoot = localRoot;
    this.walDir = Path.of(localRoot, "accumulo", "wal");
    this.port = port;
    this.server = ServerBuilder.forPort(port).addService(new WalFileServiceImpl()).build();
  }

  public void start() throws IOException {
    server.start();
    log.info("WalRecoveryServer started on port {} serving WALs from {}", port, walDir);
  }

  @Override
  public void close() {
    server.shutdown();
    try {
      if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
        server.shutdownNow();
      }
    } catch (InterruptedException e) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    }
    log.info("WalRecoveryServer stopped");
  }

  public int getPort() {
    return port;
  }

  /**
   * Returns {@code true} if the gRPC server is running and not shut down.
   */
  public boolean isHealthy() {
    return server != null && !server.isShutdown();
  }

  /**
   * Resolves a WAL request path to a local file, validating that it falls under the WAL directory
   * and does not contain directory traversal sequences.
   *
   * @param requestPath the path from the gRPC request (e.g.,
   *        {@code /accumulo/wal/10.96.0.5+9997/uuid})
   * @return the resolved local file
   * @throws IllegalArgumentException if the path contains ".." or does not resolve under the WAL
   *         directory
   */
  File resolveWalPath(String requestPath) {
    if (requestPath.contains("..")) {
      throw new IllegalArgumentException("Path must not contain '..': " + requestPath);
    }
    // Request paths are like /accumulo/wal/address/uuid — resolve against localRoot
    String cleaned = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath;
    Path resolved = Path.of(localRoot, cleaned).normalize();

    // Security: ensure the resolved path is under the WAL directory
    if (!resolved.startsWith(walDir)) {
      throw new IllegalArgumentException(
          "Path does not resolve under WAL directory: " + requestPath);
    }
    return resolved.toFile();
  }

  /**
   * gRPC service implementation that only handles {@code ReadFile} for WAL recovery. All other RPCs
   * inherited from {@link PvcFileServiceGrpc.PvcFileServiceImplBase} return {@code UNIMPLEMENTED}.
   */
  private class WalFileServiceImpl extends PvcFileServiceGrpc.PvcFileServiceImplBase {

    @Override
    public void readFile(ReadFileRequest request,
        StreamObserver<FileChunk> responseObserver) {
      String requestedPath = request.getPath();

      // Validate and resolve the path
      File file;
      try {
        file = resolveWalPath(requestedPath);
      } catch (IllegalArgumentException e) {
        responseObserver.onError(Status.PERMISSION_DENIED
            .withDescription("Only WAL files can be served for recovery: " + e.getMessage())
            .asRuntimeException());
        return;
      }

      if (!file.exists() || !file.isFile()) {
        responseObserver.onError(Status.NOT_FOUND
            .withDescription("WAL file not found: " + requestedPath).asRuntimeException());
        return;
      }

      // Stream the WAL file in chunks
      try (InputStream in = Files.newInputStream(file.toPath())) {
        long offset = request.getOffset();
        long length = request.getLength();
        if (offset > 0) {
          long skipped = in.skip(offset);
          if (skipped < offset) {
            responseObserver.onError(Status.OUT_OF_RANGE
                .withDescription("Offset beyond file length").asRuntimeException());
            return;
          }
        }

        byte[] buffer = new byte[CHUNK_SIZE];
        long totalRead = 0;
        int bytesRead;

        while ((bytesRead = in.read(buffer)) != -1) {
          if (length > 0 && totalRead + bytesRead > length) {
            bytesRead = (int) (length - totalRead);
          }
          boolean isLast =
              (length > 0 && totalRead + bytesRead >= length) || bytesRead < buffer.length;
          responseObserver.onNext(FileChunk.newBuilder()
              .setData(ByteString.copyFrom(buffer, 0, bytesRead))
              .setOffset(offset + totalRead).setLast(isLast).build());
          totalRead += bytesRead;
          if (isLast) {
            break;
          }
        }

        // If the file was empty or fully read at offset, send an empty last chunk
        if (totalRead == 0) {
          responseObserver.onNext(FileChunk.newBuilder().setData(ByteString.EMPTY)
              .setOffset(offset).setLast(true).build());
        }

        responseObserver.onCompleted();
        log.debug("Served WAL {} ({} bytes from offset {})", requestedPath, totalRead, offset);

      } catch (IOException e) {
        log.error("Error reading WAL file {}", requestedPath, e);
        responseObserver.onError(Status.INTERNAL
            .withDescription("Error reading WAL: " + e.getMessage()).asRuntimeException());
      }
    }
  }
}
