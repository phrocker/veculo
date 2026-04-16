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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.accumulo.server.fs.pvc.proto.DeleteFileRequest;
import org.apache.accumulo.server.fs.pvc.proto.DeleteFileResponse;
import org.apache.accumulo.server.fs.pvc.proto.FileChunk;
import org.apache.accumulo.server.fs.pvc.proto.FileStatusProto;
import org.apache.accumulo.server.fs.pvc.proto.FileStatusResponse;
import org.apache.accumulo.server.fs.pvc.proto.GetFileStatusRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListFilesRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListFilesResponse;
import org.apache.accumulo.server.fs.pvc.proto.MkdirsRequest;
import org.apache.accumulo.server.fs.pvc.proto.MkdirsResponse;
import org.apache.accumulo.server.fs.pvc.proto.PvcFileServiceGrpc;
import org.apache.accumulo.server.fs.pvc.proto.ReadFileRequest;
import org.apache.accumulo.server.fs.pvc.proto.RenameRequest;
import org.apache.accumulo.server.fs.pvc.proto.RenameResponse;
import org.apache.accumulo.server.fs.pvc.proto.WriteFileRequest;
import org.apache.accumulo.server.fs.pvc.proto.WriteFileResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * gRPC server that runs on each tablet server, serving file operations against the local PVC
 * volume. This enables remote tablet servers to access files stored on another tserver's PVC through
 * the {@code pvc://} filesystem scheme.
 */
public class PvcFileServer {

  private static final Logger log = LoggerFactory.getLogger(PvcFileServer.class);

  public static final int DEFAULT_PORT = 9701;
  public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB

  private final Server server;
  private final String localRoot;
  private final int port;
  private final PvcFileServiceImpl serviceImpl;

  // ─── Metrics counters ─────────────────────────────────────────────────────
  private final AtomicLong bytesReadTotal = new AtomicLong();
  private final AtomicLong bytesWrittenTotal = new AtomicLong();
  private final AtomicLong requestsReadFile = new AtomicLong();
  private final AtomicLong requestsWriteFile = new AtomicLong();
  private final AtomicLong requestsDeleteFile = new AtomicLong();
  private final AtomicLong requestsListFiles = new AtomicLong();
  private final AtomicLong requestsGetFileStatus = new AtomicLong();
  private final AtomicLong requestsRename = new AtomicLong();
  private final AtomicLong requestsMkdirs = new AtomicLong();
  private final AtomicLong errorsTotal = new AtomicLong();
  private final AtomicLong readLatencyTotalMs = new AtomicLong();
  private final AtomicLong readLatencyCount = new AtomicLong();

  public PvcFileServer(String localRoot, int port) {
    this.localRoot = localRoot;
    this.port = port;
    this.serviceImpl = new PvcFileServiceImpl(localRoot, this);
    this.server = ServerBuilder.forPort(port).addService(serviceImpl).build();
  }

  public void start() throws IOException {
    server.start();
    log.info("PvcFileServer started on port {} serving root {}", port, localRoot);
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
      log.info("PvcFileServer stopped");
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
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
   * Returns a snapshot of server-side metrics for monitoring and the admin dashboard.
   */
  public Map<String,Object> getMetrics() {
    Map<String,Object> metrics = new LinkedHashMap<>();
    metrics.put("pvc_bytes_read_total", bytesReadTotal.get());
    metrics.put("pvc_bytes_written_total", bytesWrittenTotal.get());
    metrics.put("pvc_requests_read_file", requestsReadFile.get());
    metrics.put("pvc_requests_write_file", requestsWriteFile.get());
    metrics.put("pvc_requests_delete_file", requestsDeleteFile.get());
    metrics.put("pvc_requests_list_files", requestsListFiles.get());
    metrics.put("pvc_requests_get_file_status", requestsGetFileStatus.get());
    metrics.put("pvc_requests_rename", requestsRename.get());
    metrics.put("pvc_requests_mkdirs", requestsMkdirs.get());
    metrics.put("pvc_requests_total",
        requestsReadFile.get() + requestsWriteFile.get() + requestsDeleteFile.get()
            + requestsListFiles.get() + requestsGetFileStatus.get() + requestsRename.get()
            + requestsMkdirs.get());
    metrics.put("pvc_errors_total", errorsTotal.get());
    long count = readLatencyCount.get();
    metrics.put("pvc_read_latency_avg_ms", count > 0 ? readLatencyTotalMs.get() / count : 0);
    metrics.put("pvc_read_latency_count", count);
    metrics.put("healthy", isHealthy());
    return metrics;
  }

  /**
   * Resolves a requested path against the local root, preventing directory traversal attacks.
   *
   * @throws IllegalArgumentException if the path contains ".."
   */
  static File resolveAndValidate(String localRoot, String requestPath) {
    if (requestPath.contains("..")) {
      throw new IllegalArgumentException(
          "Path must not contain '..': " + requestPath);
    }
    String cleaned = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath;
    return java.nio.file.Path.of(localRoot, cleaned).toFile();
  }

  static class PvcFileServiceImpl extends PvcFileServiceGrpc.PvcFileServiceImplBase {

    private final String localRoot;
    private final PvcFileServer server;

    PvcFileServiceImpl(String localRoot, PvcFileServer server) {
      this.localRoot = localRoot;
      this.server = server;
    }

    @Override
    public void readFile(ReadFileRequest request,
        StreamObserver<FileChunk> responseObserver) {
      server.requestsReadFile.incrementAndGet();
      long startTime = System.currentTimeMillis();
      File file;
      try {
        file = resolveAndValidate(localRoot, request.getPath());
      } catch (IllegalArgumentException e) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        return;
      }

      if (!file.exists() || !file.isFile()) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("File not found: " + request.getPath())
                .asRuntimeException());
        return;
      }

      try (java.io.InputStream fis = java.nio.file.Files.newInputStream(file.toPath())) {
        long offset = request.getOffset();
        long length = request.getLength();
        if (offset > 0) {
          long skipped = fis.skip(offset);
          if (skipped < offset) {
            server.errorsTotal.incrementAndGet();
            responseObserver.onError(
                Status.OUT_OF_RANGE
                    .withDescription("Offset beyond file length")
                    .asRuntimeException());
            return;
          }
        }

        byte[] buffer = new byte[DEFAULT_CHUNK_SIZE];
        long totalRead = 0;
        int bytesRead;

        while ((bytesRead = fis.read(buffer)) != -1) {
          if (length > 0 && totalRead + bytesRead > length) {
            bytesRead = (int) (length - totalRead);
          }
          boolean isLast =
              (length > 0 && totalRead + bytesRead >= length) || bytesRead < buffer.length;
          FileChunk chunk = FileChunk.newBuilder()
              .setData(ByteString.copyFrom(buffer, 0, bytesRead))
              .setOffset(offset + totalRead).setLast(isLast).build();
          responseObserver.onNext(chunk);
          totalRead += bytesRead;
          if (isLast) {
            break;
          }
        }

        // If we read nothing at all, send an empty last chunk
        if (totalRead == 0) {
          responseObserver.onNext(
              FileChunk.newBuilder().setData(ByteString.EMPTY).setOffset(offset).setLast(true)
                  .build());
        }

        server.bytesReadTotal.addAndGet(totalRead);
        server.readLatencyTotalMs.addAndGet(System.currentTimeMillis() - startTime);
        server.readLatencyCount.incrementAndGet();
        responseObserver.onCompleted();
      } catch (IOException e) {
        server.errorsTotal.incrementAndGet();
        log.error("Error reading file {}", request.getPath(), e);
        responseObserver.onError(
            Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
      }
    }

    @Override
    public StreamObserver<WriteFileRequest> writeFile(
        StreamObserver<WriteFileResponse> responseObserver) {
      server.requestsWriteFile.incrementAndGet();
      return new StreamObserver<>() {
        private OutputStream outputStream;
        private long bytesWritten = 0;

        @Override
        public void onNext(WriteFileRequest request) {
          try {
            if (request.hasHeader()) {
              var header = request.getHeader();
              File file = resolveAndValidate(localRoot, header.getPath());
              if (file.exists() && !header.getOverwrite()) {
                server.errorsTotal.incrementAndGet();
                responseObserver.onNext(WriteFileResponse.newBuilder()
                    .setSuccess(false).setError("File already exists").build());
                responseObserver.onCompleted();
                return;
              }
              File parent = file.getParentFile();
              if (parent != null && !parent.exists()) {
                parent.mkdirs();
              }
              outputStream = java.nio.file.Files.newOutputStream(file.toPath());
              log.debug("Opened file for writing: {}", header.getPath());
            } else if (request.hasChunk()) {
              if (outputStream == null) {
                server.errorsTotal.incrementAndGet();
                responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Header must be sent before chunks")
                    .asRuntimeException());
                return;
              }
              var chunk = request.getChunk();
              byte[] data = chunk.getData().toByteArray();
              outputStream.write(data);
              bytesWritten += data.length;
              if (chunk.getLast()) {
                outputStream.flush();
              }
            }
          } catch (IllegalArgumentException e) {
            server.errorsTotal.incrementAndGet();
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
          } catch (IOException e) {
            server.errorsTotal.incrementAndGet();
            log.error("Error writing file", e);
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
          }
        }

        @Override
        public void onError(Throwable t) {
          server.errorsTotal.incrementAndGet();
          log.error("Write stream error", t);
          closeOutputStream();
        }

        @Override
        public void onCompleted() {
          closeOutputStream();
          server.bytesWrittenTotal.addAndGet(bytesWritten);
          responseObserver.onNext(WriteFileResponse.newBuilder()
              .setSuccess(true).setBytesWritten(bytesWritten).build());
          responseObserver.onCompleted();
        }

        private void closeOutputStream() {
          if (outputStream != null) {
            try {
              outputStream.close();
            } catch (IOException e) {
              log.warn("Error closing output stream", e);
            }
          }
        }
      };
    }

    @Override
    public void deleteFile(DeleteFileRequest request,
        StreamObserver<DeleteFileResponse> responseObserver) {
      server.requestsDeleteFile.incrementAndGet();
      try {
        File file = resolveAndValidate(localRoot, request.getPath());
        boolean success;
        if (request.getRecursive() && file.isDirectory()) {
          success = deleteRecursive(file);
        } else {
          success = file.delete();
        }
        responseObserver
            .onNext(DeleteFileResponse.newBuilder().setSuccess(success).build());
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      }
    }

    private boolean deleteRecursive(File file) {
      if (file.isDirectory()) {
        File[] children = file.listFiles();
        if (children != null) {
          for (File child : children) {
            if (!deleteRecursive(child)) {
              return false;
            }
          }
        }
      }
      return file.delete();
    }

    @Override
    public void listFiles(ListFilesRequest request,
        StreamObserver<ListFilesResponse> responseObserver) {
      server.requestsListFiles.incrementAndGet();
      try {
        File dir = resolveAndValidate(localRoot, request.getPath());
        ListFilesResponse.Builder builder = ListFilesResponse.newBuilder();
        if (dir.exists() && dir.isDirectory()) {
          try (Stream<Path> stream = Files.list(dir.toPath())) {
            stream.forEach(p -> {
              File f = p.toFile();
              builder.addFiles(toFileStatusProto(f, request.getPath()));
            });
          }
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      } catch (IOException e) {
        server.errorsTotal.incrementAndGet();
        log.error("Error listing files at {}", request.getPath(), e);
        responseObserver.onError(
            Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
      }
    }

    @Override
    public void getFileStatus(GetFileStatusRequest request,
        StreamObserver<FileStatusResponse> responseObserver) {
      server.requestsGetFileStatus.incrementAndGet();
      try {
        File file = resolveAndValidate(localRoot, request.getPath());
        if (!file.exists()) {
          responseObserver.onNext(
              FileStatusResponse.newBuilder().setExists(false).build());
        } else {
          responseObserver.onNext(FileStatusResponse.newBuilder()
              .setExists(true)
              .setStatus(toFileStatusProto(file, request.getPath()))
              .build());
        }
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      }
    }

    @Override
    public void rename(RenameRequest request,
        StreamObserver<RenameResponse> responseObserver) {
      server.requestsRename.incrementAndGet();
      try {
        File source = resolveAndValidate(localRoot, request.getSource());
        File dest = resolveAndValidate(localRoot, request.getDestination());
        boolean success;
        try {
          Files.move(source.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
          success = true;
        } catch (IOException e) {
          log.warn("Rename failed from {} to {}", request.getSource(),
              request.getDestination(), e);
          success = false;
        }
        responseObserver
            .onNext(RenameResponse.newBuilder().setSuccess(success).build());
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      }
    }

    @Override
    public void mkdirs(MkdirsRequest request,
        StreamObserver<MkdirsResponse> responseObserver) {
      server.requestsMkdirs.incrementAndGet();
      try {
        File dir = resolveAndValidate(localRoot, request.getPath());
        boolean success = dir.exists() || dir.mkdirs();
        responseObserver
            .onNext(MkdirsResponse.newBuilder().setSuccess(success).build());
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        server.errorsTotal.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      }
    }

    private FileStatusProto toFileStatusProto(File file, String logicalPath) {
      String path = logicalPath.endsWith("/") ? logicalPath + file.getName()
          : logicalPath + "/" + file.getName();
      // If the file itself is the target (not a child listing), use the path directly
      if (file.getName().equals(java.nio.file.Path.of(logicalPath).getFileName().toString())) {
        path = logicalPath;
      }
      return FileStatusProto.newBuilder().setPath(path).setLength(file.length())
          .setIsDirectory(file.isDirectory())
          .setModificationTime(file.lastModified())
          .setAccessTime(file.lastModified())
          .setPermission(0755).build();
    }
  }
}
