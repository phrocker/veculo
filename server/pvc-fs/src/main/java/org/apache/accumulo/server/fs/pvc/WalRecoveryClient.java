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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.fs.pvc.proto.FileChunk;
import org.apache.accumulo.server.fs.pvc.proto.PvcFileServiceGrpc;
import org.apache.accumulo.server.fs.pvc.proto.ReadFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Client for reading WAL files from remote tservers via gRPC. Used during WAL recovery when a
 * tablet has migrated to a different tserver and the WAL resides on the old tserver's PVC.
 *
 * <p>
 * Example usage:
 *
 * <pre>
 * try (WalRecoveryClient client = new WalRecoveryClient("tserver-0.tserver-hl", 9701);
 *     InputStream walStream = client.readWal("/accumulo/wal/10.96.0.5+9997/uuid")) {
 *   // read WAL data from walStream
 * }
 * </pre>
 */
public class WalRecoveryClient implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(WalRecoveryClient.class);

  private final ManagedChannel channel;
  private final PvcFileServiceGrpc.PvcFileServiceBlockingStub stub;
  private final String hostname;
  private final int port;

  /**
   * Creates a client connected to the WAL recovery server on the specified tserver.
   *
   * @param hostname the tserver hostname or pod DNS name
   * @param port the gRPC port (typically 9701)
   */
  public WalRecoveryClient(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
    this.channel = ManagedChannelBuilder.forAddress(hostname, port).usePlaintext()
        .keepAliveTime(30, TimeUnit.SECONDS).build();
    this.stub = PvcFileServiceGrpc.newBlockingStub(channel);
    log.debug("WalRecoveryClient created for {}:{}", hostname, port);
  }

  /**
   * Read a WAL file from a remote tserver.
   *
   * @param walPath the WAL path (e.g., {@code /accumulo/wal/10.96.0.5+9997/uuid})
   * @return an InputStream over the WAL file bytes
   */
  public InputStream readWal(String walPath) {
    return readWal(walPath, 0);
  }

  /**
   * Read a WAL file from a remote tserver starting at the given offset.
   *
   * @param walPath the WAL path (e.g., {@code /accumulo/wal/10.96.0.5+9997/uuid})
   * @param offset byte offset to start reading from
   * @return an InputStream over the WAL file bytes
   */
  public InputStream readWal(String walPath, long offset) {
    log.debug("Reading WAL {} from {}:{} at offset {}", walPath, hostname, port, offset);
    Iterator<FileChunk> chunks = stub.readFile(
        ReadFileRequest.newBuilder().setPath(walPath).setOffset(offset).build());
    return new GrpcChunkInputStream(chunks);
  }

  @Override
  public void close() {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
    log.debug("WalRecoveryClient closed for {}:{}", hostname, port);
  }

  /**
   * An {@link InputStream} that reads from a gRPC streaming response of {@link FileChunk} messages.
   * Chunks are consumed lazily from the iterator as bytes are read.
   */
  static class GrpcChunkInputStream extends InputStream {

    private final Iterator<FileChunk> chunks;
    private byte[] currentChunk;
    private int chunkOffset;
    private boolean exhausted;

    GrpcChunkInputStream(Iterator<FileChunk> chunks) {
      this.chunks = chunks;
      this.currentChunk = null;
      this.chunkOffset = 0;
      this.exhausted = false;
    }

    @Override
    public int read() throws IOException {
      if (exhausted) {
        return -1;
      }
      if (!ensureChunkAvailable()) {
        return -1;
      }
      return currentChunk[chunkOffset++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (exhausted) {
        return -1;
      }

      int totalCopied = 0;
      while (totalCopied < len) {
        if (!ensureChunkAvailable()) {
          break;
        }
        int available = currentChunk.length - chunkOffset;
        int toCopy = Math.min(available, len - totalCopied);
        System.arraycopy(currentChunk, chunkOffset, b, off + totalCopied, toCopy);
        chunkOffset += toCopy;
        totalCopied += toCopy;
      }

      return totalCopied == 0 ? -1 : totalCopied;
    }

    /**
     * Ensures that {@code currentChunk} has data available. If the current chunk is exhausted,
     * fetches the next one from the iterator.
     *
     * @return true if data is available, false if the stream is exhausted
     */
    private boolean ensureChunkAvailable() {
      // Current chunk still has data
      if (currentChunk != null && chunkOffset < currentChunk.length) {
        return true;
      }

      // Try to get the next chunk
      while (chunks.hasNext()) {
        FileChunk chunk = chunks.next();
        byte[] data = chunk.getData().toByteArray();
        if (chunk.getLast() && data.length == 0) {
          // Final marker chunk with no data
          exhausted = true;
          return false;
        }
        currentChunk = data;
        chunkOffset = 0;
        if (chunk.getLast()) {
          exhausted = true;
        }
        if (data.length > 0) {
          return true;
        }
      }

      exhausted = true;
      return false;
    }

    @Override
    public void close() throws IOException {
      exhausted = true;
      currentChunk = null;
      super.close();
    }
  }
}
