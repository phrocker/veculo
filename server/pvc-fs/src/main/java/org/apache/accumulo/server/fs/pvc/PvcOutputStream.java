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
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.server.fs.pvc.proto.FileChunk;
import org.apache.accumulo.server.fs.pvc.proto.PvcFileServiceGrpc;
import org.apache.accumulo.server.fs.pvc.proto.WriteFileHeader;
import org.apache.accumulo.server.fs.pvc.proto.WriteFileRequest;
import org.apache.accumulo.server.fs.pvc.proto.WriteFileResponse;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

/**
 * An {@link OutputStream} that writes data to a remote PVC file server via gRPC client-streaming.
 * Implements {@link Syncable} for WAL compatibility -- {@code hsync()} and {@code hflush()} flush
 * the current buffer immediately.
 */
public class PvcOutputStream extends OutputStream implements Syncable {

  private static final Logger log = LoggerFactory.getLogger(PvcOutputStream.class);
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1MB

  private final StreamObserver<WriteFileRequest> requestObserver;
  private final CountDownLatch finishLatch;
  private final AtomicReference<WriteFileResponse> responseRef;
  private final AtomicReference<Throwable> errorRef;

  private byte[] buffer;
  private int bufferPos;
  private long totalBytesWritten;
  private boolean closed;

  public PvcOutputStream(PvcFileServiceGrpc.PvcFileServiceStub asyncStub, String path,
      boolean overwrite, boolean syncable, int bufferSize, long blockSize) {
    this.buffer = new byte[DEFAULT_BUFFER_SIZE];
    this.bufferPos = 0;
    this.totalBytesWritten = 0;
    this.closed = false;
    this.finishLatch = new CountDownLatch(1);
    this.responseRef = new AtomicReference<>();
    this.errorRef = new AtomicReference<>();

    this.requestObserver = asyncStub.writeFile(new StreamObserver<>() {
      @Override
      public void onNext(WriteFileResponse response) {
        responseRef.set(response);
      }

      @Override
      public void onError(Throwable t) {
        errorRef.set(t);
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        finishLatch.countDown();
      }
    });

    // Send header as the first message
    requestObserver.onNext(WriteFileRequest.newBuilder()
        .setHeader(WriteFileHeader.newBuilder().setPath(path).setOverwrite(overwrite)
            .setSyncable(syncable).setBufferSize(bufferSize).setBlockSize(blockSize).build())
        .build());
  }

  @Override
  public void write(int b) throws IOException {
    checkNotClosed();
    buffer[bufferPos++] = (byte) b;
    if (bufferPos >= buffer.length) {
      flushBuffer(false);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkNotClosed();
    int remaining = len;
    int srcOff = off;
    while (remaining > 0) {
      int space = buffer.length - bufferPos;
      int toCopy = Math.min(space, remaining);
      System.arraycopy(b, srcOff, buffer, bufferPos, toCopy);
      bufferPos += toCopy;
      srcOff += toCopy;
      remaining -= toCopy;
      if (bufferPos >= buffer.length) {
        flushBuffer(false);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    checkNotClosed();
    flushBuffer(false);
  }

  @Override
  public void hflush() throws IOException {
    checkNotClosed();
    flushBuffer(false);
  }

  @Override
  public void hsync() throws IOException {
    checkNotClosed();
    flushBuffer(false);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    try {
      // Flush any remaining buffered data with last=true
      flushBuffer(true);
      requestObserver.onCompleted();

      // Wait for server response
      if (!finishLatch.await(60, TimeUnit.SECONDS)) {
        throw new IOException("Timed out waiting for write completion");
      }

      Throwable error = errorRef.get();
      if (error != null) {
        throw new IOException("gRPC write error", error);
      }

      WriteFileResponse response = responseRef.get();
      if (response != null && !response.getSuccess()) {
        throw new IOException("Write failed: " + response.getError());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for write completion", e);
    }

    super.close();
  }

  private void flushBuffer(boolean isLast) throws IOException {
    checkError();
    if (bufferPos > 0 || isLast) {
      FileChunk.Builder chunkBuilder = FileChunk.newBuilder()
          .setOffset(totalBytesWritten).setLast(isLast);
      if (bufferPos > 0) {
        chunkBuilder.setData(ByteString.copyFrom(buffer, 0, bufferPos));
        totalBytesWritten += bufferPos;
        bufferPos = 0;
      } else {
        chunkBuilder.setData(ByteString.EMPTY);
      }
      requestObserver
          .onNext(WriteFileRequest.newBuilder().setChunk(chunkBuilder.build()).build());
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    checkError();
  }

  private void checkError() throws IOException {
    Throwable error = errorRef.get();
    if (error != null) {
      throw new IOException("gRPC write error", error);
    }
  }
}
