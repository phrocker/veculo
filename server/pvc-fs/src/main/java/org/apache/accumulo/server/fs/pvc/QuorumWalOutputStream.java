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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

import org.apache.accumulo.server.fs.pvc.proto.CloseSegmentRequest;
import org.apache.accumulo.server.fs.pvc.proto.CloseSegmentResponse;
import org.apache.accumulo.server.fs.pvc.proto.SegmentId;
import org.apache.accumulo.server.fs.pvc.proto.SyncSegmentRequest;
import org.apache.accumulo.server.fs.pvc.proto.SyncSegmentResponse;
import org.apache.accumulo.server.fs.pvc.proto.WalQuorumLocalGrpc;
import org.apache.accumulo.server.fs.pvc.proto.WriteEntryRequest;
import org.apache.accumulo.server.fs.pvc.proto.WriteEntryResponse;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * An {@link OutputStream} that writes WAL entries to the quorum replication sidecar via
 * bidirectional gRPC streaming. Implements {@link Syncable} so that Accumulo's WAL writer can call
 * {@code hsync()} and {@code hflush()} to ensure quorum durability.
 *
 * <p>
 * The write path is:
 * <ol>
 * <li>Buffer incoming bytes (64KB buffer for low-latency WAL writes)</li>
 * <li>On buffer full or sync: flush buffered data as a {@link WriteEntryRequest} with a sequence
 * number and CRC-32C checksum</li>
 * <li>On {@code hflush()}: flush buffer, then call {@code SyncSegment(hsync=false)} and block until
 * quorum ack</li>
 * <li>On {@code hsync()}: flush buffer, then call {@code SyncSegment(hsync=true)} and block until
 * quorum ack</li>
 * <li>On {@code close()}: flush remaining data, complete the stream, call {@code CloseSegment} to
 * trigger seal + GCS upload, and wait for confirmation</li>
 * </ol>
 */
public class QuorumWalOutputStream extends OutputStream implements Syncable {

  private static final Logger log = LoggerFactory.getLogger(QuorumWalOutputStream.class);

  /** 64KB buffer — WAL entries are small, we want low latency. */
  private static final int BUFFER_SIZE = 64 * 1024;

  /** Timeout for CloseSegment (includes GCS upload time). */
  private static final long CLOSE_TIMEOUT_SECONDS = 60;

  /** Minimum quorum count considered healthy (local + at least 1 peer). */
  private static final int HEALTHY_QUORUM = 2;

  private final WalQuorumLocalGrpc.WalQuorumLocalBlockingStub blockingStub;
  private final SegmentId segmentId;
  private final AtomicLong sequenceNum = new AtomicLong(0);
  private final AtomicReference<Throwable> streamError = new AtomicReference<>();
  private final AtomicReference<WriteEntryResponse> lastAck = new AtomicReference<>();
  private final CountDownLatch streamFinishLatch = new CountDownLatch(1);

  /** Set to true when the sidecar reports quorum_count < HEALTHY_QUORUM. */
  private volatile boolean degraded = false;

  private final StreamObserver<WriteEntryRequest> requestObserver;
  private final byte[] buffer = new byte[BUFFER_SIZE];
  private int bufferPos;
  private long totalBytesWritten;
  private boolean closed;

  /**
   * Creates a new quorum WAL output stream.
   *
   * @param asyncStub the async stub for bidirectional WriteEntries streaming
   * @param blockingStub the blocking stub for SyncSegment and CloseSegment calls
   * @param segmentId the segment being written
   */
  public QuorumWalOutputStream(WalQuorumLocalGrpc.WalQuorumLocalStub asyncStub,
      WalQuorumLocalGrpc.WalQuorumLocalBlockingStub blockingStub, SegmentId segmentId) {
    this.blockingStub = blockingStub;
    this.segmentId = segmentId;
    this.bufferPos = 0;
    this.totalBytesWritten = 0;
    this.closed = false;

    // Open the bidirectional WriteEntries stream
    this.requestObserver = asyncStub.writeEntries(new StreamObserver<>() {
      @Override
      public void onNext(WriteEntryResponse response) {
        lastAck.set(response);
        int qc = response.getQuorumCount();
        if (qc < HEALTHY_QUORUM && !degraded) {
          degraded = true;
          log.warn("Quorum degraded for segment {} — quorum_count={}, expected >= {}",
              segmentId.getUuid(), qc, HEALTHY_QUORUM);
        } else if (qc >= HEALTHY_QUORUM && degraded) {
          degraded = false;
          log.info("Quorum recovered for segment {} — quorum_count={}",
              segmentId.getUuid(), qc);
        }
        log.trace("Ack received: seq={}, quorum={}, offset={}",
            response.getAckedSequenceNum(), qc, response.getCommittedOffset());
      }

      @Override
      public void onError(Throwable t) {
        streamError.set(t);
        streamFinishLatch.countDown();
        log.error("WriteEntries stream error for segment {}", segmentId.getUuid(), t);
      }

      @Override
      public void onCompleted() {
        streamFinishLatch.countDown();
        log.debug("WriteEntries stream completed for segment {}", segmentId.getUuid());
      }
    });
  }

  @Override
  public void write(int b) throws IOException {
    checkNotClosed();
    buffer[bufferPos++] = (byte) b;
    if (bufferPos >= buffer.length) {
      flushBuffer();
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
        flushBuffer();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    checkNotClosed();
    if (bufferPos > 0) {
      flushBuffer();
    }
  }

  @Override
  public void hflush() throws IOException {
    checkNotClosed();
    if (bufferPos > 0) {
      flushBuffer();
    }

    // Block until quorum has flushed (not necessarily fdatasync)
    try {
      SyncSegmentResponse response = blockingStub.syncSegment(
          SyncSegmentRequest.newBuilder().setSegmentId(segmentId).build());
      if (!response.getSuccess()) {
        throw new IOException(
            "hflush failed for segment " + segmentId.getUuid() + ": " + response.getError());
      }
      log.trace("hflush complete for segment {}, synced offset={}",
          segmentId.getUuid(), response.getSyncedOffset());
    } catch (StatusRuntimeException e) {
      throw new IOException("gRPC error during hflush for segment " + segmentId.getUuid(), e);
    }
  }

  @Override
  public void hsync() throws IOException {
    checkNotClosed();
    if (bufferPos > 0) {
      flushBuffer();
    }

    // Block until quorum has called fdatasync
    try {
      SyncSegmentResponse response = blockingStub.syncSegment(
          SyncSegmentRequest.newBuilder().setSegmentId(segmentId).build());
      if (!response.getSuccess()) {
        throw new IOException(
            "hsync failed for segment " + segmentId.getUuid() + ": " + response.getError());
      }
      log.trace("hsync complete for segment {}, synced offset={}",
          segmentId.getUuid(), response.getSyncedOffset());
    } catch (StatusRuntimeException e) {
      throw new IOException("gRPC error during hsync for segment " + segmentId.getUuid(), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    try {
      // Flush any remaining buffered data
      if (bufferPos > 0) {
        flushBuffer();
      }

      // Complete the bidirectional WriteEntries stream
      requestObserver.onCompleted();

      // Wait for the stream to finish
      if (!streamFinishLatch.await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        log.warn("Timed out waiting for WriteEntries stream completion for segment {}",
            segmentId.getUuid());
      }

      Throwable error = streamError.get();
      if (error != null) {
        throw new IOException(
            "WriteEntries stream had error for segment " + segmentId.getUuid(), error);
      }

      // Seal the segment and trigger GCS upload
      log.debug("Closing segment {} — triggering seal + GCS upload", segmentId.getUuid());
      CloseSegmentResponse closeResponse = blockingStub
          .withDeadlineAfter(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .closeSegment(CloseSegmentRequest.newBuilder()
              .setSegmentId(segmentId)
              .build());

      if (!closeResponse.getSuccess()) {
        throw new IOException("CloseSegment failed for " + segmentId.getUuid() + ": "
            + closeResponse.getError());
      }

      log.info("WAL segment {} sealed: size={}, gcs={}",
          segmentId.getUuid(), closeResponse.getSegmentSize(),
          closeResponse.getGcsObjectPath());

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          "Interrupted waiting for segment close: " + segmentId.getUuid(), e);
    } catch (StatusRuntimeException e) {
      throw new IOException(
          "gRPC error closing segment " + segmentId.getUuid(), e);
    }

    super.close();
  }

  /**
   * Flushes the internal buffer to the sidecar's WriteEntries stream as a single entry with a
   * sequence number and CRC-32C checksum.
   */
  private void flushBuffer() throws IOException {
    checkError();
    if (bufferPos == 0) {
      return;
    }

    // Compute CRC-32C checksum of the buffered data
    CRC32C crc = new CRC32C();
    crc.update(buffer, 0, bufferPos);

    long seq = sequenceNum.incrementAndGet();
    requestObserver.onNext(WriteEntryRequest.newBuilder()
        .setSegmentId(segmentId)
        .setSequenceNum(seq)
        .setData(ByteString.copyFrom(buffer, 0, bufferPos))
        .setChecksum((int) crc.getValue())
        .build());

    totalBytesWritten += bufferPos;
    bufferPos = 0;

    log.trace("Flushed entry seq={} for segment {}, total written={}",
        seq, segmentId.getUuid(), totalBytesWritten);
  }

  /**
   * Returns true if the sidecar reported degraded quorum (fewer peer acks than expected). When
   * degraded, the caller should close this WAL early so the segment gets sealed and uploaded to GCS,
   * ensuring full durability.
   */
  public boolean isDegraded() {
    return degraded;
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    checkError();
  }

  private void checkError() throws IOException {
    Throwable error = streamError.get();
    if (error != null) {
      throw new IOException(
          "WriteEntries stream error for segment " + segmentId.getUuid(), error);
    }
  }
}
