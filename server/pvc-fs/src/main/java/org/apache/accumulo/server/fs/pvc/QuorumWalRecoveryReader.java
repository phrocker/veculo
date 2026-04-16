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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.fs.pvc.proto.ListSegmentsRequest;
import org.apache.accumulo.server.fs.pvc.proto.ListSegmentsResponse;
import org.apache.accumulo.server.fs.pvc.proto.ReadSegmentRequest;
import org.apache.accumulo.server.fs.pvc.proto.SegmentChunk;
import org.apache.accumulo.server.fs.pvc.proto.SegmentId;
import org.apache.accumulo.server.fs.pvc.proto.SegmentInfo;
import org.apache.accumulo.server.fs.pvc.proto.WalQuorumPeerGrpc;
import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * An {@link FSInputStream} that reads WAL segments from a peer sidecar during recovery. Used by
 * {@link QuorumWalFileSystem#open(org.apache.hadoop.fs.Path, int)} when a sealed segment is not yet
 * available in GCS.
 *
 * <p>
 * On construction, this reader connects to each peer sidecar, calls {@code ListSegments} to find
 * which peers hold a replica of the requested segment, and selects the peer with the largest
 * (most complete) replica. It then opens a {@code ReadSegment} stream from that peer.
 *
 * <p>
 * The read pattern mirrors {@link PvcInputStream}: data is buffered from gRPC streaming chunks and
 * served to the caller on demand. Seeking is supported by closing the current stream and opening a
 * new {@code ReadSegment} request at the desired offset.
 */
public class QuorumWalRecoveryReader extends FSInputStream {

  private static final Logger log = LoggerFactory.getLogger(QuorumWalRecoveryReader.class);

  private final SegmentId segmentId;
  private final List<ManagedChannel> peerChannels = new ArrayList<>();

  private ManagedChannel selectedChannel;
  private WalQuorumPeerGrpc.WalQuorumPeerBlockingStub selectedStub;
  private String selectedPeer;
  private long selectedPeerSize;

  private long position;
  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;
  private Iterator<SegmentChunk> chunkIterator;
  private boolean streamExhausted;
  private boolean isClosed;

  /**
   * Creates a recovery reader by querying peers for the segment.
   *
   * @param peerHostnames list of peer sidecar hostnames to query
   * @param peerPort the TCP port for peer sidecar gRPC connections
   * @param segmentId the segment to read
   * @throws IOException if no peer holds the requested segment
   */
  public QuorumWalRecoveryReader(List<String> peerHostnames, int peerPort, SegmentId segmentId)
      throws IOException {
    this.segmentId = segmentId;
    this.position = 0;
    this.buffer = null;
    this.bufferOffset = 0;
    this.bufferLength = 0;
    this.chunkIterator = null;
    this.streamExhausted = false;
    this.isClosed = false;

    // Query each peer to find who has this segment and pick the largest replica
    String bestPeer = null;
    long bestSize = -1;
    ManagedChannel bestChannel = null;
    WalQuorumPeerGrpc.WalQuorumPeerBlockingStub bestStub = null;

    for (String hostname : peerHostnames) {
      ManagedChannel channel = null;
      try {
        channel = ManagedChannelBuilder.forAddress(hostname, peerPort)
            .usePlaintext()
            .keepAliveTime(30, TimeUnit.SECONDS)
            .build();
        peerChannels.add(channel);

        WalQuorumPeerGrpc.WalQuorumPeerBlockingStub stub =
            WalQuorumPeerGrpc.newBlockingStub(channel);

        ListSegmentsResponse response = stub.listSegments(
            ListSegmentsRequest.newBuilder().build());

        for (SegmentInfo info : response.getSegmentsList()) {
          if (info.getSegmentId().getUuid().equals(segmentId.getUuid())) {
            log.debug("Found segment {} on peer {} with size {}",
                segmentId.getUuid(), hostname, info.getSize());
            if (info.getSize() > bestSize) {
              bestPeer = hostname;
              bestSize = info.getSize();
              bestChannel = channel;
              bestStub = stub;
            }
            break;
          }
        }
      } catch (StatusRuntimeException e) {
        log.debug("Failed to query peer {}: {}", hostname, e.getMessage());
      }
    }

    if (bestPeer == null) {
      // Clean up channels before throwing
      closeAllChannels();
      throw new FileNotFoundException(
          "WAL segment " + segmentId.getUuid() + " not found on any peer sidecar");
    }

    this.selectedPeer = bestPeer;
    this.selectedPeerSize = bestSize;
    this.selectedChannel = bestChannel;
    this.selectedStub = bestStub;

    log.info("Selected peer {} for segment {} recovery (size={})",
        selectedPeer, segmentId.getUuid(), selectedPeerSize);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException("Cannot seek to negative position: " + pos);
    }
    if (pos != this.position) {
      this.position = pos;
      // Invalidate current buffer and stream — next read will open a new stream
      this.buffer = null;
      this.bufferOffset = 0;
      this.bufferLength = 0;
      this.chunkIterator = null;
      this.streamExhausted = false;
    }
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    return false;
  }

  @Override
  public int read() throws IOException {
    byte[] single = new byte[1];
    int result = read(single, 0, 1);
    if (result == -1) {
      return -1;
    }
    return single[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (isClosed) {
      throw new IOException("Stream is closed");
    }

    int totalCopied = 0;

    while (totalCopied < len) {
      // If we have data in the buffer, copy from it
      if (buffer != null && bufferOffset < bufferLength) {
        int available = bufferLength - bufferOffset;
        int toCopy = Math.min(available, len - totalCopied);
        System.arraycopy(buffer, bufferOffset, b, off + totalCopied, toCopy);
        bufferOffset += toCopy;
        totalCopied += toCopy;
        position += toCopy;
        continue;
      }

      // Buffer exhausted, try to get next chunk
      if (!fillBuffer()) {
        break;
      }
    }

    return totalCopied == 0 ? -1 : totalCopied;
  }

  /**
   * Fills the internal buffer from the gRPC ReadSegment stream. Opens a new stream if needed
   * (initial read or after a seek).
   *
   * @return true if data is available, false if the stream is exhausted
   */
  private boolean fillBuffer() {
    if (streamExhausted) {
      return false;
    }

    // Start a new ReadSegment stream if needed
    if (chunkIterator == null) {
      try {
        chunkIterator = selectedStub.readSegment(
            ReadSegmentRequest.newBuilder()
                .setSegmentId(segmentId)
                .setOffset(position)
                .setMaxBytes(0) // 0 = read to end
                .build());
      } catch (StatusRuntimeException e) {
        log.error("Failed to open ReadSegment stream from peer {}: {}",
            selectedPeer, e.getMessage());
        streamExhausted = true;
        return false;
      }
    }

    if (chunkIterator.hasNext()) {
      SegmentChunk chunk = chunkIterator.next();
      buffer = chunk.getData().toByteArray();
      bufferOffset = 0;
      bufferLength = buffer.length;
      if (chunk.getLast()) {
        streamExhausted = true;
      }
      return bufferLength > 0;
    } else {
      streamExhausted = true;
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }
    isClosed = true;
    buffer = null;
    chunkIterator = null;
    closeAllChannels();
    super.close();
  }

  /**
   * Shuts down all peer gRPC channels.
   */
  private void closeAllChannels() {
    for (ManagedChannel channel : peerChannels) {
      try {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
          channel.shutdownNow();
        }
      } catch (InterruptedException e) {
        channel.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    peerChannels.clear();
  }
}
