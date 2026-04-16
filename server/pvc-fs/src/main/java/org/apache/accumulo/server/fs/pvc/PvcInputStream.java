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
import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.server.fs.pvc.proto.FileChunk;
import org.apache.accumulo.server.fs.pvc.proto.PvcFileServiceGrpc;
import org.apache.accumulo.server.fs.pvc.proto.ReadFileRequest;
import org.apache.hadoop.fs.FSInputStream;

/**
 * An {@link FSInputStream} backed by gRPC streaming reads from a remote PVC file server. Supports
 * seeking by issuing new ReadFile requests at the desired offset.
 */
public class PvcInputStream extends FSInputStream {

  private final PvcFileServiceGrpc.PvcFileServiceBlockingStub stub;
  private final String path;
  private long position;
  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;
  private Iterator<FileChunk> chunkIterator;
  private boolean streamExhausted;

  public PvcInputStream(PvcFileServiceGrpc.PvcFileServiceBlockingStub stub, String path) {
    this.stub = stub;
    this.path = path;
    this.position = 0;
    this.buffer = null;
    this.bufferOffset = 0;
    this.bufferLength = 0;
    this.chunkIterator = null;
    this.streamExhausted = false;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException("Cannot seek to negative position: " + pos);
    }
    if (pos != this.position) {
      this.position = pos;
      // Invalidate current buffer and stream
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
        break; // No more data
      }
    }

    return totalCopied == 0 ? -1 : totalCopied;
  }

  /**
   * Fills the internal buffer from the gRPC stream. Returns true if data is available.
   */
  private boolean fillBuffer() {
    if (streamExhausted) {
      return false;
    }

    // Start a new stream if needed
    if (chunkIterator == null) {
      ReadFileRequest request = ReadFileRequest.newBuilder().setPath(path).setOffset(position)
          .setLength(0) // 0 = read to end
          .build();
      try {
        chunkIterator = stub.readFile(request);
      } catch (Exception e) {
        streamExhausted = true;
        return false;
      }
    }

    if (chunkIterator.hasNext()) {
      FileChunk chunk = chunkIterator.next();
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
    buffer = null;
    chunkIterator = null;
    super.close();
  }
}
