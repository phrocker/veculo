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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.fs.FSInputStream;

/**
 * An {@link FSInputStream} that reads from a local cache file using {@link RandomAccessFile}.
 * Supports seeking for random-access reads (e.g., RFile block reads).
 */
class LocalCacheInputStream extends FSInputStream {

  private final RandomAccessFile raf;
  private final long fileLength;
  private boolean closed;

  LocalCacheInputStream(File cacheFile) throws IOException {
    this.raf = new RandomAccessFile(cacheFile, "r");
    this.fileLength = cacheFile.length();
    this.closed = false;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkNotClosed();
    if (pos < 0) {
      throw new EOFException("Cannot seek to negative position: " + pos);
    }
    raf.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    checkNotClosed();
    return raf.getFilePointer();
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    return false;
  }

  @Override
  public int read() throws IOException {
    checkNotClosed();
    return raf.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkNotClosed();
    return raf.read(b, off, len);
  }

  @Override
  public int available() throws IOException {
    checkNotClosed();
    long remaining = fileLength - raf.getFilePointer();
    return (int) Math.min(remaining, Integer.MAX_VALUE);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      raf.close();
      super.close();
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
  }
}
