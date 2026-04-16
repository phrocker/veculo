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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link FSInputStream} that reads from a remote source (e.g., GCS) and simultaneously writes
 * the bytes to a local cache file. On cache miss, the caller reads bytes streamed from the remote
 * source while the cache file is populated in the background. If the stream is fully consumed, the
 * temporary cache file is atomically renamed to the final cache path so subsequent reads hit the
 * local cache.
 *
 * <p>
 * Seek behavior:
 * <ul>
 * <li>Forward seeks within already-cached bytes are served from the partial cache file.</li>
 * <li>Forward seeks beyond cached bytes skip ahead in the remote stream, caching bytes along the
 * way.</li>
 * <li>Backward seeks into already-cached bytes switch to reading from the partial cache file.</li>
 * </ul>
 */
class CacheThroughInputStream extends FSInputStream {

  private static final Logger log = LoggerFactory.getLogger(CacheThroughInputStream.class);

  private final FSDataInputStream remoteStream;
  private final File tempFile;
  private final File finalFile;
  private final CachingGcsFileSystem parentFs;
  private final OutputStream cacheOut;

  /**
   * Bytes written to the cache file so far. Everything in [0, cachedBytes) is available on disk.
   */
  private long cachedBytes;

  /** Current logical read position for the caller. */
  private long position;

  /**
   * When non-null, we are reading from the partial cache file (for backward or within-cache seeks).
   */
  private RandomAccessFile cacheReader;

  /** True after close() has been called. */
  private boolean closed;

  /** True if the entire remote stream has been consumed and the cache file is complete. */
  private boolean cacheComplete;

  CacheThroughInputStream(FSDataInputStream remoteStream, File cacheFile,
      CachingGcsFileSystem parentFs) throws IOException {
    this.remoteStream = remoteStream;
    this.finalFile = cacheFile;
    this.parentFs = parentFs;
    this.position = 0;
    this.cachedBytes = 0;
    this.closed = false;
    this.cacheComplete = false;
    this.cacheReader = null;

    // Write to a temp file, rename atomically when complete
    this.tempFile = new File(cacheFile.getParentFile(),
        cacheFile.getName() + ".tmp." + Thread.currentThread().getId());
    cacheFile.getParentFile().mkdirs();
    this.cacheOut = new FileOutputStream(tempFile);
  }

  @Override
  public void seek(long pos) throws IOException {
    checkNotClosed();
    if (pos < 0) {
      throw new EOFException("Cannot seek to negative position: " + pos);
    }
    if (pos == position) {
      return;
    }

    if (pos < cachedBytes) {
      // Seeking within already-cached data: switch to reading from cache file
      if (cacheReader == null) {
        if (tempFile != null && tempFile.exists()) {
          cacheOut.flush();
          cacheReader = new RandomAccessFile(tempFile, "r");
        } else {
          // Cache file was evicted — fall back to re-reading from GCS
          log.warn("Cache file evicted during seek to {}, falling back to GCS", pos);
          remoteStream.seek(pos);
          position = pos;
          cachedBytes = 0;
          return;
        }
      }
      cacheReader.seek(pos);
      position = pos;
    } else if (pos > position) {
      // Forward seek beyond current position: drain remote stream to cache, advancing position
      closeCacheReader();
      skipRemoteTo(pos);
    } else {
      // Backward seek into cached region was already handled above (pos < cachedBytes).
      // If pos >= cachedBytes but < position, that's also a backward seek, but into un-cached
      // territory. This shouldn't happen because cachedBytes >= position for remote reads,
      // but handle defensively by re-seeking the remote stream.
      closeCacheReader();
      position = pos;
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
    checkNotClosed();
    byte[] single = new byte[1];
    int result = read(single, 0, 1);
    if (result == -1) {
      return -1;
    }
    return single[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkNotClosed();
    if (len == 0) {
      return 0;
    }

    // If reading from the cached portion
    if (cacheReader != null && position < cachedBytes) {
      int toRead = (int) Math.min(len, cachedBytes - position);
      int bytesRead = cacheReader.read(b, off, toRead);
      if (bytesRead > 0) {
        position += bytesRead;
        // If we've consumed all cached bytes, switch back to remote
        if (position >= cachedBytes) {
          closeCacheReader();
        }
        return bytesRead;
      }
      // Fall through to remote read
      closeCacheReader();
    }

    // Read from remote stream, writing through to cache
    if (cacheComplete) {
      return -1;
    }

    int bytesRead = remoteStream.read(b, off, len);
    if (bytesRead == -1) {
      finalizeCacheFile();
      return -1;
    }

    // Write to cache
    cacheOut.write(b, off, bytesRead);
    cachedBytes += bytesRead;
    position += bytesRead;
    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    closeCacheReader();
    try {
      remoteStream.close();
    } catch (IOException e) {
      log.debug("Error closing remote stream", e);
    }

    try {
      cacheOut.close();
    } catch (IOException e) {
      log.debug("Error closing cache output", e);
    }

    if (!cacheComplete) {
      // Stream was not fully consumed; keep the partial cache for potential reuse,
      // but don't promote it. Clean up the temp file.
      if (!tempFile.delete()) {
        log.debug("Failed to delete partial cache file: {}", tempFile);
      }
    }

    super.close();
  }

  /**
   * Skips the remote stream forward to the target position, caching all skipped bytes so the cache
   * file remains contiguous from offset 0.
   */
  private void skipRemoteTo(long targetPos) throws IOException {
    byte[] skipBuf = new byte[8192];
    while (position < targetPos && !cacheComplete) {
      int toRead = (int) Math.min(skipBuf.length, targetPos - position);
      int bytesRead = remoteStream.read(skipBuf, 0, toRead);
      if (bytesRead == -1) {
        finalizeCacheFile();
        return;
      }
      cacheOut.write(skipBuf, 0, bytesRead);
      cachedBytes += bytesRead;
      position += bytesRead;
    }
  }

  /**
   * Promotes the temp cache file to the final path atomically and notifies the parent filesystem of
   * the new cache entry size.
   */
  private void finalizeCacheFile() throws IOException {
    if (cacheComplete) {
      return;
    }
    cacheComplete = true;
    cacheOut.flush();
    cacheOut.close();

    long fileSize = tempFile.length();
    parentFs.evictIfNeeded(fileSize);

    if (!tempFile.renameTo(finalFile)) {
      // Rename failed -- another thread may have cached it. Clean up.
      if (!tempFile.delete()) {
        log.debug("Failed to delete temp cache file after rename failure: {}", tempFile);
      }
      if (finalFile.exists()) {
        log.debug("Cache file already exists (cached by another thread): {}", finalFile);
      } else {
        log.warn("Failed to rename temp cache file {} to {}", tempFile, finalFile);
      }
    } else {
      parentFs.addToCacheSize(fileSize);
      log.debug("Cached file: {} ({} bytes)", finalFile, fileSize);
    }
  }

  private void closeCacheReader() {
    if (cacheReader != null) {
      try {
        cacheReader.close();
      } catch (IOException e) {
        log.debug("Error closing cache reader", e);
      }
      cacheReader = null;
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
  }
}
