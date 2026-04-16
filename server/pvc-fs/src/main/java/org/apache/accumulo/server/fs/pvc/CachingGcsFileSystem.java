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
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A caching wrapper around GCS (or any Hadoop {@link FileSystem}). Caches file reads on a local
 * directory (PVC mount) for fast subsequent access.
 *
 * <p>
 * This extends {@link FilterFileSystem} which delegates all operations to a wrapped FileSystem. Only
 * {@link #open(Path, int)} is overridden for read caching, and {@link #delete(Path, boolean)} is
 * overridden for cache cleanup. All other operations (create, rename, mkdirs, listStatus,
 * getFileStatus) pass through to the delegate.
 *
 * <h3>Configuration</h3>
 * <ul>
 * <li>{@code fs.cache.delegate.impl} - Fully qualified class name of the delegate FileSystem
 * implementation. Default: {@code com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem}</li>
 * <li>{@code fs.cache.local.dir} - Local PVC directory for cached files. Default:
 * {@code /mnt/data/cache}</li>
 * <li>{@code fs.cache.max.size} - Maximum cache size in bytes. Default: 40 GB</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <p>
 * Register as a new scheme (recommended):
 *
 * <pre>
 *   fs.cgs.impl = org.apache.accumulo.server.fs.pvc.CachingGcsFileSystem
 * </pre>
 *
 * Or as a transparent wrapper over {@code gs://}:
 *
 * <pre>
 *   fs.gs.impl = org.apache.accumulo.server.fs.pvc.CachingGcsFileSystem
 *   fs.cache.delegate.impl = com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
 * </pre>
 *
 * <h3>Thread Safety</h3>
 * <p>
 * Cache population uses temp files with thread-specific names and atomic rename so concurrent
 * threads caching the same file don't corrupt each other. Cache eviction is best-effort LRU by file
 * modification time.
 */
public class CachingGcsFileSystem extends FilterFileSystem {

  private static final Logger log = LoggerFactory.getLogger(CachingGcsFileSystem.class);

  public static final String CONFIG_DELEGATE_IMPL = "fs.cache.delegate.impl";
  public static final String CONFIG_CACHE_DIR = "fs.cache.local.dir";
  public static final String CONFIG_MAX_CACHE_SIZE = "fs.cache.max.size";

  public static final String DEFAULT_DELEGATE_IMPL =
      "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  public static final String DEFAULT_CACHE_DIR = "/mnt/data/cache";
  public static final long DEFAULT_MAX_CACHE_SIZE = 40L * 1024 * 1024 * 1024; // 40 GB

  private File cacheDir;
  private long maxCacheSize;
  private final AtomicLong currentCacheSize = new AtomicLong(0);

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    // Instantiate and initialize the delegate filesystem
    String delegateImpl = conf.get(CONFIG_DELEGATE_IMPL, DEFAULT_DELEGATE_IMPL);
    try {
      Class<?> delegateClass = conf.getClassByName(delegateImpl);
      FileSystem delegate = (FileSystem) ReflectionUtils.newInstance(delegateClass, conf);
      delegate.initialize(name, conf);
      this.fs = delegate;
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not load delegate FileSystem class: " + delegateImpl, e);
    }

    super.initialize(name, conf);

    this.cacheDir = new File(conf.get(CONFIG_CACHE_DIR, DEFAULT_CACHE_DIR));
    this.maxCacheSize = conf.getLong(CONFIG_MAX_CACHE_SIZE, DEFAULT_MAX_CACHE_SIZE);

    if (!cacheDir.exists() && !cacheDir.mkdirs()) {
      throw new IOException("Failed to create cache directory: " + cacheDir);
    }

    calculateCurrentCacheSize();

    log.info(
        "Initialized CachingGcsFileSystem: delegate={}, cacheDir={}, "
            + "maxCacheSize={} bytes, currentCacheSize={} bytes",
        delegateImpl, cacheDir, maxCacheSize, currentCacheSize.get());
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    File cachedFile = toCachePath(f);

    if (cachedFile.exists() && cachedFile.length() > 0) {
      // Cache hit -- serve from local PVC
      log.debug("Cache hit: {}", f);
      // Touch the file so LRU eviction keeps recently-read files
      if (!cachedFile.setLastModified(System.currentTimeMillis())) {
        log.trace("Failed to touch cache file: {}", cachedFile);
      }
      return new FSDataInputStream(new LocalCacheInputStream(cachedFile));
    }

    // Cache miss -- read from delegate (GCS) and cache through to local PVC
    log.debug("Cache miss: {}", f);
    FSDataInputStream remoteStream = super.open(f, bufferSize);
    return new FSDataInputStream(new CacheThroughInputStream(remoteStream, cachedFile, this));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    // Delete from the delegate (GCS)
    boolean result = super.delete(f, recursive);

    // Also remove from the local cache
    File cachedFile = toCachePath(f);
    deleteCachedFile(cachedFile, recursive);

    return result;
  }

  /**
   * Converts a remote path (e.g., {@code gs://bucket/accumulo/tables/3/t-001/F001.rf}) to a local
   * cache path (e.g., {@code /mnt/data/cache/bucket/accumulo/tables/3/t-001/F001.rf}).
   */
  private File toCachePath(Path remotePath) {
    URI uri = remotePath.toUri();
    String host = uri.getHost();
    String pathStr = uri.getPath();
    // Include the bucket (host) in the cache path to avoid collisions across buckets
    if (host != null && !host.isEmpty()) {
      return new File(cacheDir, host + pathStr);
    }
    return new File(cacheDir, pathStr);
  }

  /**
   * Evicts cached files (oldest first by modification time) until the cache has room for an
   * incoming file of the given size. This is best-effort: if eviction fails, the cache may
   * temporarily exceed {@code maxCacheSize}.
   */
  void evictIfNeeded(long incomingSize) {
    long target = maxCacheSize - incomingSize;
    if (currentCacheSize.get() <= target) {
      return;
    }

    log.debug("Cache eviction needed: current={}, max={}, incoming={}", currentCacheSize.get(),
        maxCacheSize, incomingSize);

    File[] allFiles = listCacheFilesRecursive(cacheDir);
    if (allFiles == null || allFiles.length == 0) {
      return;
    }

    // Sort by last modified time ascending (oldest first) for LRU eviction
    Arrays.sort(allFiles, Comparator.comparingLong(File::lastModified));

    for (File file : allFiles) {
      if (currentCacheSize.get() <= target) {
        break;
      }
      long fileSize = file.length();
      if (file.delete()) {
        currentCacheSize.addAndGet(-fileSize);
        log.debug("Evicted cache file: {} ({} bytes)", file, fileSize);
      }
    }
  }

  /**
   * Adds the given size to the tracked cache size. Called by {@link CacheThroughInputStream} after
   * successfully caching a file.
   */
  void addToCacheSize(long size) {
    currentCacheSize.addAndGet(size);
  }

  /**
   * Recursively scans the cache directory and sums file sizes to initialize
   * {@link #currentCacheSize}.
   */
  private void calculateCurrentCacheSize() {
    long total = sumDirectorySize(cacheDir);
    currentCacheSize.set(total);
  }

  private long sumDirectorySize(File dir) {
    long size = 0;
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          size += sumDirectorySize(file);
        } else {
          size += file.length();
        }
      }
    }
    return size;
  }

  /**
   * Lists all regular files in the directory tree rooted at {@code dir}.
   */
  private File[] listCacheFilesRecursive(File dir) {
    java.util.List<File> result = new java.util.ArrayList<>();
    collectFiles(dir, result);
    return result.toArray(new File[0]);
  }

  private void collectFiles(File dir, java.util.List<File> result) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          collectFiles(file, result);
        } else if (!file.getName().contains(".tmp.")) {
          // Skip temp files that are still being written
          result.add(file);
        }
      }
    }
  }

  /**
   * Deletes a cached file or directory from the local cache and adjusts the tracked cache size.
   */
  private void deleteCachedFile(File cachedFile, boolean recursive) {
    if (!cachedFile.exists()) {
      return;
    }
    if (cachedFile.isDirectory() && recursive) {
      File[] children = cachedFile.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteCachedFile(child, true);
        }
      }
      if (!cachedFile.delete()) {
        log.debug("Failed to delete cache directory: {}", cachedFile);
      }
    } else if (cachedFile.isFile()) {
      long size = cachedFile.length();
      if (cachedFile.delete()) {
        currentCacheSize.addAndGet(-size);
      } else {
        log.debug("Failed to delete cache file: {}", cachedFile);
      }
    }
  }
}
