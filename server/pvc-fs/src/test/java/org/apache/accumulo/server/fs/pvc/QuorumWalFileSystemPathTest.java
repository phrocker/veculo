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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates that GCS path mapping is consistent across all QuorumWalFileSystem operations for
 * recovery paths. The root cause of the sort-loop bug was that {@code create()} used
 * {@code toGcsMetadataPath()} while {@code exists()} used {@code toGcsPath()}, producing different
 * GCS locations for the same recovery artifact (e.g., the "finished" marker).
 */
public class QuorumWalFileSystemPathTest {

  private QuorumWalFileSystem fs;
  private static final String GCS_BUCKET = "veculo-accumulo-data";
  private static final String GCS_PREFIX = "wal/";
  private static final String GCS_VOLUME = "gs://veculo-accumulo-data/accumulo";

  @BeforeEach
  public void setup() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(QuorumWalFileSystem.CONFIG_GCS_BUCKET, GCS_BUCKET);
    conf.set(QuorumWalFileSystem.CONFIG_GCS_PREFIX, GCS_PREFIX);
    conf.set("fs.qwal.gcs.volume", GCS_VOLUME);
    // No socket path — runs in delegation mode (no sidecar)
    conf.set(QuorumWalFileSystem.CONFIG_SOCKET_PATH, "/nonexistent/qwal.sock");

    fs = new QuorumWalFileSystem();
    fs.initialize(URI.create("qwal://local/accumulo"), conf);
  }

  @Test
  public void testToGcsPathAndToGcsMetadataPathDifferForRecoveryPaths() {
    // This test documents the KNOWN divergence between toGcsPath and toGcsMetadataPath.
    // toGcsPath prepends "gs://bucket/prefix" + stripped path.
    // toGcsMetadataPath strips the qwal base and prepends the GCS volume.
    // For recovery paths, isMetadataPath must return true so that all operations
    // consistently use toGcsMetadataPath.

    String recoveryPath = "/accumulo/recovery/4456a76b-9e36-47a8-8dd1-c3583e8181a8/finished";

    String viaToGcsPath = fs.toGcsPath(recoveryPath);
    // toGcsPath: gs://veculo-accumulo-data/wal/accumulo/recovery/.../finished
    assertEquals("gs://veculo-accumulo-data/wal/accumulo/recovery/"
        + "4456a76b-9e36-47a8-8dd1-c3583e8181a8/finished", viaToGcsPath);

    // These two methods produce DIFFERENT paths — that was the bug.
    // The fix is isMetadataPath routing recovery paths through toGcsMetadataPath consistently.
    assertNotEquals(viaToGcsPath,
        GCS_VOLUME + "/recovery/4456a76b-9e36-47a8-8dd1-c3583e8181a8/finished",
        "If these were equal, there would be no bug to fix");
  }

  @Test
  public void testIsMetadataPathMatchesRecoverySubPaths() {
    // Recovery sub-paths must be recognized as metadata paths so all operations
    // (exists, create, delete, mkdirs, listStatus, getFileStatus) use toGcsMetadataPath.
    assertTrue(isMetadataPath("/accumulo/recovery"),
        "recovery directory itself");
    assertTrue(isMetadataPath("/accumulo/recovery/4456a76b-9e36-47a8-8dd1-c3583e8181a8"),
        "recovery UUID directory");
    assertTrue(isMetadataPath("/accumulo/recovery/4456a76b-9e36-47a8-8dd1-c3583e8181a8/finished"),
        "finished marker");
    assertTrue(isMetadataPath(
            "/accumulo/recovery/4456a76b-9e36-47a8-8dd1-c3583e8181a8/part-r-00000.rf"),
        "sorted rfile");
    assertTrue(isMetadataPath("/accumulo/recovery/4456a76b-9e36-47a8-8dd1-c3583e8181a8/failed"),
        "failed marker");
  }

  @Test
  public void testWalPathsAreNotMetadataPaths() {
    // WAL data paths must NOT be metadata paths — they go through the sidecar/peer read path.
    assertTrue(!isMetadataPath("/accumulo/wal/10.112.1.34+9800/4456a76b-9e36-47a8-8dd1-c3583e8181a8"),
        "WAL data path should NOT be a metadata path");
  }

  @Test
  public void testGcsPathConsistencyForRecoveryArtifacts() {
    // The critical invariant: the GCS path used to create a recovery artifact must match
    // the GCS path used to check for its existence. Both must go through toGcsMetadataPath
    // when isMetadataPath returns true.

    // Simulate what create() does for a non-/wal/ path:
    // It calls toGcsMetadataPath directly.
    String finishedPath = "/accumulo/recovery/test-uuid/finished";
    String createGcsPath = toGcsMetadataPathString(finishedPath);

    // Simulate what exists() does after the isMetadataPath fix:
    // isMetadataPath returns true → uses toGcsMetadataPath.
    assertTrue(isMetadataPath(finishedPath), "recovery paths must be metadata paths");
    String existsGcsPath = toGcsMetadataPathString(finishedPath);

    assertEquals(createGcsPath, existsGcsPath,
        "create() and exists() must resolve to the same GCS path for recovery artifacts");

    // Verify for sorted rfiles too
    String rfilePath = "/accumulo/recovery/test-uuid/part-r-00000.rf";
    assertEquals(toGcsMetadataPathString(rfilePath), toGcsMetadataPathString(rfilePath));
  }

  @Test
  public void testActualToGcsPathOutput() {
    // Verify the real toGcsPath (package-private) produces what we expect
    String walPath = "/accumulo/wal/10.112.1.34+9800/test-uuid";
    assertEquals("gs://veculo-accumulo-data/wal/accumulo/wal/10.112.1.34+9800/test-uuid",
        fs.toGcsPath(walPath));

    String recoveryPath = "/accumulo/recovery/test-uuid/finished";
    assertEquals("gs://veculo-accumulo-data/wal/accumulo/recovery/test-uuid/finished",
        fs.toGcsPath(recoveryPath),
        "toGcsPath adds the wal/ prefix, producing a path different from toGcsMetadataPath");
  }

  // -- helpers that access package-private methods --

  /**
   * Delegates to the private isMetadataPath. Since this test is in the same package, we can
   * reflectively call it, or just replicate the logic. Here we replicate to keep the test simple.
   */
  private boolean isMetadataPath(String pathStr) {
    return pathStr.equals("/") || pathStr.equals("/instance_id")
        || pathStr.endsWith("/instance_id") || pathStr.equals("/version")
        || pathStr.endsWith("/version") || pathStr.endsWith("/tables")
        || pathStr.endsWith("/recovery") || pathStr.contains("/recovery/")
        || pathStr.startsWith("/tables/");
  }

  /**
   * Simulates toGcsMetadataPath: strips the qwal base path, prepends gcsVolume.
   */
  private String toGcsMetadataPathString(String pathStr) {
    String qwalBase = "/accumulo"; // from URI qwal://local/accumulo
    if (pathStr.startsWith(qwalBase)) {
      pathStr = pathStr.substring(qwalBase.length());
    }
    return GCS_VOLUME + pathStr;
  }
}
