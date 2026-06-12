/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.system;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class SystemMetricsTest {

  private File tempDir;

  @Before
  public void setUp() {
    tempDir = new File("target", "system-metrics-test-" + System.nanoTime());
    File dataDir = new File(tempDir, "data");
    assertTrue(dataDir.mkdirs());
  }

  @After
  public void tearDown() {
    if (tempDir != null) {
      // Best-effort cleanup; the data subdir may already be gone after the test.
      new File(tempDir, "data").delete();
      tempDir.delete();
    }
  }

  /**
   * Regression test for the case where the directory backing a cached {@link
   * java.nio.file.FileStore} is removed while IoTDB is running (e.g. an empty data region directory
   * deleted during region migration). The disk-space metrics must recover by re-resolving the file
   * stores instead of throwing / permanently returning the stale value.
   */
  @Test
  public void testDiskSpaceRecoversWhenBackingDirIsRemoved() {
    SystemMetrics systemMetrics = new SystemMetrics();
    File dataDir = new File(tempDir, "data");
    systemMetrics.setDiskDirs(Collections.singletonList(dataDir.getAbsolutePath()));

    // Sanity check: the file store resolves to a real device with a positive size.
    assertTrue(systemMetrics.getSystemDiskTotalSpace() > 0L);
    assertTrue(systemMetrics.getSystemDiskFreeSpace() > 0L);
    assertTrue(systemMetrics.getSystemDiskAvailableSpace() > 0L);

    // The directory the FileStore was pinned to is removed; its parent still lives on the same
    // device. The metrics should re-resolve the file store and keep reporting a positive size
    // rather than flooding the log with NoSuchFileException.
    assertTrue(dataDir.delete());

    assertTrue(systemMetrics.getSystemDiskTotalSpace() > 0L);
    assertTrue(systemMetrics.getSystemDiskFreeSpace() > 0L);
    assertTrue(systemMetrics.getSystemDiskAvailableSpace() > 0L);
  }
}
