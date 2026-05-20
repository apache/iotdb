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

package org.apache.iotdb.commons.cluster;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DiskCheckerTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private NodeStatus savedStatus;
  private String savedReason;

  @Before
  public void setUp() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    savedStatus = config.getNodeStatus();
    savedReason = config.getStatusReason();
    config.setNodeStatus(NodeStatus.Running);
    config.setStatusReason(null);
  }

  @After
  public void tearDown() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    config.setNodeStatus(savedStatus);
    config.setStatusReason(savedReason);
  }

  @Test
  public void checkReturnsNormalForWritableDirectoryWithSpace() throws Exception {
    File dir = tmp.newFolder();
    // threshold 0.0 → any positive usable space passes
    assertEquals(
        DiskChecker.DiskStatus.NORMAL,
        DiskChecker.check(Collections.singletonList(dir.getAbsolutePath()), 0.0));
  }

  @Test
  public void checkReturnsDiskFullWhenBelowThreshold() throws Exception {
    File dir = tmp.newFolder();
    // threshold > 1 forces every directory to be reported as full
    assertEquals(
        DiskChecker.DiskStatus.DISK_FULL,
        DiskChecker.check(Collections.singletonList(dir.getAbsolutePath()), 2.0));
  }

  @Test
  public void checkReturnsDiskCrashWhenDirectoryMissing() {
    String missing = new File(tmp.getRoot(), "does-not-exist").getAbsolutePath();
    assertEquals(
        DiskChecker.DiskStatus.DISK_CRASH,
        DiskChecker.check(Collections.singletonList(missing), 0.0));
  }

  @Test
  public void checkReturnsDiskCrashWhenPathIsAFile() throws Exception {
    File file = tmp.newFile();
    assertEquals(
        DiskChecker.DiskStatus.DISK_CRASH,
        DiskChecker.check(Collections.singletonList(file.getAbsolutePath()), 0.0));
  }

  @Test
  public void checkPrioritizesCrashOverFull() throws Exception {
    File healthy = tmp.newFolder();
    String missing = new File(tmp.getRoot(), "missing").getAbsolutePath();
    // Even with a "full" threshold the missing dir trumps it.
    assertEquals(
        DiskChecker.DiskStatus.DISK_CRASH,
        DiskChecker.check(java.util.Arrays.asList(healthy.getAbsolutePath(), missing), 2.0));
  }

  @Test
  public void checkSkipsNullAndEmptyEntries() throws Exception {
    File dir = tmp.newFolder();
    assertEquals(
        DiskChecker.DiskStatus.NORMAL,
        DiskChecker.check(java.util.Arrays.asList(null, "", dir.getAbsolutePath()), 0.0));
  }

  @Test
  public void applyDiskFullSetsReadOnlyFromRunning() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_FULL);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());
  }

  @Test
  public void applyDiskCrashSetsReadOnlyFromRunning() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(NodeStatus.DISK_CRASH, config.getStatusReason());
  }

  @Test
  public void applyDiskCrashUpgradesFromDiskFull() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_FULL);
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(NodeStatus.DISK_CRASH, config.getStatusReason());
  }

  @Test
  public void applyDiskFullDoesNotDowngradeDiskCrash() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_FULL);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(
        "DiskCrash must outrank DiskFull", NodeStatus.DISK_CRASH, config.getStatusReason());
  }

  @Test
  public void applyNormalRecoversFromDiskFull() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_FULL);
    DiskChecker.apply(DiskChecker.DiskStatus.NORMAL);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    assertEquals(NodeStatus.Running, config.getNodeStatus());
    assertNull(config.getStatusReason());
  }

  @Test
  public void applyNormalRecoversFromDiskCrash() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    DiskChecker.apply(DiskChecker.DiskStatus.NORMAL);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    assertEquals(NodeStatus.Running, config.getNodeStatus());
    assertNull(config.getStatusReason());
  }

  @Test
  public void applyLeavesNonDiskReadOnlyReasonUntouched() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    config.setNodeStatus(NodeStatus.ReadOnly);
    config.setStatusReason("ManualMaintenance");

    DiskChecker.apply(DiskChecker.DiskStatus.NORMAL);
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals("ManualMaintenance", config.getStatusReason());

    DiskChecker.apply(DiskChecker.DiskStatus.DISK_FULL);
    // DISK_FULL only fires when not already DiskFull/DiskCrash — it does take over here,
    // mirroring the existing behavior for the legacy sampleDiskLoad path.
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());
  }

  @Test
  public void applyIsIdempotentForRepeatedDiskCrash() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    NodeStatus before = CommonDescriptor.getInstance().getConfig().getNodeStatus();
    String reasonBefore = CommonDescriptor.getInstance().getConfig().getStatusReason();
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    assertEquals(before, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertEquals(reasonBefore, CommonDescriptor.getInstance().getConfig().getStatusReason());
  }

  @Test
  public void checkAndApplyDrivesStatusEndToEnd() throws Exception {
    File healthy = tmp.newFolder();
    DiskChecker.checkAndApply(Collections.singletonList(healthy.getAbsolutePath()), 0.0);
    assertEquals(NodeStatus.Running, CommonDescriptor.getInstance().getConfig().getNodeStatus());

    String missing = new File(tmp.getRoot(), "still-missing").getAbsolutePath();
    DiskChecker.checkAndApply(Collections.singletonList(missing), 0.0);
    assertEquals(NodeStatus.ReadOnly, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertEquals(
        NodeStatus.DISK_CRASH, CommonDescriptor.getInstance().getConfig().getStatusReason());

    DiskChecker.checkAndApply(Collections.singletonList(healthy.getAbsolutePath()), 0.0);
    assertEquals(NodeStatus.Running, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertNull(CommonDescriptor.getInstance().getConfig().getStatusReason());
  }

  @Test
  public void emptyDirListIsNormal() {
    assertEquals(DiskChecker.DiskStatus.NORMAL, DiskChecker.check(Collections.emptyList(), 1.0));
    assertEquals(DiskChecker.DiskStatus.NORMAL, DiskChecker.check(null, 1.0));
  }

  @Test
  public void smokeProbeFileIsDeleted() throws Exception {
    File dir = tmp.newFolder();
    DiskChecker.check(Collections.singletonList(dir.getAbsolutePath()), 0.0);
    File[] leftovers = dir.listFiles();
    assertTrue(
        "Disk probe should clean up its temp file", leftovers == null || leftovers.length == 0);
  }
}
