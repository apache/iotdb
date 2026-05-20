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
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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

  // -- checkFreeRatio ------------------------------------------------------------------------

  @Test
  public void checkFreeRatioReturnsNormalWhenRatioMeetsThreshold() throws Exception {
    File dir = tmp.newFolder();
    assertEquals(
        DiskChecker.DiskStatus.NORMAL,
        DiskChecker.checkFreeRatio(Collections.singletonList(dir.getAbsolutePath()), 0.0));
  }

  @Test
  public void checkFreeRatioReturnsDiskFullWhenAnyDirBelowThreshold() throws Exception {
    File dir = tmp.newFolder();
    // threshold above 1.0 forces any directory with positive total space to register as full
    assertEquals(
        DiskChecker.DiskStatus.DISK_FULL,
        DiskChecker.checkFreeRatio(Collections.singletonList(dir.getAbsolutePath()), 2.0));
  }

  @Test
  public void checkFreeRatioSkipsNullAndEmptyEntries() throws Exception {
    File dir = tmp.newFolder();
    assertEquals(
        DiskChecker.DiskStatus.NORMAL,
        DiskChecker.checkFreeRatio(Arrays.asList(null, "", dir.getAbsolutePath()), 0.0));
  }

  @Test
  public void emptyDirListIsNormal() {
    assertEquals(
        DiskChecker.DiskStatus.NORMAL, DiskChecker.checkFreeRatio(Collections.emptyList(), 1.0));
    assertEquals(DiskChecker.DiskStatus.NORMAL, DiskChecker.checkFreeRatio(null, 1.0));
  }

  // -- apply() state machine -----------------------------------------------------------------

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
  public void applyNormalDoesNotRecoverFromDiskCrash() {
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    DiskChecker.apply(DiskChecker.DiskStatus.NORMAL);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    // DiskCrash is sticky: a free-ratio probe (the only source of NORMAL) cannot prove writes
    // work again, so the node stays ReadOnly(DiskCrash) until restart.
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(NodeStatus.DISK_CRASH, config.getStatusReason());
  }

  @Test
  public void applyLeavesNonDiskReadOnlyReasonUntouched() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    config.setNodeStatus(NodeStatus.ReadOnly);
    config.setStatusReason("ManualMaintenance");

    DiskChecker.apply(DiskChecker.DiskStatus.NORMAL);
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals("ManualMaintenance", config.getStatusReason());

    // A non-disk ReadOnly reason is not guarded, so DISK_FULL takes it over.
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_FULL);
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

  // -- checkFreeRatioAndApply ----------------------------------------------------------------

  @Test
  public void checkFreeRatioAndApplyDrivesStatusEndToEnd() throws Exception {
    File dir = tmp.newFolder();
    // Threshold above 1.0 -> always "DiskFull"
    DiskChecker.checkFreeRatioAndApply(Collections.singletonList(dir.getAbsolutePath()), 2.0);
    assertEquals(NodeStatus.ReadOnly, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertEquals(
        NodeStatus.DISK_FULL, CommonDescriptor.getInstance().getConfig().getStatusReason());

    // Threshold 0.0 -> always "Normal" -> recovery
    DiskChecker.checkFreeRatioAndApply(Collections.singletonList(dir.getAbsolutePath()), 0.0);
    assertEquals(NodeStatus.Running, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertNull(CommonDescriptor.getInstance().getConfig().getStatusReason());
  }

  @Test
  public void checkFreeRatioAndApplyDoesNotClearDiskCrash() throws Exception {
    File dir = tmp.newFolder();
    // Simulate a Ratis-passive DiskCrash signal.
    DiskChecker.apply(DiskChecker.DiskStatus.DISK_CRASH);
    // Subsequent healthy free-ratio polling must keep the node in ReadOnly(DiskCrash).
    DiskChecker.checkFreeRatioAndApply(Collections.singletonList(dir.getAbsolutePath()), 0.0);
    assertEquals(NodeStatus.ReadOnly, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertEquals(
        NodeStatus.DISK_CRASH, CommonDescriptor.getInstance().getConfig().getStatusReason());
  }
}
