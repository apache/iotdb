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

package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.cluster.NodeStatus;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class CommonConfigTest {

  @Test
  public void testSubscriptionDisabledInCommonConfig() {
    assertFalse(CommonConfig.SUBSCRIPTION_ENABLED);
    assertFalse(new CommonConfig().getSubscriptionEnabled());
  }

  @Test
  public void testSubscriptionIsNotExposedInConfigurationTemplate() throws IOException {
    assertNull(ConfigurationFileUtils.getConfigurationDefaultValue("subscription_enabled"));
  }

  @Test
  public void testConsensusSubscriptionCatchUpBatchDefaults() {
    final CommonConfig config = new CommonConfig();

    Assert.assertEquals(512, config.getSubscriptionConsensusBatchMaxTabletCount());
    Assert.assertEquals(1024, config.getSubscriptionConsensusBatchMaxWalEntries());
  }

  @Test
  public void testSameNodeStatusDoesNotClearStatusReason() {
    CommonConfig config = new CommonConfig();
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);

    config.setNodeStatus(NodeStatus.ReadOnly);

    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());
  }

  @Test
  public void testSetReadOnlyWithReasonPriority() {
    CommonConfig config = new CommonConfig();

    // DiskFull can enter from Running.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());

    // UnrecoverableError can override DiskFull.
    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 10:00:00.000, first");
    Assert.assertEquals(
        "UnrecoverableError, 2026-09-02 10:00:00.000, first", config.getStatusReason());

    // A second UnrecoverableError keeps the first reason.
    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 11:00:00.000, second");
    Assert.assertEquals(
        "UnrecoverableError, 2026-09-02 10:00:00.000, first", config.getStatusReason());

    // Manual overrides UnrecoverableError.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    Assert.assertEquals(NodeStatus.MANUAL, config.getStatusReason());

    // DiskFull cannot override Manual.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    Assert.assertEquals(NodeStatus.MANUAL, config.getStatusReason());

    // UnrecoverableError cannot override Manual either.
    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 12:00:00.000, third");
    Assert.assertEquals(NodeStatus.MANUAL, config.getStatusReason());

    // Stopping overrides Manual.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.STOPPING);
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    // Manual cannot override Stopping.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    // Unknown/Removing are always overridden, as in the pre-reason behavior.
    config.setNodeStatus(NodeStatus.Unknown);
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());

    config.setNodeStatus(NodeStatus.Removing);
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.MANUAL, config.getStatusReason());
  }

  @Test
  public void testStoppingOverridesUnrecoverableError() {
    CommonConfig config = new CommonConfig();

    // Stopping is the highest-priority reason: it overrides UnrecoverableError as well.
    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 10:00:00.000, broken");
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.STOPPING);
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    // And an UnrecoverableError can not override Stopping afterwards.
    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 11:00:00.000, broken again");
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());
  }

  @Test
  public void testClassifiedReasonOverridesUnclassifiedReadOnly() {
    CommonConfig config = new CommonConfig();

    // A null-reason ReadOnly can be entered from Running...
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, null);
    Assert.assertNull(config.getStatusReason());

    // ...but any classified reason overrides it.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());

    config.setNodeStatusWithReason(NodeStatus.ReadOnly, null);
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());

    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 10:00:00.000, broken");
    Assert.assertEquals(
        "UnrecoverableError, 2026-09-02 10:00:00.000, broken", config.getStatusReason());

    config.setNodeStatusWithReason(NodeStatus.ReadOnly, null);
    Assert.assertEquals(
        "UnrecoverableError, 2026-09-02 10:00:00.000, broken", config.getStatusReason());
  }

  @Test
  public void testDiskFullRecoveryRequiresEverySourceToRecover() {
    CommonConfig config = new CommonConfig();
    AtomicBoolean dataAvailable = new AtomicBoolean(true);
    AtomicBoolean snapshotAvailable = new AtomicBoolean(false);
    AtomicInteger dataChecks = new AtomicInteger();
    AtomicInteger snapshotChecks = new AtomicInteger();
    config.setReadOnlyForDiskFull(
        "data",
        () -> {
          dataChecks.incrementAndGet();
          return dataAvailable.get();
        });
    config.setReadOnlyForDiskFull(
        "snapshot",
        () -> {
          snapshotChecks.incrementAndGet();
          return snapshotAvailable.get();
        });

    config.tryRecoverFromDiskFull();
    Assert.assertEquals(1, snapshotChecks.get());
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());

    dataAvailable.set(false);
    snapshotAvailable.set(true);
    int previousDataChecks = dataChecks.get();
    config.tryRecoverFromDiskFull();
    Assert.assertEquals(previousDataChecks + 1, dataChecks.get());
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, config.getStatusReason());

    dataAvailable.set(true);
    previousDataChecks = dataChecks.get();
    int previousSnapshotChecks = snapshotChecks.get();
    config.tryRecoverFromDiskFull();
    Assert.assertEquals(previousDataChecks + 1, dataChecks.get());
    Assert.assertEquals(previousSnapshotChecks + 1, snapshotChecks.get());
    Assert.assertEquals(NodeStatus.Running, config.getNodeStatus());
    Assert.assertNull(config.getStatusReason());
  }

  @Test
  public void testRepeatedDiskFullSourceReplacesItsRecoveryCheck() {
    CommonConfig config = new CommonConfig();
    Object source = new Object();
    AtomicInteger previousChecks = new AtomicInteger();
    AtomicInteger latestChecks = new AtomicInteger();
    config.setReadOnlyForDiskFull(
        source,
        () -> {
          previousChecks.incrementAndGet();
          return false;
        });
    config.setReadOnlyForDiskFull(
        source,
        () -> {
          latestChecks.incrementAndGet();
          return true;
        });

    config.tryRecoverFromDiskFull();

    Assert.assertEquals(0, previousChecks.get());
    Assert.assertEquals(1, latestChecks.get());
    Assert.assertEquals(NodeStatus.Running, config.getNodeStatus());
    Assert.assertNull(config.getStatusReason());
  }

  @Test
  public void testDiskRecoveryCannotClearConcurrentHigherPriorityReason() {
    for (String reason :
        new String[] {NodeStatus.MANUAL, NodeStatus.UNRECOVERABLE_ERROR + ", broken"}) {
      CommonConfig config = new CommonConfig();
      AtomicInteger checks = new AtomicInteger();
      config.setReadOnlyForDiskFull(
          "data",
          () -> {
            checks.incrementAndGet();
            config.setNodeStatusWithReason(NodeStatus.ReadOnly, reason);
            return true;
          });

      config.tryRecoverFromDiskFull();
      Assert.assertEquals(1, checks.get());
      Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
      Assert.assertEquals(reason, config.getStatusReason());

      config.tryRecoverFromDiskFull();
      Assert.assertEquals(1, checks.get());
      Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
      Assert.assertEquals(reason, config.getStatusReason());
    }
  }

  @Test
  public void testManualRunningClearsPreviousDiskRecoveryChecks() {
    CommonConfig config = new CommonConfig();
    AtomicInteger previousChecks = new AtomicInteger();
    AtomicInteger currentChecks = new AtomicInteger();
    config.setReadOnlyForDiskFull(
        "old data",
        () -> {
          previousChecks.incrementAndGet();
          return false;
        });
    config.tryRecoverFromDiskFull();
    Assert.assertEquals(1, previousChecks.get());
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());

    config.setNodeStatus(NodeStatus.Running);
    Assert.assertEquals(NodeStatus.Running, config.getNodeStatus());
    Assert.assertNull(config.getStatusReason());
    config.setReadOnlyForDiskFull(
        "new data",
        () -> {
          currentChecks.incrementAndGet();
          return true;
        });
    config.tryRecoverFromDiskFull();

    Assert.assertEquals(1, previousChecks.get());
    Assert.assertEquals(1, currentChecks.get());
    Assert.assertEquals(NodeStatus.Running, config.getNodeStatus());
    Assert.assertNull(config.getStatusReason());
  }

  @Test
  public void testHandleUnrecoverableErrorBuildsStatusReason() {
    // HandleSystemErrorStrategy writes the singleton config held by CommonDescriptor.
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    NodeStatus originalStatus = config.getNodeStatus();
    String originalStatusReason = config.getStatusReason();
    try {
      config.setNodeStatus(NodeStatus.Running);
      config.handleUnrecoverableError(
          new IOException(" \r\n  disk\t broken\r\n\t|\u00a0page\u2028fault\u0085at\b42 \t"));
      Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
      Assert.assertTrue(config.getStatusReason().startsWith(NodeStatus.UNRECOVERABLE_ERROR + ", "));
      Assert.assertTrue(config.getStatusReason().endsWith(", disk broken | page fault at 42"));
    } finally {
      config.setNodeStatus(NodeStatus.Running);
      config.setNodeStatusWithReason(originalStatus, originalStatusReason);
    }
  }

  @Test
  public void testSetNodeStatusWithReasonClearsReasonForNonReadOnly() {
    CommonConfig config = new CommonConfig();

    for (NodeStatus status : NodeStatus.values()) {
      if (status == NodeStatus.ReadOnly) {
        continue;
      }
      config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
      config.setNodeStatusWithReason(status, NodeStatus.DISK_FULL);
      Assert.assertEquals(status, config.getNodeStatus());
      Assert.assertNull(config.getStatusReason());

      // Repeated updates cannot attach a ReadOnly reason to another status either.
      config.setNodeStatusWithReason(status, NodeStatus.MANUAL);
      Assert.assertNull(config.getStatusReason());
    }
  }

  @Test
  public void testNullReadOnlyReasonKeepsLegacySemantics() {
    CommonConfig config = new CommonConfig();

    // A null reason can only enter from a non-ReadOnly status.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, null);
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertNull(config.getStatusReason());

    // ...and can never override a classified reason.
    config.setNodeStatus(NodeStatus.Running);
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, null);
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.MANUAL, config.getStatusReason());
  }

  @Test
  public void testHandleUnrecoverableErrorFallsBackToClassName() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    NodeStatus originalStatus = config.getNodeStatus();
    String originalStatusReason = config.getStatusReason();
    try {
      config.setNodeStatus(NodeStatus.Running);
      config.handleUnrecoverableError(new RuntimeException());
      Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
      Assert.assertTrue(config.getStatusReason().endsWith(", RuntimeException"));
      config.setNodeStatus(NodeStatus.Running);
      config.handleUnrecoverableError(new IOException("\t\r\n\u3000"));
      Assert.assertTrue(config.getStatusReason().endsWith(", IOException"));
    } finally {
      config.setNodeStatus(NodeStatus.Running);
      config.setNodeStatusWithReason(originalStatus, originalStatusReason);
    }
  }

  @Test
  public void testHandleUnrecoverableErrorTruncatesLongMessage() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    NodeStatus originalStatus = config.getNodeStatus();
    String originalStatusReason = config.getStatusReason();
    try {
      config.setNodeStatus(NodeStatus.Running);
      String prefix = "x".repeat(254);
      String exactFit = prefix + " y";
      config.handleUnrecoverableError(new IOException(prefix + "\t".repeat(300) + "y \n"));
      Assert.assertTrue(config.getStatusReason().endsWith(", " + exactFit));

      config.setNodeStatus(NodeStatus.Running);
      config.handleUnrecoverableError(new IOException(exactFit + "z"));
      Assert.assertTrue(config.getStatusReason().endsWith(", " + exactFit + "..."));

      config.setNodeStatus(NodeStatus.Running);
      config.handleUnrecoverableError(new IOException(prefix + "x\uD83D\uDE00"));
      Assert.assertTrue(config.getStatusReason().endsWith(", " + prefix + "x..."));
    } finally {
      config.setNodeStatus(NodeStatus.Running);
      config.setNodeStatusWithReason(originalStatus, originalStatusReason);
    }
  }

  @Test
  public void testStoppingEntersReadOnlyFromRunningAndBlocksAllOtherReasons() {
    CommonConfig config = new CommonConfig();

    // The shutdown hook performs exactly this transition: from Running into ReadOnly with the
    // Stopping reason. The state is transient in the real shutdown sequence, so the entry is
    // exercised here directly instead of racing it.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.STOPPING);
    Assert.assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    // Stopping has the highest priority: no classified reason can override it.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    config.setNodeStatusWithReason(
        NodeStatus.ReadOnly, "UnrecoverableError, 2026-09-02 10:00:00.000, broken");
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    // ...nor can an unclassified reason.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, null);
    Assert.assertEquals(NodeStatus.STOPPING, config.getStatusReason());

    // Only an explicit management change leaving ReadOnly clears the reason.
    config.setNodeStatus(NodeStatus.Running);
    Assert.assertEquals(NodeStatus.Running, config.getNodeStatus());
    Assert.assertNull(config.getStatusReason());
  }
}
