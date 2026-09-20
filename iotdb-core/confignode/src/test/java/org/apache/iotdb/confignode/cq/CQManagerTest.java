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
package org.apache.iotdb.confignode.cq;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.cq.CQScheduleTask;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.rpc.thrift.TCQDuration;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CQManagerTest {

  @SuppressWarnings("unchecked")
  @Test
  public void dropCQShouldCancelLocallyScheduledTask() throws Exception {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    CQManager cqManager = new CQManager(configManager);
    ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
    CQScheduleTask task = newScheduledTask(configManager, future, "token");

    try {
      assertTrue(cqManager.markCQLocallyScheduled("testCq", "token", task));
      task.submitSelf();
      cqManager.dropCQ(new TDropCQReq("testCq"));

      Mockito.verify(future).cancel(false);
    } finally {
      cqManager.stopCQScheduler();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void newTokenShouldCancelPreviousLocallyScheduledTask() {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    CQManager cqManager = new CQManager(configManager);
    ScheduledFuture<?> previousFuture = Mockito.mock(ScheduledFuture.class);
    CQScheduleTask previousTask = newScheduledTask(configManager, previousFuture, "previousToken");
    ScheduledFuture<?> currentFuture = Mockito.mock(ScheduledFuture.class);
    CQScheduleTask currentTask = newScheduledTask(configManager, currentFuture, "currentToken");

    try {
      assertTrue(cqManager.markCQLocallyScheduled("testCq", "previousToken", previousTask));
      previousTask.submitSelf();
      assertTrue(cqManager.markCQLocallyScheduled("testCq", "currentToken", currentTask));

      Mockito.verify(previousFuture).cancel(false);
    } finally {
      cqManager.stopCQScheduler();
    }
  }

  @Test
  public void mixedCalendarAndFixedDurationsReachCapabilityValidation() {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    Mockito.when(configManager.getNodeManager()).thenReturn(nodeManager);
    Mockito.when(nodeManager.getNodeVersionInfo()).thenReturn(java.util.Collections.emptyMap());
    CQManager cqManager = new CQManager(configManager);

    TCreateCQReq req =
        new TCreateCQReq(
            "mixedDurationCq",
            0,
            0,
            0,
            0,
            TimeoutPolicy.BLOCKED.getType(),
            "select 1",
            "create cq mixedDurationCq",
            "UTC",
            "root");
    req.setDurationEncodingVersion((short) 1);
    req.setEveryDuration(new TCQDuration(0, 86_400_000));
    req.setStartOffsetDuration(new TCQDuration(1, 86_400_000));
    req.setEndOffsetDuration(new TCQDuration(0, 0));
    req.setBoundaryExplicit(true);

    try {
      TSStatus status = cqManager.createCQ(req);
      assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), status.getCode());
      assertEquals(
          org.apache.iotdb.confignode.i18n.ManagerMessages
              .MESSAGE_CQ_CALENDAR_DURATION_REQUIRES_ALL_NODES_SUPPORT_49534072,
          status.getMessage());
    } finally {
      cqManager.stopCQScheduler();
    }
  }

  @Test
  public void markerlessCreateCQIsRejectedAtRpcIngress() {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    CQManager cqManager = new CQManager(configManager);
    try {
      TCreateCQReq req =
          new TCreateCQReq(
              "legacyCq",
              1_000,
              0,
              1_000,
              0,
              TimeoutPolicy.BLOCKED.getType(),
              "select 1",
              "create cq legacyCq",
              "UTC",
              "root");
      TSStatus status = cqManager.createCQ(req);
      assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), status.getCode());
      assertEquals(
          org.apache.iotdb.confignode.i18n.ManagerMessages
              .MESSAGE_CQ_DURATION_ENCODING_MARKER_REQUIRED_9035980A,
          status.getMessage());
    } finally {
      cqManager.stopCQScheduler();
    }
  }

  @Test
  public void invalidStructuredDurationIsRejectedAtRpcIngress() {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    CQManager cqManager = new CQManager(configManager);
    try {
      TCreateCQReq req =
          new TCreateCQReq(
              "invalidDurationCq",
              0,
              0,
              0,
              0,
              TimeoutPolicy.BLOCKED.getType(),
              "select 1",
              "create cq invalidDurationCq",
              "UTC",
              "root");
      req.setDurationEncodingVersion((short) 1);
      req.setEveryDuration(new TCQDuration(0, 0));
      req.setStartOffsetDuration(new TCQDuration(0, 0));
      req.setEndOffsetDuration(new TCQDuration(0, 0));
      req.setBoundaryExplicit(true);

      TSStatus status = cqManager.createCQ(req);
      assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), status.getCode());
      assertEquals(
          org.apache.iotdb.confignode.i18n.ManagerMessages
              .EXCEPTION_CQ_EVERY_DURATION_MUST_BE_POSITIVE_69C29D26,
          status.getMessage());
    } finally {
      cqManager.stopCQScheduler();
    }
  }

  @SuppressWarnings("unchecked")
  private CQScheduleTask newScheduledTask(
      ConfigManager configManager, ScheduledFuture<?> scheduledFuture, String cqToken) {
    ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(
            executor.schedule(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
        .thenReturn((ScheduledFuture) scheduledFuture);
    return new CQScheduleTask(
        "testCq",
        1000,
        0,
        1000,
        TimeoutPolicy.BLOCKED,
        "select s1 into root.backup.d1.s1 from root.sg.d1",
        cqToken,
        "Asia",
        "root",
        executor,
        configManager,
        System.currentTimeMillis() + 10_000);
  }
}
