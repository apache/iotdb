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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigManagerShutdownReportTest {

  private static final TSStatus SUCCESS_STATUS = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);

  private static ConfigManager CONFIG_MANAGER_SPY;
  private static LoadManager LOAD_MANAGER;

  private static final int DATA_NODE_ID = 1;
  private static final int CONFIG_NODE_ID = 2;

  private static final TDataNodeLocation DATA_NODE_LOCATION =
      new TDataNodeLocation(
          DATA_NODE_ID,
          new TEndPoint("127.0.0.1", 2000),
          new TEndPoint("127.0.0.1", 2001),
          new TEndPoint("127.0.0.1", 2002),
          new TEndPoint("127.0.0.1", 2003),
          new TEndPoint("127.0.0.1", 2004));

  private static final TConfigNodeLocation CONFIG_NODE_LOCATION =
      new TConfigNodeLocation(
          CONFIG_NODE_ID, new TEndPoint("127.0.0.1", 1000), new TEndPoint("127.0.0.1", 1001));

  @BeforeClass
  public static void setUp() throws IOException {
    final ConfigManager configManager = new ConfigManager();
    CONFIG_MANAGER_SPY = spy(configManager);
    LOAD_MANAGER = mock(LoadManager.class);
    when(CONFIG_MANAGER_SPY.getLoadManager()).thenReturn(LOAD_MANAGER);
    doReturn(SUCCESS_STATUS).when(CONFIG_MANAGER_SPY).confirmLeader();
  }

  @Before
  public void clearPreviousInteractions() {
    // The shared LoadManager mock keeps interactions from previous tests; verification must only
    // consider calls from the current test.
    clearInvocations(LOAD_MANAGER);
    when(LOAD_MANAGER.trySetNodeStatus(anyInt(), any(NodeStatus.class), eq(false)))
        .thenReturn(SUCCESS_STATUS);
  }

  @Test
  public void testReportDataNodeShutdownUpdatesRemovingDataNodeThroughCache() {
    when(LOAD_MANAGER.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Removing);

    Assert.assertEquals(
        SUCCESS_STATUS.getCode(),
        CONFIG_MANAGER_SPY.reportDataNodeShutdown(DATA_NODE_LOCATION).getCode());

    verify(LOAD_MANAGER).trySetNodeStatus(eq(DATA_NODE_ID), eq(NodeStatus.Stopped), eq(false));
  }

  @Test
  public void testReportDataNodeShutdownRequestsStoppedStatus() {
    when(LOAD_MANAGER.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Stopped);

    Assert.assertEquals(
        SUCCESS_STATUS.getCode(),
        CONFIG_MANAGER_SPY.reportDataNodeShutdown(DATA_NODE_LOCATION).getCode());

    verify(LOAD_MANAGER).trySetNodeStatus(eq(DATA_NODE_ID), eq(NodeStatus.Stopped), eq(false));
  }

  @Test
  public void testReportConfigNodeShutdownUpdatesRemovingConfigNodeThroughCache() {
    when(LOAD_MANAGER.getNodeStatus(CONFIG_NODE_ID)).thenReturn(NodeStatus.Removing);

    Assert.assertEquals(
        SUCCESS_STATUS.getCode(),
        CONFIG_MANAGER_SPY.reportConfigNodeShutdown(CONFIG_NODE_LOCATION).getCode());

    verify(LOAD_MANAGER).trySetNodeStatus(eq(CONFIG_NODE_ID), eq(NodeStatus.Stopped), eq(false));
  }

  @Test
  public void testReportConfigNodeShutdownRequestsStoppedStatus() {
    when(LOAD_MANAGER.getNodeStatus(CONFIG_NODE_ID)).thenReturn(NodeStatus.Stopped);

    Assert.assertEquals(
        SUCCESS_STATUS.getCode(),
        CONFIG_MANAGER_SPY.reportConfigNodeShutdown(CONFIG_NODE_LOCATION).getCode());

    verify(LOAD_MANAGER).trySetNodeStatus(eq(CONFIG_NODE_ID), eq(NodeStatus.Stopped), eq(false));
  }

  @Test
  public void testReportDataNodeShutdownPropagatesPersistenceFailure() {
    TSStatus failure = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    when(LOAD_MANAGER.trySetNodeStatus(eq(DATA_NODE_ID), eq(NodeStatus.Stopped), eq(false)))
        .thenReturn(failure);

    for (NodeStatus currentStatus : new NodeStatus[] {NodeStatus.Running, NodeStatus.Removing}) {
      when(LOAD_MANAGER.getNodeStatus(DATA_NODE_ID)).thenReturn(currentStatus);
      Assert.assertSame(failure, CONFIG_MANAGER_SPY.reportDataNodeShutdown(DATA_NODE_LOCATION));
    }
    verify(LOAD_MANAGER, times(2))
        .trySetNodeStatus(eq(DATA_NODE_ID), eq(NodeStatus.Stopped), eq(false));
  }

  @Test
  public void testReportConfigNodeShutdownPropagatesPersistenceFailure() {
    TSStatus failure = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    when(LOAD_MANAGER.trySetNodeStatus(eq(CONFIG_NODE_ID), eq(NodeStatus.Stopped), eq(false)))
        .thenReturn(failure);

    for (NodeStatus currentStatus : new NodeStatus[] {NodeStatus.Running, NodeStatus.Removing}) {
      when(LOAD_MANAGER.getNodeStatus(CONFIG_NODE_ID)).thenReturn(currentStatus);
      Assert.assertSame(failure, CONFIG_MANAGER_SPY.reportConfigNodeShutdown(CONFIG_NODE_LOCATION));
    }
    verify(LOAD_MANAGER, times(2))
        .trySetNodeStatus(eq(CONFIG_NODE_ID), eq(NodeStatus.Stopped), eq(false));
  }
}
