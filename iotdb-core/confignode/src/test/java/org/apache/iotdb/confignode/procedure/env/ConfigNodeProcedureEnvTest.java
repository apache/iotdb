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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.DataNodeTransportFailureUtils;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigNodeProcedureEnvTest {

  private static final int DATA_NODE_ID = 1;

  private final ConfigManager configManager = mock(ConfigManager.class);
  private final LoadManager loadManager = mock(LoadManager.class);
  private final ConfigNodeProcedureEnv env =
      new ConfigNodeProcedureEnv(configManager, mock(ProcedureScheduler.class));

  @Before
  public void setUp() {
    when(configManager.getLoadManager()).thenReturn(loadManager);
  }

  @Test
  public void testStoppedNodeIsNotRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Stopped);

    // A Stopped node is handled like Unknown: it can not serve as an active runtime writer.
    Assert.assertFalse(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testUnknownNodeIsNotRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Unknown);

    Assert.assertFalse(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testRemovingNodeIsNotRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Removing);

    Assert.assertFalse(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testRunningNodeIsRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Running);

    Assert.assertTrue(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testReadOnlyNodeIsRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.ReadOnly);

    // ReadOnly still responds and can serve as a runtime writer, unlike Unknown/Stopped/Removing.
    Assert.assertTrue(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testNegativeNodeIdIsNotRuntimeActiveWriter() {
    // The node id guard dominates the status check: even a Running status can not turn an
    // invalid node id into an active writer.
    when(loadManager.getNodeStatus(-1)).thenReturn(NodeStatus.Running);

    Assert.assertFalse(env.isRuntimeActiveWriterNode(-1));
  }

  @Test
  public void testMissingPipeResponsesAreMarkedAsTransportTimeouts() throws Exception {
    final DataNodeAsyncRequestContext<Object, TPushPipeMetaResp> context =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_PUSH_MULTI_META,
            Map.of(1, new TDataNodeLocation(), 2, new TDataNodeLocation()));
    final TPushPipeMetaResp rejected =
        new TPushPipeMetaResp(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()));
    context.getResponseMap().put(2, rejected);

    fillMissingResponses("fillMissingPipePushMetaResponses", context);

    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportFailure(
            context.getResponseMap().get(1).getStatus()));
    Assert.assertSame(rejected, context.getResponseMap().get(2));
    Assert.assertFalse(DataNodeTransportFailureUtils.isTransportFailure(rejected.getStatus()));
  }

  @Test
  public void testMissingConsumerGroupResponsesAreMarkedAsTransportTimeouts() throws Exception {
    final DataNodeAsyncRequestContext<Object, TPushConsumerGroupMetaResp> context =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
            Map.of(1, new TDataNodeLocation(), 2, new TDataNodeLocation()));
    final TPushConsumerGroupMetaResp rejected =
        new TPushConsumerGroupMetaResp(
            new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()));
    context.getResponseMap().put(2, rejected);

    fillMissingResponses("fillMissingConsumerGroupPushMetaResponses", context);

    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportFailure(
            context.getResponseMap().get(1).getStatus()));
    Assert.assertSame(rejected, context.getResponseMap().get(2));
    Assert.assertFalse(DataNodeTransportFailureUtils.isTransportFailure(rejected.getStatus()));
  }

  private static void fillMissingResponses(
      final String methodName, final DataNodeAsyncRequestContext<?, ?> context) throws Exception {
    final Method method =
        ConfigNodeProcedureEnv.class.getDeclaredMethod(
            methodName, DataNodeAsyncRequestContext.class, long.class);
    method.setAccessible(true);
    method.invoke(null, context, 1L);
  }
}
