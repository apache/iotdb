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

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryFailedTasksThreadTest {

  private static final int STOPPED_DATA_NODE_ID = 1;

  private final IManager configManager = mock(IManager.class);
  private final NodeManager nodeManager = mock(NodeManager.class);
  private final LoadManager loadManager = mock(LoadManager.class);

  private final TDataNodeConfiguration stoppedDataNode =
      new TDataNodeConfiguration()
          .setLocation(
              new TDataNodeLocation(
                  STOPPED_DATA_NODE_ID,
                  new TEndPoint("127.0.0.1", 2000),
                  new TEndPoint("127.0.0.1", 2001),
                  new TEndPoint("127.0.0.1", 2002),
                  new TEndPoint("127.0.0.1", 2003),
                  new TEndPoint("127.0.0.1", 2004)));

  private RetryFailedTasksThread retryFailedTasksThread;

  // The thread captures its managers at construction time, so it must be created after the stubs.
  private void createThreadWithStoppedDataNode() {
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(nodeManager.getRegisteredDataNodes())
        .thenReturn(Collections.singletonList(stoppedDataNode));
    when(loadManager.getNodeStatus(STOPPED_DATA_NODE_ID)).thenReturn(NodeStatus.Stopped);
    when(configManager.transfer(anyList()))
        .thenReturn(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    retryFailedTasksThread = new RetryFailedTasksThread(configManager);
  }

  private void invokeTriggerDetectTask() throws Exception {
    final Method method = RetryFailedTasksThread.class.getDeclaredMethod("triggerDetectTask");
    method.setAccessible(true);
    method.invoke(retryFailedTasksThread);
  }

  @Test
  public void testStoppedDataNodeTriggersRegionTransfer() throws Exception {
    createThreadWithStoppedDataNode();

    invokeTriggerDetectTask();

    // A Stopped node is handled like Unknown: its regions are transferred.
    verify(configManager)
        .transfer(
            argThat(
                (List<TDataNodeLocation> nodes) ->
                    nodes.size() == 1 && nodes.get(0).getDataNodeId() == STOPPED_DATA_NODE_ID));
  }

  @Test
  public void testContinuingStoppedDataNodeIsNotTransferredTwice() throws Exception {
    createThreadWithStoppedDataNode();

    invokeTriggerDetectTask();
    invokeTriggerDetectTask();

    // Like a continuing Unknown node, a continuing Stopped node only triggers one transfer.
    verify(configManager, times(1)).transfer(anyList());
  }
}
