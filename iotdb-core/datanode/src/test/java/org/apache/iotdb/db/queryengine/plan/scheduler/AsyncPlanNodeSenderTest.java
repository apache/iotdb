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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;

import org.apache.thrift.TException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncPlanNodeSenderTest {

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnClientWhenDispatchThrowsSynchronously() throws Exception {
    final TEndPoint endPoint = new TEndPoint("127.0.0.1", 10730);
    final ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager =
        mock(ClientManager.class);
    final AsyncDataNodeInternalServiceClient client =
        mock(AsyncDataNodeInternalServiceClient.class);
    when(clientManager.borrowClient(endPoint)).thenReturn(client);
    doThrow(new TException("dispatch failed")).when(client).sendBatchPlanNode(any(), any());

    new AsyncPlanNodeSender(
            clientManager, Collections.singletonList(createFragmentInstance(endPoint)))
        .sendAll();

    verify(clientManager).returnClient(endPoint, client);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldLeaveClientReturnToAsyncCallbackAfterSuccessfulDispatch() throws Exception {
    final TEndPoint endPoint = new TEndPoint("127.0.0.1", 10730);
    final ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager =
        mock(ClientManager.class);
    final AsyncDataNodeInternalServiceClient client =
        mock(AsyncDataNodeInternalServiceClient.class);
    when(clientManager.borrowClient(endPoint)).thenReturn(client);

    new AsyncPlanNodeSender(
            clientManager, Collections.singletonList(createFragmentInstance(endPoint)))
        .sendAll();

    verify(clientManager, never()).returnClient(endPoint, client);
  }

  private static FragmentInstance createFragmentInstance(TEndPoint endPoint) {
    final PlanNode planNode = mock(PlanNode.class);
    when(planNode.serializeToByteBuffer()).thenReturn(ByteBuffer.allocate(0));
    final PlanFragment fragment = mock(PlanFragment.class);
    when(fragment.getPlanNodeTree()).thenReturn(planNode);

    final FragmentInstance fragmentInstance = mock(FragmentInstance.class);
    when(fragmentInstance.getHostDataNode())
        .thenReturn(new TDataNodeLocation().setInternalEndPoint(endPoint));
    when(fragmentInstance.getFragment()).thenReturn(fragment);
    when(fragmentInstance.getRegionReplicaSet())
        .thenReturn(
            new TRegionReplicaSet()
                .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1)));
    return fragmentInstance;
  }
}
