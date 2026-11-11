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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FragmentInstanceDispatcherLifecycleTest {
  @Test
  public void abortPreventsDispatchAndRetainsOriginalAttemptMetadata() throws Exception {
    QueryId queryId = new QueryId("aborted_dispatch");
    MPPQueryContext context = new MPPQueryContext(queryId);
    context.setStartTime(1000);
    context.setTimeOut(2000);
    FragmentInstanceDispatcherImpl dispatcher =
        new FragmentInstanceDispatcherImpl(QueryType.READ, context, null, null);
    context.prepareForRetry();
    FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getId()).thenReturn(new FragmentInstanceId(new PlanFragmentId(queryId, 0), "0"));
    PlanFragment fragment = mock(PlanFragment.class);
    when(instance.getFragment()).thenReturn(fragment);
    dispatcher.abort();
    FragInstanceDispatchResult result =
        dispatcher.dispatch(null, Collections.singletonList(instance)).get();
    assertFalse(result.isSuccessful());
    assertEquals(
        TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), result.getFailureStatus().getCode());
    verify(instance, never()).getHostDataNode();
    verify(instance).setQueryAttempt(0, 3000);
  }

  @Test
  public void transportRetryReusesAdmissionDeadlineAndStopFreezesCutoff() throws Exception {
    QueryId queryId = new QueryId("retry_admission");
    MPPQueryContext context = new MPPQueryContext(queryId);
    context.setStartTime(System.currentTimeMillis());
    context.setTimeOut(Long.MAX_VALUE);
    PlanFragment fragment = mock(PlanFragment.class);
    FragmentInstance instance =
        spy(
            new FragmentInstance(
                fragment,
                new FragmentInstanceId(new PlanFragmentId(queryId, 0), "0"),
                null,
                QueryType.READ,
                Long.MAX_VALUE,
                null,
                false,
                false));
    doReturn(ByteBuffer.wrap(new byte[] {1})).when(instance).serializeToByteBuffer();
    TEndPoint endpoint = new TEndPoint("remote-host", 10730);
    doReturn(new TDataNodeLocation().setInternalEndPoint(endpoint))
        .when(instance)
        .getHostDataNode();
    instance.setExecutorType(mock(ExecutorType.class));
    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        mock(IClientManager.class);
    SyncDataNodeInternalServiceClient client = mock(SyncDataNodeInternalServiceClient.class);
    when(clients.borrowClient(endpoint)).thenReturn(client);
    List<TSendFragmentInstanceReq> requests = new ArrayList<>();
    when(client.sendFragmentInstance(any()))
        .thenAnswer(
            invocation -> {
              TSendFragmentInstanceReq request = invocation.getArgument(0);
              requests.add(request.deepCopy());
              if (requests.size() == 1) {
                throw new TException("response lost");
              }
              return new TSendFragmentInstanceResp(true);
            });
    FragmentInstanceDispatcherImpl dispatcher =
        new FragmentInstanceDispatcherImpl(QueryType.READ, context, clients, null);
    assertTrue(dispatcher.dispatch(null, Collections.singletonList(instance)).get().isSuccessful());
    assertEquals(2, requests.size());
    assertEquals(Long.MAX_VALUE, requests.get(0).getQueryDeadline());
    assertTrue(requests.get(0).getAdmissionDeadline() < Long.MAX_VALUE);
    assertEquals(requests.get(0).getAdmissionDeadline(), requests.get(1).getAdmissionDeadline());
    long cutoff = dispatcher.getLastAdmissionDeadline();
    dispatcher.abort();
    assertFalse(
        dispatcher.dispatch(null, Collections.singletonList(instance)).get().isSuccessful());
    assertEquals(2, requests.size());
    assertEquals(cutoff, dispatcher.getLastAdmissionDeadline());
  }
}
