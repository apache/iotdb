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
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TQueryAdmissionToken;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleQueryTerminatorTest {

  private ScheduledExecutorService scheduledExecutor;
  private SimpleQueryTerminator terminator;

  @Before
  public void setUp() {
    scheduledExecutor = mock(ScheduledExecutorService.class);
    final MPPQueryContext queryContext = mock(MPPQueryContext.class);
    when(queryContext.getQueryId()).thenReturn(new QueryId("test_query"));
    when(queryContext.getEndPointBlackList()).thenReturn(Collections.emptySet());
    terminator =
        new SimpleQueryTerminator(
            scheduledExecutor,
            queryContext,
            Collections.emptyList(),
            null,
            mock(IFragInstanceStateTracker.class),
            Collections::emptyMap);
  }

  @Test
  public void terminateWaitsForTheGracePeriod() {
    terminator.terminate(new RuntimeException("query failed"));

    verify(scheduledExecutor).schedule(any(Callable.class), eq(1000L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void terminateImmediatelySkipsTheGracePeriod() {
    terminator.terminateImmediately(new RuntimeException("dispatch failed"));

    verify(scheduledExecutor).schedule(any(Callable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void closesAttemptEvenWhenNoFragmentIsReportedUnfinished() throws Exception {
    final MPPQueryContext queryContext = mock(MPPQueryContext.class);
    final QueryId queryId = new QueryId("attempt_to_close");
    when(queryContext.getQueryId()).thenReturn(queryId);
    when(queryContext.getQueryAttemptId()).thenReturn(2);
    when(queryContext.getEndPointBlackList()).thenReturn(Collections.emptySet());
    final TEndPoint endpoint = new TEndPoint("remote-host", 10730);
    final TQueryAdmissionToken token = new TQueryAdmissionToken("receiver", 123456L, 1000L);
    final FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getHostDataNode())
        .thenReturn(new TDataNodeLocation().setInternalEndPoint(endpoint));
    when(instance.getId()).thenReturn(new FragmentInstanceId(new PlanFragmentId(queryId, 0), "0"));
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        mock(IClientManager.class);
    final SyncDataNodeInternalServiceClient client = mock(SyncDataNodeInternalServiceClient.class);
    when(clients.borrowClient(endpoint)).thenReturn(client);
    final IFragInstanceStateTracker tracker = mock(IFragInstanceStateTracker.class);
    when(tracker.filterUnFinishedFIs(any())).thenReturn(Collections.emptyList());
    final SimpleQueryTerminator terminator =
        new SimpleQueryTerminator(
            scheduledExecutor,
            queryContext,
            Collections.singletonList(instance),
            clients,
            tracker,
            () -> Collections.singletonMap(endpoint, token));
    // A delayed termination must retain the old attempt, even after the context starts a retry.
    when(queryContext.getQueryAttemptId()).thenReturn(3);
    terminator.terminateImmediately(new RuntimeException("dispatch failure"));
    terminator.syncTerminate();
    ArgumentCaptor<TCancelQueryReq> request = ArgumentCaptor.forClass(TCancelQueryReq.class);
    verify(client).cancelQuery(request.capture());
    assertEquals(2, request.getValue().getQueryAttemptId());
    assertEquals(token, request.getValue().getAdmissionToken());
    assertTrue(request.getValue().getFragmentInstanceIds().isEmpty());
  }
}
