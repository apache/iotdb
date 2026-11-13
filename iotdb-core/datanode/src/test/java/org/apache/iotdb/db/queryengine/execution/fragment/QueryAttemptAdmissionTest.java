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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragmentInstanceDispatcherImpl;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class QueryAttemptAdmissionTest {
  @Test
  public void fullSocketTimeoutStillAllowsTheReconnectRetry() throws Exception {
    assertTransportTimeout(20, true);
  }

  @Test
  public void expiredAdmissionIsRetryableInsteadOfReportingQueryTimeout() throws Exception {
    assertTransportTimeout(41, false);
  }

  private void assertTransportTimeout(long firstRpcElapsedMs, boolean expectedSuccess)
      throws Exception {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    int previousTimeout = config.getDnConnectionTimeoutInMS();
    config.setDnConnectionTimeoutInMS(20);
    try {
      QueryId queryId = new QueryId("socket_timeout_admission");
      MPPQueryContext context = new MPPQueryContext(queryId);
      context.setStartTime(System.currentTimeMillis());
      context.setTimeOut(TimeUnit.HOURS.toMillis(1));
      // An arbitrary receiver monotonic origin, unrelated to the coordinator's wall clock.
      AtomicLong receiverTick = new AtomicLong(-TimeUnit.DAYS.toNanos(123));
      QueryAttemptRegistry receiver = new QueryAttemptRegistry(receiverTick::get);
      FragmentInstance instance =
          spy(
              new FragmentInstance(
                  mock(PlanFragment.class),
                  new FragmentInstanceId(new PlanFragmentId(queryId, 0), "0"),
                  null,
                  QueryType.READ,
                  TimeUnit.HOURS.toMillis(1),
                  null,
                  false,
                  false));
      instance.setDataNodeFINum(1);
      instance.setExecutorType(mock(ExecutorType.class));
      doReturn(ByteBuffer.wrap(new byte[] {1})).when(instance).serializeToByteBuffer();
      TEndPoint endpoint = new TEndPoint("remote-host", 10730);
      doReturn(new TDataNodeLocation().setInternalEndPoint(endpoint))
          .when(instance)
          .getHostDataNode();
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
          mock(IClientManager.class);
      SyncDataNodeInternalServiceClient client = mock(SyncDataNodeInternalServiceClient.class);
      when(clients.borrowClient(endpoint)).thenReturn(client);
      when(client.fetchQueryAdmissionToken(anyLong()))
          .thenAnswer(invocation -> receiver.issueAdmissionToken(invocation.getArgument(0)));
      List<TSendFragmentInstanceReq> requests = new ArrayList<>();
      when(client.sendFragmentInstance(any()))
          .thenAnswer(
              invocation -> {
                TSendFragmentInstanceReq request = invocation.getArgument(0);
                requests.add(request.deepCopy());
                if (requests.size() == 1) {
                  receiverTick.addAndGet(TimeUnit.MILLISECONDS.toNanos(firstRpcElapsedMs));
                  throw new TTransportException(TTransportException.TIMED_OUT, "socket timed out");
                }
                FragmentInstanceId receivedId = instance.getId();
                FragmentInstance received = mock(FragmentInstance.class);
                when(received.getId()).thenReturn(receivedId);
                when(received.hasQueryAttempt()).thenReturn(true);
                when(received.getQueryAttemptId()).thenReturn(request.getQueryAttemptId());
                when(received.getAdmissionToken()).thenReturn(request.getAdmissionToken());
                when(received.getDataNodeFINum()).thenReturn(1);
                when(received.getTimeOut()).thenReturn(TimeUnit.HOURS.toMillis(1));
                try {
                  receiver
                      .register(
                          received,
                          new FragmentInstanceStateMachine(received.getId(), Runnable::run),
                          true)
                      .close();
                  return new TSendFragmentInstanceResp(true);
                } catch (IoTDBRuntimeException e) {
                  return new TSendFragmentInstanceResp(false)
                      .setNeedRetry(true)
                      .setStatus(new TSStatus(e.getErrorCode()).setMessage(e.getMessage()));
                }
              });
      FragmentInstanceDispatcherImpl dispatcher =
          new FragmentInstanceDispatcherImpl(QueryType.READ, context, clients, null);
      FragInstanceDispatchResult result =
          dispatcher.dispatch(null, Collections.singletonList(instance)).get();
      assertEquals(expectedSuccess, result.isSuccessful());
      assertEquals(2, requests.size());
      assertEquals(requests.get(0).getAdmissionToken(), requests.get(1).getAdmissionToken());
      if (!expectedSuccess) {
        assertEquals(
            TSStatusCode.DISPATCH_ERROR.getStatusCode(), result.getFailureStatus().getCode());
        assertTrue(
            result
                .getFailureStatus()
                .getMessage()
                .contains(String.valueOf(requests.get(0).getAdmissionToken().getDeadlineNanos())));
        assertTrue(
            result.getFailureStatus().getMessage().contains(String.valueOf(receiverTick.get())));
      }
    } finally {
      config.setDnConnectionTimeoutInMS(previousTimeout);
    }
  }

  @Test
  public void tokenIssuanceAfterFenceExpiryCannotRecreateAnAttempt() {
    AtomicLong tick = new AtomicLong();
    QueryAttemptRegistry receiver = new QueryAttemptRegistry(tick::get);
    org.apache.iotdb.mpp.rpc.thrift.TQueryAdmissionToken token = receiver.issueAdmissionToken(1);
    receiver.close(
        new org.apache.iotdb.db.queryengine.common.QueryAttemptId(new QueryId("closed"), 0),
        token,
        true);
    tick.set(TimeUnit.MILLISECONDS.toNanos(1));
    receiver.removeExpired();
    assertEquals(0, receiver.getRetainedEntryCount());
    // A delayed issuance RPC is deliberately query-agnostic and allocates no attempt entry.
    receiver.issueAdmissionToken(Long.MAX_VALUE);
    assertEquals(0, receiver.getRetainedEntryCount());
    assertTrue(
        receiver.issueAdmissionToken(Long.MAX_VALUE).getValidityMs() <= TimeUnit.DAYS.toMillis(1));
  }
}
