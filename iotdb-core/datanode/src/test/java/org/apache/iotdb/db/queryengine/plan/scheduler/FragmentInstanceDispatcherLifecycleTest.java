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
import org.apache.iotdb.mpp.rpc.thrift.TQueryAdmissionToken;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FragmentInstanceDispatcherLifecycleTest {
  private final QueryId queryId = new QueryId("dispatch_admission");
  private final MPPQueryContext context = new MPPQueryContext(queryId);
  private final TEndPoint endpoint = new TEndPoint("remote-host", 10730);
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
      mock(IClientManager.class);
  private final SyncDataNodeInternalServiceClient client =
      mock(SyncDataNodeInternalServiceClient.class);
  private final TQueryAdmissionToken token = new TQueryAdmissionToken("receiver", 123456L, 60000L);

  private FragmentInstanceDispatcherImpl dispatcher() throws Exception {
    context.setStartTime(System.currentTimeMillis());
    context.setTimeOut(Long.MAX_VALUE);
    when(clients.borrowClient(endpoint)).thenReturn(client);
    when(client.fetchQueryAdmissionToken(anyLong())).thenReturn(token);
    return new FragmentInstanceDispatcherImpl(QueryType.READ, context, clients, null);
  }

  private FragmentInstance instance(int fragmentId) {
    FragmentInstance instance =
        spy(
            new FragmentInstance(
                mock(PlanFragment.class),
                new FragmentInstanceId(new PlanFragmentId(queryId, fragmentId), "0"),
                null,
                QueryType.READ,
                Long.MAX_VALUE,
                null,
                false,
                false));
    doReturn(ByteBuffer.wrap(new byte[] {1})).when(instance).serializeToByteBuffer();
    doReturn(new TDataNodeLocation().setInternalEndPoint(endpoint))
        .when(instance)
        .getHostDataNode();
    instance.setExecutorType(mock(ExecutorType.class));
    return instance;
  }

  @Test
  public void abortPreventsTokenAcquisitionAndDispatch() throws Exception {
    FragmentInstanceDispatcherImpl dispatcher = dispatcher();
    dispatcher.abort();
    FragInstanceDispatchResult result =
        dispatcher.dispatch(null, Collections.singletonList(instance(0))).get();
    assertFalse(result.isSuccessful());
    assertEquals(
        TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), result.getFailureStatus().getCode());
    verify(client, never()).fetchQueryAdmissionToken(anyLong());
    verify(client, never()).sendFragmentInstance(any());
  }

  @Test
  public void transportRetryReusesTokenAndStopFreezesIssuedTokens() throws Exception {
    FragmentInstanceDispatcherImpl dispatcher = dispatcher();
    context.prepareForRetry();
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
    FragmentInstance instance = instance(0);
    assertTrue(dispatcher.dispatch(null, Collections.singletonList(instance)).get().isSuccessful());
    assertEquals(2, requests.size());
    assertEquals(0, requests.get(0).getQueryAttemptId());
    assertEquals(token, requests.get(0).getAdmissionToken());
    assertEquals(requests.get(0).getAdmissionToken(), requests.get(1).getAdmissionToken());
    verify(client).fetchQueryAdmissionToken(anyLong());
    Map<TEndPoint, TQueryAdmissionToken> issued = dispatcher.getIssuedAdmissionTokens();
    dispatcher.abort();
    assertFalse(
        dispatcher.dispatch(null, Collections.singletonList(instance)).get().isSuccessful());
    assertEquals(2, requests.size());
    assertEquals(issued, dispatcher.getIssuedAdmissionTokens());
  }

  @Test
  public void fragmentsOnTheSameNodeReuseOneTokenRpc() throws Exception {
    FragmentInstanceDispatcherImpl dispatcher = dispatcher();
    when(client.sendFragmentInstance(any())).thenReturn(new TSendFragmentInstanceResp(true));
    assertTrue(
        dispatcher.dispatch(null, Arrays.asList(instance(0), instance(1))).get().isSuccessful());
    verify(client).fetchQueryAdmissionToken(anyLong());
  }

  @Test
  public void legacyReceiverUsesTheOriginalProtocol() throws Exception {
    FragmentInstanceDispatcherImpl dispatcher = dispatcher();
    when(client.fetchQueryAdmissionToken(anyLong()))
        .thenThrow(
            new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unknown method"));
    when(client.sendFragmentInstance(any()))
        .thenAnswer(
            invocation -> {
              TSendFragmentInstanceReq request = invocation.getArgument(0);
              assertFalse(request.isSetQueryAttemptId());
              assertFalse(request.isSetAdmissionToken());
              return new TSendFragmentInstanceResp(true);
            });
    assertTrue(
        dispatcher.dispatch(null, Arrays.asList(instance(0), instance(1))).get().isSuccessful());
    verify(client).fetchQueryAdmissionToken(anyLong());
    assertTrue(dispatcher.getIssuedAdmissionTokens().isEmpty());
  }

  @Test
  public void wrappedUnknownMethodUsesAFreshClientForLegacyDispatch() throws Exception {
    FragmentInstanceDispatcherImpl dispatcher = dispatcher();
    SyncDataNodeInternalServiceClient fresh = mock(SyncDataNodeInternalServiceClient.class);
    java.util.concurrent.atomic.AtomicBoolean returned =
        new java.util.concurrent.atomic.AtomicBoolean();
    java.util.concurrent.atomic.AtomicInteger borrows =
        new java.util.concurrent.atomic.AtomicInteger();
    org.mockito.Mockito.doAnswer(
            ignored -> {
              returned.set(true);
              return null;
            })
        .when(client)
        .close();
    when(clients.borrowClient(endpoint))
        .thenAnswer(
            ignored -> {
              if (borrows.getAndIncrement() == 0) {
                return client;
              }
              assertTrue(returned.get());
              return fresh;
            });
    when(client.fetchQueryAdmissionToken(anyLong()))
        .thenThrow(
            new TException(
                new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unknown method")));
    when(fresh.sendFragmentInstance(any()))
        .thenAnswer(
            invocation -> {
              TSendFragmentInstanceReq request = invocation.getArgument(0);
              assertFalse(request.isSetAdmissionToken());
              return new TSendFragmentInstanceResp(true);
            });
    assertTrue(
        dispatcher.dispatch(null, Collections.singletonList(instance(0))).get().isSuccessful());
    verify(client, never()).sendFragmentInstance(any());
    verify(fresh, never()).fetchQueryAdmissionToken(anyLong());
    verify(fresh).sendFragmentInstance(any());
  }

  @Test
  public void abortDuringTokenRpcCannotPublishALateFragment() throws Exception {
    FragmentInstanceDispatcherImpl dispatcher = dispatcher();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch acquiring = new CountDownLatch(1);
    CountDownLatch returnToken = new CountDownLatch(1);
    when(client.fetchQueryAdmissionToken(anyLong()))
        .thenAnswer(
            invocation -> {
              acquiring.countDown();
              assertTrue(returnToken.await(10, TimeUnit.SECONDS));
              return token;
            });
    try {
      Future<FragInstanceDispatchResult> result =
          executor.submit(
              () -> dispatcher.dispatch(null, Collections.singletonList(instance(0))).get());
      assertTrue(acquiring.await(10, TimeUnit.SECONDS));
      dispatcher.abort();
      assertTrue(dispatcher.getIssuedAdmissionTokens().isEmpty());
      returnToken.countDown();
      assertFalse(result.get(10, TimeUnit.SECONDS).isSuccessful());
      verify(client, never()).sendFragmentInstance(any());
      assertTrue(dispatcher.getIssuedAdmissionTokens().isEmpty());
    } finally {
      returnToken.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
