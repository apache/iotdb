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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceFailureInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.scheduler.FixedRateFragInsStateTracker;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceInfoReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceInfoResp;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SinkChannelFailurePropagationTest {

  @Test
  public void testFailureCallbackRunsBeforeChannelAccounting() {
    ExecutorService exchangeExecutor = Executors.newSingleThreadExecutor();
    try {
      MPPDataExchangeManager exchangeManager =
          new MPPDataExchangeManager(
              Mockito.mock(LocalMemoryManager.class),
              new TsBlockSerdeFactory(),
              exchangeExecutor,
              Mockito.mock(IClientManager.class));
      AtomicInteger remainingChannels = new AtomicInteger(1);
      AtomicBoolean hasChannelFailedOrAborted = new AtomicBoolean(false);
      AtomicBoolean callbackInvoked = new AtomicBoolean(false);
      IllegalStateException callbackFailure =
          new IllegalStateException("injected callback failure");
      MPPDataExchangeManager.ISinkChannelListenerImpl listener =
          exchangeManager
          .new ISinkChannelListenerImpl(
              new TFragmentInstanceId("q_failure_callback_order", 0, "0"),
              Mockito.mock(FragmentInstanceContext.class),
              failure -> {
                Assert.assertEquals(1, remainingChannels.get());
                callbackInvoked.set(true);
                throw callbackFailure;
              },
              remainingChannels,
              hasChannelFailedOrAborted);

      IllegalStateException actualFailure =
          Assert.assertThrows(
              IllegalStateException.class,
              () ->
                  listener.onFailure(
                      Mockito.mock(ISink.class), new TException("injected channel failure")));

      Assert.assertSame(callbackFailure, actualFailure);
      Assert.assertTrue(callbackInvoked.get());
      Assert.assertTrue(hasChannelFailedOrAborted.get());
      Assert.assertEquals(0, remainingChannels.get());
    } finally {
      exchangeExecutor.shutdownNow();
    }
  }

  @Test
  public void testFailurePreventsLastNormalChannelFromClosingShuffleSinkHandle() throws Exception {
    final String queryId = "q_failure_with_two_channels";
    final TFragmentInstanceId upstreamThriftId = new TFragmentInstanceId(queryId, 1, "0");
    final FragmentInstanceId upstreamId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 1), "0");
    final TEndPoint firstEndpoint = new TEndPoint("remote-exchange-0", 10740);
    final TEndPoint secondEndpoint = new TEndPoint("remote-exchange-1", 10740);

    ExecutorService exchangeExecutor = Executors.newSingleThreadExecutor();
    ExecutorService fragmentNotificationExecutor = Executors.newSingleThreadExecutor();

    try {
      LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
      MemoryPool memoryPool = Utils.createMockNonBlockedMemoryPool();
      Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);

      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> exchangeClientManager =
          Mockito.mock(IClientManager.class);
      SyncDataNodeMPPDataExchangeServiceClient exchangeClient =
          Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
      TException expectedFailure = new TException("injected onNewDataBlockEvent failure");
      Mockito.when(exchangeClientManager.borrowClient(secondEndpoint)).thenReturn(exchangeClient);
      Mockito.doThrow(expectedFailure)
          .when(exchangeClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));

      FragmentInstanceStateMachine fragmentStateMachine =
          new FragmentInstanceStateMachine(upstreamId, fragmentNotificationExecutor);
      FragmentInstanceContext fragmentContext =
          FragmentInstanceContext.createFragmentInstanceContext(upstreamId, fragmentStateMachine);
      MPPDataExchangeManager exchangeManager =
          new MPPDataExchangeManager(
              localMemoryManager,
              new TsBlockSerdeFactory(),
              exchangeExecutor,
              exchangeClientManager);
      ISinkHandle sinkHandle =
          exchangeManager.createShuffleSinkHandle(
              Arrays.asList(
                  new DownStreamChannelLocation(
                      firstEndpoint, new TFragmentInstanceId(queryId, 0, "0"), "root-exchange-0"),
                  new DownStreamChannelLocation(
                      secondEndpoint, new TFragmentInstanceId(queryId, 0, "1"), "root-exchange-1")),
              new DownStreamChannelIndex(1),
              ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
              upstreamThriftId,
              "upstream-sink",
              fragmentContext);
      SinkChannel normallyFinishedChannel = (SinkChannel) sinkHandle.getChannel(0);
      SinkChannel failedChannel = (SinkChannel) sinkHandle.getChannel(1);
      failedChannel.setRetryIntervalInMs(0);

      Assert.assertTrue(sinkHandle.isFull().isDone());
      sinkHandle.send(Utils.createMockTsBlocks(1, 1024).get(0));
      Mockito.verify(exchangeClient, Mockito.timeout(5_000).times(SinkChannel.MAX_ATTEMPT_TIMES))
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
      long waitStartNanos = System.nanoTime();
      while (!fragmentStateMachine.getState().isDone()
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - waitStartNanos) < 5) {
        Thread.sleep(10);
      }

      Assert.assertEquals(FragmentInstanceState.FAILED, fragmentStateMachine.getState());
      Assert.assertEquals(expectedFailure, fragmentStateMachine.getFailureCauses().peek());
      Assert.assertTrue(normallyFinishedChannel.close());

      Assert.assertEquals(0, exchangeManager.getShuffleSinkHandleSize());
      Assert.assertFalse(sinkHandle.isClosed());
      Assert.assertFalse(sinkHandle.isAborted());

      Assert.assertTrue(sinkHandle.abort());
      Assert.assertTrue(sinkHandle.isAborted());
      Assert.assertFalse(sinkHandle.isClosed());
      Assert.assertTrue(failedChannel.isAborted());
    } finally {
      exchangeExecutor.shutdownNow();
      fragmentNotificationExecutor.shutdownNow();
    }
  }

  @Test
  public void testSingleChannelFailurePropagation() throws Exception {
    final String queryId = "q_failure_propagation";
    final TFragmentInstanceId upstreamThriftId = new TFragmentInstanceId(queryId, 1, "0");
    final TFragmentInstanceId rootThriftId = new TFragmentInstanceId(queryId, 0, "0");
    final FragmentInstanceId upstreamId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 1), "0");
    final FragmentInstanceId rootId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 0), "0");
    final TEndPoint exchangeEndpoint = new TEndPoint("remote-exchange", 10740);
    final TEndPoint stateEndpoint = new TEndPoint("remote-state", 10730);

    ExecutorService exchangeExecutor = Executors.newSingleThreadExecutor();
    ExecutorService fragmentNotificationExecutor = Executors.newSingleThreadExecutor();
    ExecutorService queryNotificationExecutor = Executors.newSingleThreadExecutor();
    ScheduledExecutorService stateTrackerExecutor = Executors.newSingleThreadScheduledExecutor();
    FixedRateFragInsStateTracker stateTracker = null;

    try {
      LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
      MemoryPool memoryPool = Utils.createMockNonBlockedMemoryPool();
      Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);

      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> exchangeClientManager =
          Mockito.mock(IClientManager.class);
      SyncDataNodeMPPDataExchangeServiceClient exchangeClient =
          Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
      TException expectedFailure = new TException("injected onNewDataBlockEvent failure");
      Mockito.when(exchangeClientManager.borrowClient(exchangeEndpoint)).thenReturn(exchangeClient);
      Mockito.doThrow(expectedFailure)
          .when(exchangeClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));

      FragmentInstanceStateMachine fragmentStateMachine =
          new FragmentInstanceStateMachine(upstreamId, fragmentNotificationExecutor);
      FragmentInstanceContext fragmentContext =
          FragmentInstanceContext.createFragmentInstanceContext(upstreamId, fragmentStateMachine);

      MPPDataExchangeManager exchangeManager =
          new MPPDataExchangeManager(
              localMemoryManager,
              new TsBlockSerdeFactory(),
              exchangeExecutor,
              exchangeClientManager);
      ISinkHandle sinkHandle =
          exchangeManager.createShuffleSinkHandle(
              Collections.singletonList(
                  new DownStreamChannelLocation(exchangeEndpoint, rootThriftId, "root-exchange")),
              new DownStreamChannelIndex(0),
              ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
              upstreamThriftId,
              "upstream-sink",
              fragmentContext);
      SinkChannel sinkChannel = (SinkChannel) sinkHandle.getChannel(0);
      sinkChannel.setRetryIntervalInMs(0);
      fragmentStateMachine.addStateChangeListener(
          newState -> {
            if (newState.isFailed()) {
              sinkHandle.abort();
            } else if (newState.isDone()) {
              sinkHandle.close();
            }
          });

      QueryStateMachine queryStateMachine =
          new QueryStateMachine(new QueryId(queryId), queryNotificationExecutor);
      queryStateMachine.transitionToRunning();

      FragmentInstance upstreamInstance = Mockito.mock(FragmentInstance.class);
      FragmentInstance rootInstance = Mockito.mock(FragmentInstance.class);
      TDataNodeLocation stateLocation =
          new TDataNodeLocation().setDataNodeId(1).setInternalEndPoint(stateEndpoint);
      Mockito.when(upstreamInstance.getId()).thenReturn(upstreamId);
      Mockito.when(upstreamInstance.getHostDataNode()).thenReturn(stateLocation);
      Mockito.when(upstreamInstance.isRoot()).thenReturn(false);
      Mockito.when(rootInstance.getId()).thenReturn(rootId);
      Mockito.when(rootInstance.getHostDataNode()).thenReturn(stateLocation);
      Mockito.when(rootInstance.isRoot()).thenReturn(true);

      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> stateClientManager =
          Mockito.mock(IClientManager.class);
      SyncDataNodeInternalServiceClient stateClient =
          Mockito.mock(SyncDataNodeInternalServiceClient.class);
      Mockito.when(stateClientManager.borrowClient(stateEndpoint)).thenReturn(stateClient);
      Mockito.when(
              stateClient.fetchFragmentInstanceInfo(
                  Mockito.any(TFetchFragmentInstanceInfoReq.class)))
          .thenAnswer(
              invocation -> {
                TFetchFragmentInstanceInfoReq request = invocation.getArgument(0);
                if (upstreamThriftId.equals(request.getFragmentInstanceId())) {
                  return toThriftResponse(fragmentContext.getInstanceInfo());
                }
                return new TFragmentInstanceInfoResp(FragmentInstanceState.RUNNING.toString());
              });

      stateTracker =
          new FixedRateFragInsStateTracker(
              queryStateMachine,
              stateTrackerExecutor,
              Arrays.asList(upstreamInstance, rootInstance),
              stateClientManager);
      stateTracker.start();

      Assert.assertTrue(sinkHandle.isFull().isDone());
      long failureStartNanos = System.nanoTime();
      sinkHandle.send(Utils.createMockTsBlocks(1, 1024).get(0));

      Mockito.verify(exchangeClient, Mockito.timeout(5_000).times(SinkChannel.MAX_ATTEMPT_TIMES))
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
      while (!fragmentStateMachine.getState().isDone()
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - failureStartNanos) < 5) {
        Thread.sleep(10);
      }

      Assert.assertEquals(FragmentInstanceState.FAILED, fragmentStateMachine.getState());
      Assert.assertEquals(expectedFailure, fragmentStateMachine.getFailureCauses().peek());
      while (!sinkChannel.isAborted()
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - failureStartNanos) < 5) {
        Thread.sleep(10);
      }
      Assert.assertTrue(sinkHandle.isAborted());
      Assert.assertFalse(sinkHandle.isClosed());
      Assert.assertTrue(sinkChannel.isAborted());
      Assert.assertFalse(sinkChannel.isClosed());
      Assert.assertEquals(0, exchangeManager.getShuffleSinkHandleSize());

      while (!queryStateMachine.getState().isDone()
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - failureStartNanos) < 5) {
        Thread.sleep(10);
      }
      Mockito.verify(stateClient, Mockito.timeout(3_000).atLeastOnce())
          .fetchFragmentInstanceInfo(Mockito.any(TFetchFragmentInstanceInfoReq.class));
      Assert.assertEquals(QueryState.FAILED, queryStateMachine.getState());
    } finally {
      if (stateTracker != null) {
        stateTracker.abort();
      }
      exchangeExecutor.shutdownNow();
      fragmentNotificationExecutor.shutdownNow();
      queryNotificationExecutor.shutdownNow();
      stateTrackerExecutor.shutdownNow();
    }
  }

  private static TFragmentInstanceInfoResp toThriftResponse(FragmentInstanceInfo info)
      throws IOException {
    TFragmentInstanceInfoResp response = new TFragmentInstanceInfoResp(info.getState().toString());
    response.setEndTime(info.getEndTime());
    response.setFailedMessages(Collections.singletonList(info.getMessage()));
    List<ByteBuffer> failureInfoList = new ArrayList<>();
    for (FragmentInstanceFailureInfo failureInfo : info.getFailureInfoList()) {
      failureInfoList.add(failureInfo.serialize());
    }
    response.setFailureInfoList(failureInfoList);
    info.getErrorCode().ifPresent(response::setErrorCode);
    return response;
  }
}
