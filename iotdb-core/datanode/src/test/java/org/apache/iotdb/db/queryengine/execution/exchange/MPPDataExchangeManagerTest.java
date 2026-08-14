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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MPPDataExchangeManagerTest {
  @Test
  public void testNewDataBlockMemoryFailureMarksTargetFragmentFailed() throws Exception {
    final String queryId = "q_memory_reservation_failure";
    final String targetPlanNodeId = "root-exchange";
    final long dataBlockSize = 1024L;
    final TFragmentInstanceId targetFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final TFragmentInstanceId sourceFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final TEndPoint sourceEndpoint = new TEndPoint("remote-exchange", 10740);
    ExecutorService exchangeExecutor = Executors.newSingleThreadExecutor();
    ExecutorService notificationExecutor = Executors.newSingleThreadExecutor();

    try {
      LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
      MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
      Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);
      IllegalArgumentException expectedFailure =
          new IllegalArgumentException("injected memory reservation failure");
      Mockito.when(
              memoryPool.reserveWithPriority(
                  Mockito.eq(queryId),
                  Mockito.anyString(),
                  Mockito.eq(targetPlanNodeId),
                  Mockito.eq(dataBlockSize),
                  Mockito.anyLong(),
                  Mockito.eq(false)))
          .thenThrow(expectedFailure);

      FragmentInstanceId targetId =
          new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 0), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(targetId, notificationExecutor);
      FragmentInstanceContext context =
          FragmentInstanceContext.createFragmentInstanceContext(targetId, stateMachine);
      MPPDataExchangeManager exchangeManager =
          new MPPDataExchangeManager(
              localMemoryManager,
              new TsBlockSerdeFactory(),
              exchangeExecutor,
              Mockito.mock(IClientManager.class));
      ISourceHandle sourceHandle =
          exchangeManager.createSourceHandle(
              targetFragmentInstanceId,
              targetPlanNodeId,
              0,
              sourceEndpoint,
              sourceFragmentInstanceId,
              context::failed);
      sourceHandle.isBlocked();

      TNewDataBlockEvent event =
          new TNewDataBlockEvent(
              targetFragmentInstanceId,
              targetPlanNodeId,
              sourceFragmentInstanceId,
              0,
              Collections.singletonList(dataBlockSize));
      IllegalArgumentException actualFailure =
          Assert.assertThrows(
              IllegalArgumentException.class,
              () ->
                  exchangeManager
                      .getOrCreateMPPDataExchangeServiceImpl()
                      .onNewDataBlockEvent(event));

      Assert.assertSame(expectedFailure, actualFailure);
      Assert.assertEquals(FragmentInstanceState.FAILED, stateMachine.getState());
      Assert.assertSame(expectedFailure, stateMachine.getFailureCauses().peek());
    } finally {
      exchangeExecutor.shutdownNow();
      notificationExecutor.shutdownNow();
    }
  }

  @Test
  public void testCreateLocalSinkHandle() {
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId("q0", 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "shuffleSink_0";
    final FragmentInstanceContext mockFragmentInstanceContext =
        Mockito.mock(FragmentInstanceContext.class);

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10240L));
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", memoryManager, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    MPPDataExchangeManager mppDataExchangeManager =
        new MPPDataExchangeManager(
            mockLocalMemoryManager,
            new TsBlockSerdeFactory(),
            Executors.newSingleThreadExecutor(),
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));

    ISinkHandle shuffleSinkHandle =
        mppDataExchangeManager.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint(
                        IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
                        IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
                    remoteFragmentInstanceId,
                    remotePlanNodeId)),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            mockFragmentInstanceContext);

    Assert.assertTrue(shuffleSinkHandle instanceof ShuffleSinkHandle);

    ISourceHandle localSourceHandle =
        mppDataExchangeManager.createLocalSourceHandleForFragment(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            0,
            t -> {});

    Assert.assertTrue(localSourceHandle instanceof LocalSourceHandle);

    Assert.assertEquals(
        ((LocalSinkChannel) shuffleSinkHandle.getChannel(0)).getSharedTsBlockQueue(),
        ((LocalSourceHandle) localSourceHandle).getSharedTsBlockQueue());
  }

  @Test
  public void testCreateLocalSourceHandle() {
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId("q0", 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "shuffleSink_0";
    final FragmentInstanceContext mockFragmentInstanceContext =
        Mockito.mock(FragmentInstanceContext.class);

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10240L));
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", memoryManager, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    MPPDataExchangeManager mppDataExchangeManager =
        new MPPDataExchangeManager(
            mockLocalMemoryManager,
            new TsBlockSerdeFactory(),
            Executors.newSingleThreadExecutor(),
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));

    ISourceHandle localSourceHandle =
        mppDataExchangeManager.createLocalSourceHandleForFragment(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            0,
            t -> {});

    Assert.assertTrue(localSourceHandle instanceof LocalSourceHandle);

    ISinkHandle shuffleSinkHandle =
        mppDataExchangeManager.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint(
                        IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
                        IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
                    remoteFragmentInstanceId,
                    remotePlanNodeId)),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            mockFragmentInstanceContext);

    Assert.assertTrue(shuffleSinkHandle instanceof ShuffleSinkHandle);

    Assert.assertEquals(
        ((LocalSinkChannel) shuffleSinkHandle.getChannel(0)).getSharedTsBlockQueue(),
        ((LocalSourceHandle) localSourceHandle).getSharedTsBlockQueue());
  }
}
