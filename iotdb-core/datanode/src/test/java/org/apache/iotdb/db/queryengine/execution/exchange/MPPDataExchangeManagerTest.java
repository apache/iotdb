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
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

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
    assertProducerOwnsQueue(
        spyMemoryPool,
        shuffleSinkHandle,
        localSourceHandle,
        localFragmentInstanceId,
        remoteFragmentInstanceId,
        localPlanNodeId);
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
    assertProducerOwnsQueue(
        spyMemoryPool,
        shuffleSinkHandle,
        localSourceHandle,
        localFragmentInstanceId,
        remoteFragmentInstanceId,
        localPlanNodeId);
  }

  @Test
  public void testConsumerCloseAfterNoMoreTsBlocksFinishesShuffleSinkHandle() {
    final TFragmentInstanceId producerId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId consumerId = new TFragmentInstanceId("q0", 0, "0");
    final FragmentInstanceContext producerContext = Mockito.mock(FragmentInstanceContext.class);
    final MPPDataExchangeManager mppDataExchangeManager = createMPPDataExchangeManager();
    final ISinkHandle shuffleSinkHandle =
        createShuffleSinkHandle(
            mppDataExchangeManager,
            producerId,
            Collections.singletonList(localChannelLocation(consumerId)),
            producerContext);
    final ISourceHandle localSourceHandle =
        createLocalSourceHandle(mppDataExchangeManager, consumerId, producerId, 0);

    // The consumer takes every block before the producer signals the end of data.
    localSourceHandle.isBlocked();
    shuffleSinkHandle.send(Utils.createMockTsBlock(256));
    Assert.assertTrue(localSourceHandle.isBlocked().isDone());
    Assert.assertNotNull(localSourceHandle.receive());

    // The consumer closes as soon as the queue reports that it is finished, before the producer's
    // own LocalSinkChannel#checkAndInvokeOnFinished() runs. This is the order seen when a finished
    // query's result handle is closed.
    localSourceHandle.isBlocked().addListener(localSourceHandle::close, directExecutor());
    shuffleSinkHandle.setNoMoreTsBlocks();

    Assert.assertTrue(shuffleSinkHandle.isChannelClosed(0));
    Assert.assertTrue(
        "ShuffleSinkHandle did not finish after the consumer closed its only channel",
        shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext).finished();
  }

  @Test
  public void testConsumerCloseBeforeNoMoreTsBlocksFinishesShuffleSinkHandle() {
    final TFragmentInstanceId producerId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId consumerId = new TFragmentInstanceId("q0", 0, "0");
    final FragmentInstanceContext producerContext = Mockito.mock(FragmentInstanceContext.class);
    final MPPDataExchangeManager mppDataExchangeManager = createMPPDataExchangeManager();
    final ISinkHandle shuffleSinkHandle =
        createShuffleSinkHandle(
            mppDataExchangeManager,
            producerId,
            Collections.singletonList(localChannelLocation(consumerId)),
            producerContext);
    final ISourceHandle localSourceHandle =
        createLocalSourceHandle(mppDataExchangeManager, consumerId, producerId, 0);

    localSourceHandle.isBlocked();
    shuffleSinkHandle.send(Utils.createMockTsBlock(256));
    // The consumer stops reading early, for example because a LIMIT is satisfied.
    localSourceHandle.close();

    Assert.assertTrue(shuffleSinkHandle.isChannelClosed(0));
    Assert.assertTrue(
        "ShuffleSinkHandle did not finish after the consumer closed its only channel",
        shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext).finished();

    // The producer's driver may still be running; its later calls must be harmless.
    shuffleSinkHandle.send(Utils.createMockTsBlock(256));
    shuffleSinkHandle.setNoMoreTsBlocks();
    Mockito.verify(producerContext, Mockito.times(1)).finished();
  }

  @Test
  public void testConsumerCloseOfRemoteChannelFinishesShuffleSinkHandle() throws Exception {
    final TFragmentInstanceId producerId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId consumerId = new TFragmentInstanceId("q0", 0, "0");
    final FragmentInstanceContext producerContext = Mockito.mock(FragmentInstanceContext.class);
    final MPPDataExchangeManager mppDataExchangeManager = createMPPDataExchangeManager();
    final ISinkHandle shuffleSinkHandle =
        createShuffleSinkHandle(
            mppDataExchangeManager,
            producerId,
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint("remote-exchange", 10740), consumerId, "exchange_0")),
            producerContext);

    // The remote consumer closes its source handle before the producer signals the end of data.
    mppDataExchangeManager
        .getOrCreateMPPDataExchangeServiceImpl()
        .onCloseSinkChannelEvent(new TCloseSinkChannelEvent(producerId, 0));

    Assert.assertTrue(shuffleSinkHandle.isChannelClosed(0));
    Assert.assertTrue(shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext).finished();
  }

  @Test
  public void testConsumerCloseOfAllLocalChannelsFinishesShuffleSinkHandle() {
    final TFragmentInstanceId producerId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId firstConsumerId = new TFragmentInstanceId("q0", 0, "0");
    final TFragmentInstanceId secondConsumerId = new TFragmentInstanceId("q0", 2, "0");
    final FragmentInstanceContext producerContext = Mockito.mock(FragmentInstanceContext.class);
    final MPPDataExchangeManager mppDataExchangeManager = createMPPDataExchangeManager();
    final ISinkHandle shuffleSinkHandle =
        createShuffleSinkHandle(
            mppDataExchangeManager,
            producerId,
            Arrays.asList(
                localChannelLocation(firstConsumerId), localChannelLocation(secondConsumerId)),
            producerContext);
    final ISourceHandle firstSourceHandle =
        createLocalSourceHandle(mppDataExchangeManager, firstConsumerId, producerId, 0);
    final ISourceHandle secondSourceHandle =
        createLocalSourceHandle(mppDataExchangeManager, secondConsumerId, producerId, 1);

    firstSourceHandle.close();
    Assert.assertFalse(shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext, Mockito.never()).finished();

    secondSourceHandle.close();
    Assert.assertTrue(shuffleSinkHandle.isChannelClosed(0));
    Assert.assertTrue(shuffleSinkHandle.isChannelClosed(1));
    Assert.assertTrue(
        "ShuffleSinkHandle did not finish after the consumers closed all of its channels",
        shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext).finished();
  }

  @Test
  public void testAbortedLocalChannelKeepsShuffleSinkHandleFromFinishing() {
    final TFragmentInstanceId producerId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId consumerId = new TFragmentInstanceId("q0", 0, "0");
    final FragmentInstanceContext producerContext = Mockito.mock(FragmentInstanceContext.class);
    final MPPDataExchangeManager mppDataExchangeManager = createMPPDataExchangeManager();
    final ShuffleSinkHandle shuffleSinkHandle =
        (ShuffleSinkHandle)
            createShuffleSinkHandle(
                mppDataExchangeManager,
                producerId,
                Collections.singletonList(localChannelLocation(consumerId)),
                producerContext);
    createLocalSourceHandle(mppDataExchangeManager, consumerId, producerId, 0);

    Assert.assertTrue(shuffleSinkHandle.getChannel(0).abort());

    Assert.assertFalse(
        "ShuffleSinkHandle must not finish when its channel was aborted",
        shuffleSinkHandle.close());
    Assert.assertFalse(shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext, Mockito.never()).finished();
  }

  @Test
  public void testAbortedLocalChannelKeepsShuffleSinkHandleFromFinishingOnConsumerClose() {
    final TFragmentInstanceId producerId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId firstConsumerId = new TFragmentInstanceId("q0", 0, "0");
    final TFragmentInstanceId secondConsumerId = new TFragmentInstanceId("q0", 2, "0");
    final FragmentInstanceContext producerContext = Mockito.mock(FragmentInstanceContext.class);
    final MPPDataExchangeManager mppDataExchangeManager = createMPPDataExchangeManager();
    final ShuffleSinkHandle shuffleSinkHandle =
        (ShuffleSinkHandle)
            createShuffleSinkHandle(
                mppDataExchangeManager,
                producerId,
                Arrays.asList(
                    localChannelLocation(firstConsumerId), localChannelLocation(secondConsumerId)),
                producerContext);
    createLocalSourceHandle(mppDataExchangeManager, firstConsumerId, producerId, 0);
    final ISourceHandle secondSourceHandle =
        createLocalSourceHandle(mppDataExchangeManager, secondConsumerId, producerId, 1);

    Assert.assertTrue(shuffleSinkHandle.getChannel(0).abort());
    secondSourceHandle.close();

    Assert.assertTrue(shuffleSinkHandle.isChannelClosed(1));
    Assert.assertFalse(
        "ShuffleSinkHandle must not finish when one of its channels was aborted",
        shuffleSinkHandle.isClosed());
    Mockito.verify(producerContext, Mockito.never()).finished();
  }

  private static MPPDataExchangeManager createMPPDataExchangeManager() {
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10240L));
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", memoryManager, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    return new MPPDataExchangeManager(
        mockLocalMemoryManager,
        new TsBlockSerdeFactory(),
        MoreExecutors.newDirectExecutorService(),
        new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));
  }

  private static DownStreamChannelLocation localChannelLocation(TFragmentInstanceId consumerId) {
    return new DownStreamChannelLocation(
        new TEndPoint(
            IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
            IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
        consumerId,
        "exchange_0");
  }

  private static ISinkHandle createShuffleSinkHandle(
      MPPDataExchangeManager mppDataExchangeManager,
      TFragmentInstanceId producerId,
      List<DownStreamChannelLocation> channelLocations,
      FragmentInstanceContext producerContext) {
    return mppDataExchangeManager.createShuffleSinkHandle(
        channelLocations,
        new DownStreamChannelIndex(0),
        ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
        producerId,
        "shuffleSink_0",
        producerContext);
  }

  private static ISourceHandle createLocalSourceHandle(
      MPPDataExchangeManager mppDataExchangeManager,
      TFragmentInstanceId consumerId,
      TFragmentInstanceId producerId,
      int channelIndex) {
    return mppDataExchangeManager.createLocalSourceHandleForFragment(
        consumerId, "exchange_0", "shuffleSink_0", producerId, channelIndex, t -> {});
  }

  private static void assertProducerOwnsQueue(
      MemoryPool pool,
      ISinkHandle sink,
      ISourceHandle source,
      TFragmentInstanceId producer,
      TFragmentInstanceId consumer,
      String planNodeId) {
    SharedTsBlockQueue queue = ((LocalSourceHandle) source).getSharedTsBlockQueue();
    sink.setMaxBytesCanReserve(512);
    source.setMaxBytesCanReserve(128);
    Assert.assertEquals(512, queue.getMaxBytesCanReserve());
    String producerId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(producer);
    String consumerId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(consumer);
    TsBlock block = Utils.createMockTsBlock(256);
    synchronized (queue) {
      Assert.assertTrue(queue.add(block).isDone());
      Assert.assertEquals(256, pool.getReservedBytes());
      Assert.assertEquals(
          256, pool.getPlanNodeMemoryReservedBytes(producer.queryId, producerId, planNodeId));
      Assert.assertEquals(
          256,
          pool.getFragmentMemoryReservedBytes(producer.queryId, producerId, MemorySide.OUTPUT));
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(producer.queryId, producerId, MemorySide.INPUT));
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(consumer.queryId, consumerId, MemorySide.INPUT));
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(consumer.queryId, consumerId, MemorySide.OUTPUT));
      Assert.assertSame(block, queue.remove());
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(producer.queryId, producerId, MemorySide.OUTPUT));
    }
  }
}
