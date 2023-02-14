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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SourceHandleTest {
  private static final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;

  private static long maxBytesPerFI;

  @BeforeClass
  public static void beforeClass() {
    maxBytesPerFI = IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance();
    IoTDBDescriptor.getInstance().getConfig().setMaxBytesPerFragmentInstance(5 * MOCK_TSBLOCK_SIZE);
  }

  @AfterClass
  public static void afterClass() {
    IoTDBDescriptor.getInstance().getConfig().setMaxBytesPerFragmentInstance(maxBytesPerFI);
  }

  @Test
  public void testNonBlockedOneTimeReceive() {
    final String queryId = "q0";
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && numOfMockTsBlock == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && numOfMockTsBlock == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFinished(sourceHandle);
  }

  @Test
  public void testBlockedOneTimeReceive() {
    final String queryId = "q0";
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * MOCK_TSBLOCK_SIZE per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", 10 * MOCK_TSBLOCK_SIZE, 5 * MOCK_TSBLOCK_SIZE));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    long maxBytesCanReserve =
        Math.min(
            5 * MOCK_TSBLOCK_SIZE,
            IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance());
    sourceHandle.setMaxBytesCanReserve(maxBytesCanReserve);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(spyMemoryPool, Mockito.timeout(10_000).times(6))
          .reserve(
              queryId,
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                  localFragmentInstanceId),
              localPlanNodeId,
              MOCK_TSBLOCK_SIZE,
              maxBytesCanReserve);
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && 5 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && 5 == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(6 * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      Mockito.verify(spyMemoryPool, Mockito.timeout(10_0000).times(i))
          .free(
              queryId,
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                  localFragmentInstanceId),
              localPlanNodeId,
              MOCK_TSBLOCK_SIZE);
      sourceHandle.receive();
      try {
        if (i < 5) {
          Assert.assertEquals(
              i == 4 ? 5 * MOCK_TSBLOCK_SIZE : 6 * MOCK_TSBLOCK_SIZE,
              sourceHandle.getBufferRetainedSizeInBytes());
          final int startSequenceId = 5 + i;
          Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
              .getDataBlock(
                  Mockito.argThat(
                      req ->
                          remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                              && startSequenceId == req.getStartSequenceId()
                              && startSequenceId + 1 == req.getEndSequenceId()));
          Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
              .onAcknowledgeDataBlockEvent(
                  Mockito.argThat(
                      e ->
                          remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                              && startSequenceId == e.getStartSequenceId()
                              && startSequenceId + 1 == e.getEndSequenceId()));
        } else {
          Assert.assertEquals(
              (numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
              sourceHandle.getBufferRetainedSizeInBytes());
        }
      } catch (TException e) {
        e.printStackTrace();
        Assert.fail();
      }
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFinished(sourceHandle);
  }

  @Test
  public void testMultiTimesReceive() {
    final String queryId = "q0";
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived in unordered manner.
    sourceHandle.updatePendingDataBlockInfo(
        numOfMockTsBlock,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(0))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(0))
          .onAcknowledgeDataBlockEvent(Mockito.any(TAcknowledgeDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && numOfMockTsBlock * 2 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && numOfMockTsBlock * 2 == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * 2 * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < 2 * numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < 2 * numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (2 * numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        numOfMockTsBlock * 2,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 == req.getStartSequenceId()
                          && numOfMockTsBlock * 3 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 == e.getStartSequenceId()
                          && numOfMockTsBlock * 3 == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(3 * numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFinished(sourceHandle);
  }

  @Test
  public void testFailedReceive() {
    final String queryId = "q0";
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    TException mockException = new TException("Mock exception");
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doThrow(mockException)
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    sourceHandle.setRetryIntervalInMs(0L);
    Future<?> blocked = sourceHandle.isBlocked();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(SourceHandle.MAX_ATTEMPT_TIMES))
          .getDataBlock(Mockito.any());
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFailure(sourceHandle, mockException);
    Assert.assertFalse(blocked.isDone());

    sourceHandle.abort();
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertTrue(sourceHandle.isAborted());
    Assert.assertTrue(blocked.isDone());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onAborted(sourceHandle);
  }

  @Test
  public void testForceClose() {
    final String queryId = "q0";
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    Future<?> blocked = sourceHandle.isBlocked();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertTrue(blocked.isCancelled());
    Assert.assertTrue(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onAborted(sourceHandle);
  }
}
