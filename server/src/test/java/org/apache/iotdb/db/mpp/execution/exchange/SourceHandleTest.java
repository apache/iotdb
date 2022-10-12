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
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SourceHandleTest {
  @Test
  public void testNonBlockedOneTimeReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
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
    } catch (TException | IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
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
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && numOfMockTsBlock == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && numOfMockTsBlock == e.getEndSequenceId()));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());

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
          (numOfMockTsBlock - 1 - i) * mockTsBlockSize,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onFinished(sourceHandle);
  }

  @Test
  public void testBlockedOneTimeReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", 10 * mockTsBlockSize, 5 * mockTsBlockSize));
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
    } catch (TException | IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
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
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
      Mockito.verify(spyMemoryPool, Mockito.times(6)).reserve(queryId, mockTsBlockSize);
      Mockito.verify(mockClient, Mockito.times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && 5 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && 5 == e.getEndSequenceId()));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(6 * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      Mockito.verify(spyMemoryPool, Mockito.times(i)).free(queryId, mockTsBlockSize);
      sourceHandle.receive();
      try {
        Thread.sleep(100L);
        if (i < 5) {
          Assert.assertEquals(
              i == 4 ? 5 * mockTsBlockSize : 6 * mockTsBlockSize,
              sourceHandle.getBufferRetainedSizeInBytes());
          final int startSequenceId = 5 + i;
          Mockito.verify(mockClient, Mockito.times(1))
              .getDataBlock(
                  Mockito.argThat(
                      req ->
                          remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                              && startSequenceId == req.getStartSequenceId()
                              && startSequenceId + 1 == req.getEndSequenceId()));
          Mockito.verify(mockClient, Mockito.times(1))
              .onAcknowledgeDataBlockEvent(
                  Mockito.argThat(
                      e ->
                          remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                              && startSequenceId == e.getStartSequenceId()
                              && startSequenceId + 1 == e.getEndSequenceId()));
        } else {
          Assert.assertEquals(
              (numOfMockTsBlock - 1 - i) * mockTsBlockSize,
              sourceHandle.getBufferRetainedSizeInBytes());
        }
      } catch (InterruptedException | TException e) {
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
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onFinished(sourceHandle);
  }

  @Test
  public void testMultiTimesReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
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
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);
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
    } catch (TException | IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
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
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(0))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      Mockito.verify(mockClient, Mockito.times(0))
          .onAcknowledgeDataBlockEvent(Mockito.any(TAcknowledgeDataBlockEvent.class));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && numOfMockTsBlock * 2 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && numOfMockTsBlock * 2 == e.getEndSequenceId()));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * 2 * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());

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
          (2 * numOfMockTsBlock - 1 - i) * mockTsBlockSize,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        numOfMockTsBlock * 2,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 == req.getStartSequenceId()
                          && numOfMockTsBlock * 3 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 == e.getStartSequenceId()
                          && numOfMockTsBlock * 3 == e.getEndSequenceId()));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());

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
          (numOfMockTsBlock - 1 - i) * mockTsBlockSize,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(3 * numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onFinished(sourceHandle);
  }

  @Test
  public void testFailedReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
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
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);
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
    } catch (TException | IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
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
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(SourceHandle.MAX_ATTEMPT_TIMES))
          .getDataBlock(Mockito.any());
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Mockito.verify(mockSourceHandleListener, Mockito.times(1))
        .onFailure(sourceHandle, mockException);
    Assert.assertFalse(blocked.isDone());

    sourceHandle.abort();
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertTrue(sourceHandle.isAborted());
    Assert.assertTrue(blocked.isDone());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onAborted(sourceHandle);
  }

  @Test
  public void testForceClose() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
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
    } catch (TException | IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
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
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onAborted(sourceHandle);
  }
}
