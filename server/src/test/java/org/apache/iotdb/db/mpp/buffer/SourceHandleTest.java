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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.buffer.DataBlockManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService.Client;
import org.apache.iotdb.mpp.rpc.thrift.GetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.GetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SourceHandleTest {

  @Test
  public void testNonBlockedOneTimeReceive() throws TException, InterruptedException {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f1", "0");
    final String localOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f0", "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    Mockito.doAnswer(
            invocation -> {
              GetDataBlockRequest req = invocation.getArgument(0);
              List<ByteBuffer> byteBuffers =
                  new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
              for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                byteBuffers.add(ByteBuffer.allocate(0));
              }
              return new GetDataBlockResponse(byteBuffers);
            })
        .when(mockClient)
        .getDataBlock(Mockito.any(GetDataBlockRequest.class));
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localOperatorId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            mockTsBlockSerde,
            mockSourceHandleListener);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    Thread.sleep(100L);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockClient, Mockito.times(1))
        .getDataBlock(
            Mockito.argThat(
                req ->
                    remoteFragmentInstanceId.equals(req.getSourceFragnemtInstanceId())
                        && 0 == req.getStartSequenceId()
                        && numOfMockTsBlock == req.getEndSequenceId()));

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isClosed());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (numOfMockTsBlock - 1 - i) * mockTsBlockSize,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.close();
    Assert.assertTrue(sourceHandle.isClosed());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
  }

  @Test
  public void testBlockedOneTimeReceive() throws TException, InterruptedException {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f1", "0");
    final String localOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f0", "0");

    // Construct a mock LocalMemoryManager with capacity 3 * mockTsBlockSize.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockBlockedMemoryPool(queryId, 5, mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    Mockito.doAnswer(
            invocation -> {
              GetDataBlockRequest req = invocation.getArgument(0);
              List<ByteBuffer> byteBuffers =
                  new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
              for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                byteBuffers.add(ByteBuffer.allocate(0));
              }
              return new GetDataBlockResponse(byteBuffers);
            })
        .when(mockClient)
        .getDataBlock(Mockito.any(GetDataBlockRequest.class));
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localOperatorId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            mockTsBlockSerde,
            mockSourceHandleListener);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    Thread.sleep(100L);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(5 * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockClient, Mockito.times(1))
        .getDataBlock(
            Mockito.argThat(
                req ->
                    remoteFragmentInstanceId.equals(req.getSourceFragnemtInstanceId())
                        && 0 == req.getStartSequenceId()
                        && 5 == req.getEndSequenceId()));

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      Thread.sleep(100L);
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isClosed());
      Assert.assertFalse(sourceHandle.isFinished());
      if (i < 5) {
        Assert.assertEquals(5 * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());
        final int startSequenceId = 5 + i;
        Mockito.verify(mockClient, Mockito.times(1))
            .getDataBlock(
                Mockito.argThat(
                    req ->
                        remoteFragmentInstanceId.equals(req.getSourceFragnemtInstanceId())
                            && startSequenceId == req.getStartSequenceId()
                            && startSequenceId + 1 == req.getEndSequenceId()));
      } else {
        Assert.assertEquals(
            (numOfMockTsBlock - 1 - i) * mockTsBlockSize,
            sourceHandle.getBufferRetainedSizeInBytes());
      }
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.close();
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
  }

  @Test
  public void testMultiTimesReceive() throws TException, InterruptedException {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f1", "0");
    final String localOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f0", "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    Mockito.doAnswer(
            invocation -> {
              GetDataBlockRequest req = invocation.getArgument(0);
              List<ByteBuffer> byteBuffers =
                  new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
              for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                byteBuffers.add(ByteBuffer.allocate(0));
              }
              return new GetDataBlockResponse(byteBuffers);
            })
        .when(mockClient)
        .getDataBlock(Mockito.any(GetDataBlockRequest.class));

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localOperatorId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            mockTsBlockSerde,
            mockSourceHandleListener);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived in unordered manner.
    sourceHandle.updatePendingDataBlockInfo(
        numOfMockTsBlock,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    Thread.sleep(100L);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockClient, Mockito.times(0))
        .getDataBlock(Mockito.any(GetDataBlockRequest.class));

    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    Thread.sleep(100L);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * 2 * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockClient, Mockito.times(1))
        .getDataBlock(
            Mockito.argThat(
                req ->
                    remoteFragmentInstanceId.equals(req.getSourceFragnemtInstanceId())
                        && 0 == req.getStartSequenceId()
                        && numOfMockTsBlock * 2 == req.getEndSequenceId()));

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < 2 * numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < 2 * numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isClosed());
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
    Thread.sleep(100L);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * mockTsBlockSize, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockClient, Mockito.times(1))
        .getDataBlock(
            Mockito.argThat(
                req ->
                    remoteFragmentInstanceId.equals(req.getSourceFragnemtInstanceId())
                        && numOfMockTsBlock * 2 == req.getStartSequenceId()
                        && numOfMockTsBlock * 3 == req.getEndSequenceId()));

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isClosed());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (numOfMockTsBlock - 1 - i) * mockTsBlockSize,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(3 * numOfMockTsBlock - 1);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.close();
    Assert.assertTrue(sourceHandle.isClosed());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
  }

  @Test
  public void testFailedReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f1", "0");
    final String localOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f0", "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    try {
      Mockito.doThrow(new TException("Mock exception"))
          .when(mockClient)
          .getDataBlock(Mockito.any(GetDataBlockRequest.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localOperatorId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            mockTsBlockSerde,
            mockSourceHandleListener);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // Receive data blocks from upstream fragment instance.
    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> mockTsBlockSize)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Thread.sleep(100L);
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }
    try {
      Assert.assertFalse(sourceHandle.isBlocked().isDone());
      Assert.fail("Expect a RuntimeException.");
    } catch (RuntimeException e) {
      Assert.assertEquals("org.apache.thrift.TException: Mock exception", e.getMessage());
    }
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    try {
      sourceHandle.receive();
      Assert.fail("Expect a RuntimeException.");
    } catch (RuntimeException e) {
      Assert.assertEquals("org.apache.thrift.TException: Mock exception", e.getMessage());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertFalse(sourceHandle.isClosed());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.close();
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
  }
}
