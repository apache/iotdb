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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.buffer.DataBlockManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService.Client;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SinkHandleTest {

  @Test
  public void testOneTimeNotBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint(
            "remote", IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    try {
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener = Mockito.mock(SinkHandleListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks);
    sinkHandle.setNoMoreTsBlocks();
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      try {
        sinkHandle.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertTrue(sinkHandle.isFull().isDone());
    }
    Assert.assertFalse(sinkHandle.isFinished());

    // Ack tsblocks.
    sinkHandle.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertTrue(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .free(queryId, numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onFinish(sinkHandle);

    // Close the SinkHandle.
    sinkHandle.close();
    Assert.assertTrue(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onClosed(sinkHandle);
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .onEndOfDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock - 1 == e.getLastSequenceId()));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testMultiTimesBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint(
            "remote", IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(queryId, numOfMockTsBlock, mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);

    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener = Mockito.mock(SinkHandleListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    try {
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks);
    Assert.assertFalse(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException | InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      try {
        sinkHandle.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertFalse(sinkHandle.isFull().isDone());
    }
    Assert.assertFalse(sinkHandle.isFinished());

    // Ack tsblocks.
    sinkHandle.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .free(queryId, numOfMockTsBlock * mockTsBlockSize);

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks);
    Assert.assertFalse(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.times(2))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == numOfMockTsBlock
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Close the SinkHandle.
    sinkHandle.setNoMoreTsBlocks();
    Assert.assertFalse(sinkHandle.isFinished());
    sinkHandle.close();
    Assert.assertTrue(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onClosed(sinkHandle);
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(1))
          .onEndOfDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 - 1 == e.getLastSequenceId()));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks after the SinkHandle is closed.
    for (int i = numOfMockTsBlock; i < numOfMockTsBlock * 2; i++) {
      try {
        sinkHandle.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
    Assert.assertFalse(sinkHandle.isFinished());

    // Ack tsblocks.
    sinkHandle.acknowledgeTsBlock(numOfMockTsBlock, numOfMockTsBlock * 2);
    Assert.assertTrue(sinkHandle.isFinished());
    Assert.assertTrue(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.times(2))
        .free(queryId, numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onFinish(sinkHandle);
  }

  @Test
  public void testFailedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint(
            "remote", IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(queryId, numOfMockTsBlock, mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener = Mockito.mock(SinkHandleListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    TException exception = new TException("Mock exception");
    try {
      Mockito.doThrow(exception)
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doThrow(exception)
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks);
    sinkHandle.setNoMoreTsBlocks();
    Assert.assertFalse(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Thread.sleep(100L);
      Mockito.verify(mockClient, Mockito.times(SinkHandle.MAX_ATTEMPT_TIMES))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onFailure(exception);

    // Close the SinkHandle.
    try {
      sinkHandle.close();
      Assert.fail("Expect an RuntimeException.");
    } catch (RuntimeException e) {
      Assert.assertEquals("Send EndOfDataBlockEvent failed", e.getMessage());
    }
    Assert.assertFalse(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(0)).onClosed(sinkHandle);

    // Abort the SinkHandle.
    sinkHandle.abort();
    Assert.assertTrue(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onAborted(sinkHandle);
    Mockito.verify(mockSinkHandleListener, Mockito.times(0)).onFinish(sinkHandle);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint(
            "remote", IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(queryId, numOfMockTsBlock, mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);

    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener = Mockito.mock(SinkHandleListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    try {
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks);
    Future<?> blocked = sinkHandle.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isClosed());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());

    sinkHandle.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertTrue(blocked.isCancelled());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertTrue(sinkHandle.isClosed());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onAborted(sinkHandle);
  }
}
