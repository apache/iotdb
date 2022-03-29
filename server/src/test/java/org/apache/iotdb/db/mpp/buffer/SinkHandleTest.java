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

import org.apache.iotdb.db.mpp.buffer.DataBlockManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService.Client;
import org.apache.iotdb.mpp.rpc.thrift.EndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.NewDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

public class SinkHandleTest {

  @Test
  public void testOneTimeNotBlockedSend() throws TException, InterruptedException {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f0", "0");
    final String remoteOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f1", "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock client.
    Client mockClient = Mockito.mock(Client.class);
    Mockito.doNothing()
        .when(mockClient)
        .onEndOfDataBlockEvent(Mockito.any(EndOfDataBlockEvent.class));
    Mockito.doNothing().when(mockClient).onNewDataBlockEvent(Mockito.any(NewDataBlockEvent.class));
    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener = Mockito.mock(SinkHandleListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            remoteOperatorId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            new TsBlockSerde(),
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
    Thread.sleep(100L);
    Mockito.verify(mockClient, Mockito.times(1))
        .onNewDataBlockEvent(
            Mockito.argThat(
                e ->
                    remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                        && remoteOperatorId.equals(e.getTargetOperatorId())
                        && localFragmentInstanceId.equals(e.getSourceFragnemtInstanceId())
                        && e.getStartSequenceId() == 0
                        && e.getBlockSizes().size() == numOfMockTsBlock));

    // Get tsblocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sinkHandle.getSerializedTsBlock(i);
      Assert.assertTrue(sinkHandle.isFull().isDone());
      if (i < numOfMockTsBlock - 1) {
        Assert.assertFalse(sinkHandle.isFinished());
      } else {
        Assert.assertTrue(sinkHandle.isFinished());
      }
      Assert.assertFalse(sinkHandle.isClosed());
      Assert.assertEquals(
          mockTsBlockSize * (numOfMockTsBlock - 1 - i), sinkHandle.getBufferRetainedSizeInBytes());
    }
    Mockito.verify(mockMemoryPool, Mockito.times(numOfMockTsBlock)).free(queryId, mockTsBlockSize);
    Assert.assertTrue(sinkHandle.isFinished());

    // Close the SinkHandle.
    sinkHandle.close();
    Thread.sleep(100L);
    Assert.assertTrue(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onClosed(sinkHandle);
    Mockito.verify(mockClient, Mockito.times(1)).onEndOfDataBlockEvent(Mockito.any());
    Mockito.verify(mockClient, Mockito.times(1))
        .onEndOfDataBlockEvent(
            Mockito.argThat(
                e ->
                    remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                        && remoteOperatorId.equals(e.getTargetOperatorId())
                        && localFragmentInstanceId.equals(e.getSourceFragnemtInstanceId())));
  }

  @Test
  public void testMultiTimesBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f0", "0");
    final String remoteOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f1", "0");

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
          .onEndOfDataBlockEvent(Mockito.any(EndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(NewDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            remoteOperatorId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            new TsBlockSerde(),
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
                          && remoteOperatorId.equals(e.getTargetOperatorId())
                          && localFragmentInstanceId.equals(e.getSourceFragnemtInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException | InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sinkHandle.getSerializedTsBlock(i);
      if (i < numOfMockTsBlock - 1) {
        Assert.assertFalse(sinkHandle.isFull().isDone());
      } else {
        Assert.assertTrue(sinkHandle.isFull().isDone());
      }
      Assert.assertFalse(sinkHandle.isFinished());
      Assert.assertFalse(sinkHandle.isClosed());
      Assert.assertEquals(
          mockTsBlockSize * (numOfMockTsBlock - 1 - i), sinkHandle.getBufferRetainedSizeInBytes());
    }
    Mockito.verify(mockMemoryPool, Mockito.times(numOfMockTsBlock)).free(queryId, mockTsBlockSize);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());

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
                          && remoteOperatorId.equals(e.getTargetOperatorId())
                          && localFragmentInstanceId.equals(e.getSourceFragnemtInstanceId())
                          && e.getStartSequenceId() == numOfMockTsBlock
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (InterruptedException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks.
    for (int i = numOfMockTsBlock; i < numOfMockTsBlock * 2; i++) {
      sinkHandle.getSerializedTsBlock(i);
      if (i < numOfMockTsBlock * 2 - 1) {
        Assert.assertFalse(sinkHandle.isFull().isDone());
      } else {
        Assert.assertTrue(sinkHandle.isFull().isDone());
      }
      Assert.assertFalse(sinkHandle.isFinished());
      Assert.assertFalse(sinkHandle.isClosed());
      Assert.assertEquals(
          mockTsBlockSize * (numOfMockTsBlock * 2 - 1 - i),
          sinkHandle.getBufferRetainedSizeInBytes());
    }
    Mockito.verify(mockMemoryPool, Mockito.times(numOfMockTsBlock * 2))
        .free(queryId, mockTsBlockSize);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());

    sinkHandle.setNoMoreTsBlocks();
    Assert.assertTrue(sinkHandle.isFinished());

    // Close the SinkHandle.
    sinkHandle.close();
    Assert.assertTrue(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onClosed(sinkHandle);
  }

  @Test
  public void testFailedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final String remoteHostname = "remote";
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, "f0", "0");
    final String remoteOperatorId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, "f1", "0");

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
      Mockito.doThrow(new TException("Mock exception"))
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(EndOfDataBlockEvent.class));
      Mockito.doThrow(new TException("Mock exception"))
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(NewDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkHandle.
    SinkHandle sinkHandle =
        new SinkHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            remoteOperatorId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockClient,
            new TsBlockSerde(),
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
      sinkHandle.send(Collections.singletonList(Mockito.mock(TsBlock.class)));
      Assert.fail("Expect a RuntimeException.");
    } catch (RuntimeException e) {
      Assert.assertEquals("org.apache.thrift.TException: Mock exception", e.getMessage());
    }

    // Close the SinkHandle.
    try {
      sinkHandle.close();
      Assert.fail("Expect a RuntimeException.");
    } catch (RuntimeException e) {
      Assert.assertEquals("org.apache.thrift.TException: Mock exception", e.getMessage());
    }
    Assert.assertFalse(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(0)).onClosed(sinkHandle);

    sinkHandle.abort();
    Assert.assertTrue(sinkHandle.isClosed());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onAborted(sinkHandle);
  }
}
