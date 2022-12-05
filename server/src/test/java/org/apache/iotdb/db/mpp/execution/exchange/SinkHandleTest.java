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
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
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

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SinkHandleTest {

  @Test
  public void testOneTimeNotBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
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
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException | IOException e) {
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
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener,
            mockClientManager);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks.get(0));
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(2))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
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

    // Set no-more-tsblocks.
    sinkHandle.setNoMoreTsBlocks();
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_000).times(1))
        .onEndOfBlocks(sinkHandle);

    // Ack tsblocks.
    sinkHandle.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertTrue(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(mockTsBlockSize, sinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
        .free(queryId, numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_0000).times(1)).onFinish(sinkHandle);

    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onEndOfDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock - 1 == e.getLastSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testMultiTimesBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
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
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException | IOException e) {
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
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener,
            mockClientManager);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(2))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
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
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
        .free(queryId, numOfMockTsBlock * mockTsBlockSize);

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(3))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == numOfMockTsBlock
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Set no-more-tsblocks.
    sinkHandle.setNoMoreTsBlocks();
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_000).times(1))
        .onEndOfBlocks(sinkHandle);

    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onEndOfDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 - 1 == e.getLastSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks after no-more-tsblocks is set.
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
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(2))
        .free(queryId, numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_0000).times(1)).onFinish(sinkHandle);
  }

  @Test
  public void testFailedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
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
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doThrow(mockException)
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException | IOException e) {
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
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener,
            mockClientManager);
    sinkHandle.setRetryIntervalInMs(0L);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(2))
        .reserve(queryId, mockTsBlockSize * numOfMockTsBlock);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(SinkHandle.MAX_ATTEMPT_TIMES))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_000).times(1))
        .onFailure(sinkHandle, mockException);

    // Close the SinkHandle.
    sinkHandle.setNoMoreTsBlocks();
    Assert.assertFalse(sinkHandle.isAborted());
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_000).times(0))
        .onEndOfBlocks(sinkHandle);

    // Abort the SinkHandle.
    sinkHandle.abort();
    Assert.assertTrue(sinkHandle.isAborted());
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_0000).times(1)).onAborted(sinkHandle);
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_0000).times(0)).onFinish(sinkHandle);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(
            new MemoryPool(
                "test", numOfMockTsBlock * mockTsBlockSize, numOfMockTsBlock * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener = Mockito.mock(SinkHandleListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (TException | IOException e) {
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
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkHandleListener,
            mockClientManager);
    Assert.assertTrue(sinkHandle.isFull().isDone());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkHandle.send(mockTsBlocks.get(0));
    Future<?> blocked = sinkHandle.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertFalse(sinkHandle.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkHandle.getNumOfBufferedTsBlocks());

    sinkHandle.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertTrue(blocked.isCancelled());
    Assert.assertFalse(sinkHandle.isFinished());
    Assert.assertTrue(sinkHandle.isAborted());
    Assert.assertEquals(0L, sinkHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkHandle.getNumOfBufferedTsBlocks());
    Mockito.verify(mockSinkHandleListener, Mockito.timeout(10_0000).times(1)).onAborted(sinkHandle);
    Assert.assertEquals(0L, spyMemoryPool.getQueryMemoryReservedBytes(queryId));
  }
}
