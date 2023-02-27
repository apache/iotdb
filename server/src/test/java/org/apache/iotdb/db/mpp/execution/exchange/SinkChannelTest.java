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
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.mpp.execution.exchange.sink.SinkChannel;
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

public class SinkChannelTest {

  @Test
  public void testOneTimeNotBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 128 * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "fragmentSink_0";
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
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
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
        sinkChannel.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertTrue(sinkChannel.isFull().isDone());
    }
    Assert.assertFalse(sinkChannel.isFinished());

    // Set no-more-tsblocks.
    sinkChannel.setNoMoreTsBlocks();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(1)).onEndOfBlocks(sinkChannel);

    // Ack tsblocks.
    sinkChannel.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertTrue(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(mockTsBlockSize, sinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onFinish(sinkChannel);

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
    final long mockTsBlockSize = 128 * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock,
            mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);

    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
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
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
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
        sinkChannel.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertFalse(sinkChannel.isFull().isDone());
    }
    Assert.assertFalse(sinkChannel.isFinished());

    // Ack tsblocks.
    sinkChannel.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock * mockTsBlockSize);

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(3))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
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
    sinkChannel.setNoMoreTsBlocks();
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(1)).onEndOfBlocks(sinkChannel);

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
        sinkChannel.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
    Assert.assertFalse(sinkChannel.isFinished());

    // Ack tsblocks.
    sinkChannel.acknowledgeTsBlock(numOfMockTsBlock, numOfMockTsBlock * 2);
    Assert.assertTrue(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(2))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onFinish(sinkChannel);
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
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock,
            mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
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
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.setRetryIntervalInMs(0L);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(SinkChannel.MAX_ATTEMPT_TIMES))
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
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(1))
        .onFailure(sinkChannel, mockException);

    // Close the SinkChannel.
    sinkChannel.setNoMoreTsBlocks();
    Assert.assertFalse(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(0)).onEndOfBlocks(sinkChannel);

    // Abort the SinkChannel.
    sinkChannel.abort();
    Assert.assertTrue(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onAborted(sinkChannel);
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(0)).onFinish(sinkChannel);
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
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(
            new MemoryPool(
                "test", numOfMockTsBlock * mockTsBlockSize, numOfMockTsBlock * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
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
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.setMaxBytesCanReserve(Long.MAX_VALUE);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Future<?> blocked = sinkChannel.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());

    sinkChannel.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertTrue(blocked.isCancelled());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertTrue(sinkChannel.isAborted());
    Assert.assertEquals(0L, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onAborted(sinkChannel);
    Assert.assertEquals(0L, spyMemoryPool.getQueryMemoryReservedBytes(queryId));
  }
}
