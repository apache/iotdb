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

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.mpp.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.mpp.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class LocalSinkChannelTest {
  @Test
  public void testSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", 10 * mockTsBlockSize, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct a shared TsBlock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(remoteFragmentInstanceId, remotePlanNodeId, mockLocalMemoryManager);

    // Construct Sink.
    LocalSinkChannel localSinkChannel =
        new LocalSinkChannel(localFragmentInstanceId, queue, mockSinkListener);

    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    // Construct SourceHandle
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            remotePlanNodeId,
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));

    Assert.assertFalse(localSinkChannel.isFull().isDone());
    localSourceHandle.isBlocked();
    // blocked of LocalSinkChannel should be completed after calling isBlocked() of corresponding
    // LocalSourceHandle
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertFalse(localSinkChannel.isAborted());
    Assert.assertEquals(0L, localSinkChannel.getBufferRetainedSizeInBytes());

    // Send TsBlocks.
    int numOfSentTsblocks = 0;
    while (localSinkChannel.isFull().isDone()) {
      localSinkChannel.send(Utils.createMockTsBlock(mockTsBlockSize));
      numOfSentTsblocks += 1;
    }
    Assert.assertEquals(11, numOfSentTsblocks);
    Assert.assertFalse(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertEquals(11 * mockTsBlockSize, localSinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .reserve(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                remoteFragmentInstanceId),
            remotePlanNodeId,
            mockTsBlockSize,
            Long.MAX_VALUE);

    // Receive TsBlocks.
    int numOfReceivedTsblocks = 0;
    while (!queue.isEmpty()) {
      queue.remove();
      numOfReceivedTsblocks += 1;
    }
    Assert.assertEquals(11, numOfReceivedTsblocks);
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertEquals(0L, localSinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                remoteFragmentInstanceId),
            remotePlanNodeId,
            mockTsBlockSize);

    // Set no-more-TsBlocks.
    localSinkChannel.setNoMoreTsBlocks();
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertTrue(localSinkChannel.isFinished());
    Mockito.verify(mockSinkListener, Mockito.times(1)).onEndOfBlocks(localSinkChannel);
    Mockito.verify(mockSinkListener, Mockito.times(1)).onFinish(localSinkChannel);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", 10 * mockTsBlockSize, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct a shared tsblock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(remoteFragmentInstanceId, remotePlanNodeId, mockLocalMemoryManager);

    // Construct SinkChannel.
    LocalSinkChannel localSinkChannel =
        new LocalSinkChannel(localFragmentInstanceId, queue, mockSinkListener);

    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    // Construct SourceHandle
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            remotePlanNodeId,
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));

    Assert.assertFalse(localSinkChannel.isFull().isDone());
    localSourceHandle.isBlocked();
    // blocked of LocalSinkChannel should be completed after calling isBlocked() of corresponding
    // LocalSourceHandle
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertFalse(localSinkChannel.isAborted());
    Assert.assertEquals(0L, localSinkChannel.getBufferRetainedSizeInBytes());

    // Send TsBlocks.
    int numOfSentTsblocks = 0;
    while (localSinkChannel.isFull().isDone()) {
      localSinkChannel.send(Utils.createMockTsBlock(mockTsBlockSize));
      numOfSentTsblocks += 1;
    }
    Assert.assertEquals(11, numOfSentTsblocks);
    ListenableFuture<?> blocked = localSinkChannel.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertEquals(11 * mockTsBlockSize, localSinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .reserve(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                remoteFragmentInstanceId),
            remotePlanNodeId,
            mockTsBlockSize,
            Long.MAX_VALUE);

    // Abort.
    localSinkChannel.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertTrue(localSinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.times(1)).onAborted(localSinkChannel);
  }
}
