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

import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class LocalSinkHandleTest {
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
    // Construct a mock SinkHandleListener.
    SinkHandleListener mockSinkHandleListener =
        Mockito.mock(MPPDataExchangeManager.SinkHandleListener.class);
    // Construct a shared TsBlock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(remoteFragmentInstanceId, remotePlanNodeId, mockLocalMemoryManager);

    // Construct SinkHandle.
    LocalSinkHandle localSinkHandle =
        new LocalSinkHandle(localFragmentInstanceId, queue, mockSinkHandleListener);

    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    // Construct SourceHandle
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            remotePlanNodeId,
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));

    Assert.assertFalse(localSinkHandle.isFull().isDone());
    localSourceHandle.isBlocked();
    // blocked of LocalSinkHandle should be completed after calling isBlocked() of corresponding
    // LocalSourceHandle
    Assert.assertTrue(localSinkHandle.isFull().isDone());
    Assert.assertFalse(localSinkHandle.isFinished());
    Assert.assertFalse(localSinkHandle.isAborted());
    Assert.assertEquals(0L, localSinkHandle.getBufferRetainedSizeInBytes());

    // Send TsBlocks.
    int numOfSentTsblocks = 0;
    while (localSinkHandle.isFull().isDone()) {
      localSinkHandle.send(Utils.createMockTsBlock(mockTsBlockSize));
      numOfSentTsblocks += 1;
    }
    Assert.assertEquals(11, numOfSentTsblocks);
    Assert.assertFalse(localSinkHandle.isFull().isDone());
    Assert.assertFalse(localSinkHandle.isFinished());
    Assert.assertEquals(11 * mockTsBlockSize, localSinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .reserve(
            queryId,
            localFragmentInstanceId.getInstanceId(),
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
    Assert.assertTrue(localSinkHandle.isFull().isDone());
    Assert.assertFalse(localSinkHandle.isFinished());
    Assert.assertEquals(0L, localSinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .free(queryId, localFragmentInstanceId.getInstanceId(), remotePlanNodeId, mockTsBlockSize);

    // Set no-more-TsBlocks.
    localSinkHandle.setNoMoreTsBlocks();
    Assert.assertTrue(localSinkHandle.isFull().isDone());
    Assert.assertTrue(localSinkHandle.isFinished());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onEndOfBlocks(localSinkHandle);
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onFinish(localSinkHandle);
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
    // Construct a mock SinkHandleListener.
    MPPDataExchangeManager.SinkHandleListener mockSinkHandleListener =
        Mockito.mock(MPPDataExchangeManager.SinkHandleListener.class);
    // Construct a shared tsblock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(remoteFragmentInstanceId, remotePlanNodeId, mockLocalMemoryManager);

    // Construct SinkHandle.
    LocalSinkHandle localSinkHandle =
        new LocalSinkHandle(localFragmentInstanceId, queue, mockSinkHandleListener);

    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    // Construct SourceHandle
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            remotePlanNodeId,
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));

    Assert.assertFalse(localSinkHandle.isFull().isDone());
    localSourceHandle.isBlocked();
    // blocked of LocalSinkHandle should be completed after calling isBlocked() of corresponding
    // LocalSourceHandle
    Assert.assertTrue(localSinkHandle.isFull().isDone());
    Assert.assertFalse(localSinkHandle.isFinished());
    Assert.assertFalse(localSinkHandle.isAborted());
    Assert.assertEquals(0L, localSinkHandle.getBufferRetainedSizeInBytes());

    // Send TsBlocks.
    int numOfSentTsblocks = 0;
    while (localSinkHandle.isFull().isDone()) {
      localSinkHandle.send(Utils.createMockTsBlock(mockTsBlockSize));
      numOfSentTsblocks += 1;
    }
    Assert.assertEquals(11, numOfSentTsblocks);
    ListenableFuture<?> blocked = localSinkHandle.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(localSinkHandle.isFinished());
    Assert.assertEquals(11 * mockTsBlockSize, localSinkHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .reserve(
            queryId,
            localFragmentInstanceId.getInstanceId(),
            remotePlanNodeId,
            mockTsBlockSize,
            Long.MAX_VALUE);

    // Abort.
    localSinkHandle.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertFalse(localSinkHandle.isFinished());
    Assert.assertTrue(localSinkHandle.isAborted());
    Mockito.verify(mockSinkHandleListener, Mockito.times(1)).onAborted(localSinkHandle);
  }
}
