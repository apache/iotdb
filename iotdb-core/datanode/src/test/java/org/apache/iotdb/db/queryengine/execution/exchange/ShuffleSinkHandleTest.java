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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class ShuffleSinkHandleTest {
  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per read.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", 10 * mockTsBlockSize, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    // Construct a mock SinkListener.
    MPPDataExchangeManager.SinkListener mockSinkListener =
        Mockito.mock(MPPDataExchangeManager.SinkListener.class);
    // Construct a shared tsblock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    // Construct SinkChannel.
    LocalSinkChannel localSinkChannel =
        new LocalSinkChannel(localFragmentInstanceId, queue, mockSinkListener);

    queue.setMaxBytesCanReserve(Long.MAX_VALUE);
    ShuffleSinkHandle shuffleSinkHandle =
        new ShuffleSinkHandle(
            remoteFragmentInstanceId,
            Collections.singletonList(localSinkChannel),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.SIMPLE_ROUND_ROBIN,
            mockSinkListener);

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
    while (shuffleSinkHandle.isFull().isDone()) {
      shuffleSinkHandle.send(Utils.createMockTsBlock(mockTsBlockSize));
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
    shuffleSinkHandle.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertTrue(localSinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.times(1)).onAborted(localSinkChannel);
  }
}
