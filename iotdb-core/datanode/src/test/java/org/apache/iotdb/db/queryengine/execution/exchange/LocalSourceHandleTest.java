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
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class LocalSourceHandleTest {
  @Test
  public void testReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a shared TsBlock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            localPlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId, localPlanNodeId, queue, mockSourceHandleListener);
    Assert.assertFalse(localSourceHandle.isBlocked().isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Assert.assertEquals(0L, localSourceHandle.getBufferRetainedSizeInBytes());

    // Local sink handle produces tsblocks.
    queue.add(Utils.createMockTsBlock(mockTsBlockSize));
    queue.setNoMoreTsBlocks(true);
    Assert.assertTrue(localSourceHandle.isBlocked().isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Assert.assertEquals(mockTsBlockSize, localSourceHandle.getBufferRetainedSizeInBytes());

    // Consume tsblocks.
    Assert.assertTrue(localSourceHandle.isBlocked().isDone());
    localSourceHandle.receive();
    ListenableFuture<?> blocked = localSourceHandle.isBlocked();
    Assert.assertTrue(blocked.isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertTrue(localSourceHandle.isFinished());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onFinished(localSourceHandle);
  }

  @Test
  public void testRetainedMemoryIsFreedWhenTheHandedOverBlockIsReleased() throws Exception {
    final String queryId = "q0";
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);

    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            localPlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            localPlanNodeId,
            queue,
            Mockito.mock(SourceHandleListener.class));

    TsBlock tsBlock = createRealTsBlock();
    long reservedSize = tsBlock.getSizeInBytes();
    queue.add(tsBlock);
    queue.setNoMoreTsBlocks(true);

    localSourceHandle.getSerializedTsBlock(true);
    Assert.assertEquals(0L, localSourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.never())
        .free(queryId, fullFragmentInstanceId, localPlanNodeId, reservedSize);

    localSourceHandle.freeRetainedMemory();
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .free(queryId, fullFragmentInstanceId, localPlanNodeId, reservedSize);

    // releasing again is a no-op
    localSourceHandle.freeRetainedMemory();
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .free(queryId, fullFragmentInstanceId, localPlanNodeId, reservedSize);
  }

  @Test
  public void testAbortReleasesTheHandedOverMemory() throws Exception {
    final String queryId = "q0";
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);

    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            localPlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            localPlanNodeId,
            queue,
            Mockito.mock(SourceHandleListener.class));

    TsBlock tsBlock = createRealTsBlock();
    long reservedSize = tsBlock.getSizeInBytes();
    queue.add(tsBlock);
    queue.setNoMoreTsBlocks(true);
    localSourceHandle.getSerializedTsBlock(true);
    Mockito.verify(mockMemoryPool, Mockito.never())
        .free(queryId, fullFragmentInstanceId, localPlanNodeId, reservedSize);

    // abort frees the memory that was not released by the caller (fallback path)
    localSourceHandle.abort();
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .free(queryId, fullFragmentInstanceId, localPlanNodeId, reservedSize);

    // a late release after abort must not free the same memory twice
    localSourceHandle.freeRetainedMemory();
    Mockito.verify(mockMemoryPool, Mockito.times(1))
        .free(queryId, fullFragmentInstanceId, localPlanNodeId, reservedSize);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a shared tsblock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            localPlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId, localPlanNodeId, queue, mockSourceHandleListener);
    ListenableFuture<?> future = localSourceHandle.isBlocked();
    Assert.assertFalse(future.isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Assert.assertEquals(0L, localSourceHandle.getBufferRetainedSizeInBytes());

    // Close the local source handle.
    localSourceHandle.abort();
    Assert.assertTrue(future.isDone());
    Assert.assertTrue(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onAborted(localSourceHandle);
  }

  /** A real TsBlock so that the real serde can serialize it. */
  private static TsBlock createRealTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(10L);
    builder.declarePosition();
    return builder.build();
  }
}
