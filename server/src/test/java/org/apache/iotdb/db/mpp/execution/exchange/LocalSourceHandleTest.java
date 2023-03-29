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

import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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
        new SharedTsBlockQueue(localFragmentInstanceId, localPlanNodeId, mockLocalMemoryManager);

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
        new SharedTsBlockQueue(localFragmentInstanceId, localPlanNodeId, mockLocalMemoryManager);

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
}
