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

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class SharedTsBlockQueueTest {

  /**
   * Test that when add() goes into the async listener path (memory blocked) and the queue is
   * aborted before the listener fires, the listener does NOT add the TsBlock to the closed queue.
   * This reproduces the race condition that caused NPE in MemoryPool.free().
   */
  @Test
  public void testAsyncListenerAfterAbortDoesNotAddTsBlock() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L;
    final TFragmentInstanceId fragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String planNodeId = "test";

    // Use a SettableFuture to manually control when the blocked-on-memory future
    // completes.
    SettableFuture<Void> manualFuture = SettableFuture.create();

    // Create a mock MemoryPool that returns the manually-controlled future
    // (simulating blocked).
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Mockito.mock(MemoryPool.class);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);

    // reserveWithPriority() returns blocked future and reserve failure.
    Mockito.when(
            mockMemoryPool.reserveWithPriority(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean()))
        .thenReturn(new MemoryReservationResult(manualFuture, false, 1024L));
    // tryCancel returns 0 — simulating future already completed (can't cancel)
    Mockito.when(mockMemoryPool.tryCancel(Mockito.any())).thenReturn(0L);

    // Use a direct executor so that when we complete manualFuture, the listener
    // runs immediately.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            fragmentInstanceId, planNodeId, mockLocalMemoryManager, newDirectExecutorService());
    queue.getCanAddTsBlock().set(null);
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    TsBlock mockTsBlock = Utils.createMockTsBlock(mockTsBlockSize);

    // Step 1: add() goes into async path — listener is registered on manualFuture.
    // reserve() returns (manualFuture, false), so the TsBlock is NOT yet added to
    // the queue.
    ListenableFuture<Void> addFuture;
    synchronized (queue) {
      addFuture = queue.add(mockTsBlock);
    }
    // The addFuture (channelBlocked) should not be done yet
    Assert.assertFalse(addFuture.isDone());
    // Queue should be empty — TsBlock is waiting for memory
    Assert.assertTrue(queue.isEmpty());

    // Step 2: Abort the queue (simulates upstream FI state change listener calling
    // abort)
    synchronized (queue) {
      queue.abort();
    }
    Assert.assertTrue(queue.isClosed());

    // Step 3: Now complete the manualFuture — this triggers the async listener.
    // Before the fix, this would add the TsBlock to the closed queue.
    // After the fix, the listener detects closed==true and returns without adding.
    manualFuture.set(null);

    // Verify: queue should still be empty (TsBlock was NOT added to the closed
    // queue)
    Assert.assertTrue(queue.isEmpty());
    // The channelBlocked future should be completed (no hang)
    Assert.assertTrue(addFuture.isDone());
  }

  @Test
  public void concurrencyTest() throws Exception {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per
    // query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10 * mockTsBlockSize));
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", memoryManager, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            new TFragmentInstanceId(queryId, 0, "0"),
            "test",
            mockLocalMemoryManager,
            newDirectExecutorService());
    queue.getCanAddTsBlock().set(null);
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    // Reuse an immutable block so mock generation is outside the concurrency deadline.
    TsBlock block = Utils.createMockTsBlock(mockTsBlockSize);
    int blockCount = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CompletionService<Void> completedTasks = new ExecutorCompletionService<>(executor);
    try {
      completedTasks.submit(
          () -> {
            for (int i = 0; i < blockCount; i++) {
              ListenableFuture<Void> blocked;
              synchronized (queue) {
                blocked = queue.add(block);
              }
              blocked.get(10, TimeUnit.SECONDS);
            }
            synchronized (queue) {
              queue.setNoMoreTsBlocks(true);
            }
            return null;
          });
      completedTasks.submit(
          () -> {
            for (int i = 0; i < blockCount; i++) {
              ListenableFuture<Void> blocked;
              synchronized (queue) {
                blocked = queue.isBlocked();
              }
              blocked.get(10, TimeUnit.SECONDS);
              synchronized (queue) {
                Assert.assertSame(block, queue.remove());
              }
            }
            return null;
          });
      for (int i = 0; i < 2; i++) {
        Future<Void> completed = completedTasks.poll(10, TimeUnit.SECONDS);
        Assert.assertNotNull(completed);
        completed.get();
      }
      Assert.assertTrue(queue.hasNoMoreTsBlocks());
      Assert.assertTrue(queue.isEmpty());
      Assert.assertEquals(0, spyMemoryPool.getReservedBytes());
    } finally {
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        synchronized (queue) {
          queue.close();
        }
      }
    }
  }

  @Test
  public void testDependentQueuesKeepIndependentBudgetsByDefault() {
    MemoryPool pool = new MemoryPool("pipeline-chain", new MemoryManager(1024), 512);
    LocalMemoryManager manager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(manager.getQueryPool()).thenReturn(pool);
    ExecutorService executor = newDirectExecutorService();
    TFragmentInstanceId id = new TFragmentInstanceId("pipeline-chain", 0, "0");
    SharedTsBlockQueue upstream = new SharedTsBlockQueue(id, "a", manager, executor);
    SharedTsBlockQueue downstream = new SharedTsBlockQueue(id, "b", manager, executor);
    upstream.setMaxBytesCanReserve(256);
    downstream.setMaxBytesCanReserve(256);
    TsBlock block = Utils.createMockTsBlock(64);
    try {
      for (int i = 0; i < 4; i++) Assert.assertTrue(upstream.add(block).isDone());
      ListenableFuture<Void> fifth = upstream.add(block);
      Assert.assertFalse(fifth.isDone());
      TsBlock forwarded = upstream.remove();
      Assert.assertTrue(fifth.isDone());
      Assert.assertTrue(downstream.add(forwarded).isDone());
      Assert.assertEquals(4, upstream.getNumOfBufferedTsBlocks());
      Assert.assertEquals(1, downstream.getNumOfBufferedTsBlocks());
      Assert.assertEquals(320, pool.getReservedBytes());
      // Both queues are producer-side OUTPUT buffers of the same fragment instance.
      String fullFragmentInstanceId =
          FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id);
      Assert.assertEquals(
          320,
          pool.getFragmentMemoryReservedBytes(
              id.queryId, fullFragmentInstanceId, MemorySide.OUTPUT));
      Assert.assertEquals(
          0,
          pool.getFragmentMemoryReservedBytes(
              id.queryId, fullFragmentInstanceId, MemorySide.INPUT));
    } finally {
      downstream.close();
      upstream.close();
      executor.shutdownNow();
    }
    Assert.assertEquals(0, pool.getReservedBytes());
  }
}
