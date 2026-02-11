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
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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

    // reserve() returns (manualFuture, false) — simulating memory blocked
    Mockito.when(
            mockMemoryPool.reserve(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenReturn(new Pair<>(manualFuture, Boolean.FALSE));
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

  @Test(timeout = 15000L)
  public void concurrencyTest() {
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

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicReference<Integer> numOfTimesSenderBlocked = new AtomicReference<>(0);
    AtomicReference<Integer> numOfTimesReceiverBlocked = new AtomicReference<>(0);
    AtomicReference<Integer> numOfTsBlocksToSend = new AtomicReference<>(1000);
    AtomicReference<Integer> numOfTsBlocksToReceive = new AtomicReference<>(1000);
    executor.submit(
        new SendTask(
            queue, mockTsBlockSize, numOfTsBlocksToSend, numOfTimesSenderBlocked, executor));
    executor.submit(
        new ReceiveTask(queue, numOfTsBlocksToReceive, numOfTimesReceiverBlocked, executor));

    while (numOfTsBlocksToSend.get() != 0 && numOfTsBlocksToReceive.get() != 0) {
      String message =
          String.format(
              "Sender %d: %d, Receiver %d: %d",
              numOfTimesSenderBlocked.get(),
              numOfTsBlocksToSend.get(),
              numOfTimesReceiverBlocked.get(),
              numOfTsBlocksToReceive.get());
      System.out.println(message);
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  private static class SendTask implements Runnable {

    private final SharedTsBlockQueue queue;
    private final long mockTsBlockSize;
    private final AtomicReference<Integer> numOfTsBlocksToSend;
    private final AtomicReference<Integer> numOfTimesBlocked;
    private final ExecutorService executor;

    public SendTask(
        SharedTsBlockQueue queue,
        long mockTsBlockSize,
        AtomicReference<Integer> numOfTsBlocksToSend,
        AtomicReference<Integer> numOfTimesBlocked,
        ExecutorService executor) {
      this.queue = Validate.notNull(queue);
      Validate.isTrue(mockTsBlockSize > 0L);
      this.mockTsBlockSize = mockTsBlockSize;
      this.numOfTsBlocksToSend = Validate.notNull(numOfTsBlocksToSend);
      this.numOfTimesBlocked = Validate.notNull(numOfTimesBlocked);
      this.executor = Validate.notNull(executor);
    }

    @Override
    public void run() {
      ListenableFuture<Void> blockedOnMemory = null;
      while (numOfTsBlocksToSend.get() > 0) {
        synchronized (queue) {
          blockedOnMemory = queue.add(Utils.createMockTsBlock(mockTsBlockSize));
        }
        numOfTsBlocksToSend.updateAndGet(v -> v - 1);
        if (!blockedOnMemory.isDone()) {
          break;
        }
      }

      if (blockedOnMemory != null) {
        numOfTimesBlocked.updateAndGet(v -> v + 1);
        blockedOnMemory.addListener(
            new SendTask(queue, mockTsBlockSize, numOfTsBlocksToSend, numOfTimesBlocked, executor),
            executor);
      } else {
        synchronized (queue) {
          queue.setNoMoreTsBlocks(true);
        }
      }
    }
  }

  private static class ReceiveTask implements Runnable {

    private final SharedTsBlockQueue queue;
    private final AtomicReference<Integer> numOfTsBlocksToReceive;
    private final AtomicReference<Integer> numOfTimesBlocked;
    private final ExecutorService executor;

    public ReceiveTask(
        SharedTsBlockQueue queue,
        AtomicReference<Integer> numOfTsBlocksToReceive,
        AtomicReference<Integer> numOfTimesBlocked,
        ExecutorService executor) {
      this.queue = Validate.notNull(queue);
      this.numOfTsBlocksToReceive = Validate.notNull(numOfTsBlocksToReceive);
      this.numOfTimesBlocked = Validate.notNull(numOfTimesBlocked);
      this.executor = Validate.notNull(executor);
    }

    @Override
    public void run() {
      ListenableFuture<Void> blocked = null;
      while (numOfTsBlocksToReceive.get() > 0) {
        synchronized (queue) {
          blocked = queue.isBlocked();
          if (blocked.isDone()) {
            queue.remove();
            numOfTsBlocksToReceive.updateAndGet(v -> v - 1);
          } else {
            break;
          }
        }
      }

      if (blocked != null) {
        numOfTimesBlocked.updateAndGet(v -> v + 1);
        blocked.addListener(
            new ReceiveTask(queue, numOfTsBlocksToReceive, numOfTimesBlocked, executor), executor);
      }
    }
  }
}
