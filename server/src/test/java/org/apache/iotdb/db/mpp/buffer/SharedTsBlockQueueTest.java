package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class SharedTsBlockQueueTest {
  @Test(timeout = 5000L)
  public void concurrencyTest() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", 10 * mockTsBlockSize, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(new TFragmentInstanceId(queryId, 0, "0"), mockLocalMemoryManager);

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
        blockedOnMemory = queue.add(Utils.createMockTsBlock(mockTsBlockSize));
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
        queue.setNoMoreTsBlocks(true);
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
        blocked = queue.isBlocked();
        if (blocked.isDone()) {
          queue.remove();
          numOfTsBlocksToReceive.updateAndGet(v -> v - 1);
        } else {
          break;
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
