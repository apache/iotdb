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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.disruptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DisruptorShutdownTest {

  @Test
  public void testBatchEventProcessorDrainsPublishedEventsOnShutdownInterrupt() throws Exception {
    final RingBuffer<TestEvent> ringBuffer = RingBuffer.createMultiProducer(TestEvent::new, 32);
    ringBuffer.publishEvent((event, sequence, value) -> event.value = value, 1);

    final TestSequenceBarrier barrier = new TestSequenceBarrier(0L);
    final AtomicInteger handledEventCount = new AtomicInteger();
    final BatchEventProcessor<TestEvent> processor =
        new BatchEventProcessor<>(
            ringBuffer,
            barrier,
            (event, sequence, endOfBatch) -> handledEventCount.incrementAndGet());

    final Thread processorThread = new Thread(processor, "pipe-batch-event-processor-test");
    processorThread.start();

    Assert.assertTrue(barrier.awaitWaitForCall());
    processor.halt();
    barrier.interruptWait();

    processorThread.join(TimeUnit.SECONDS.toMillis(5));

    Assert.assertFalse(processorThread.isAlive());
    Assert.assertEquals(1, handledEventCount.get());
    Assert.assertEquals(0L, processor.getSequence().get());
  }

  @Test
  public void testBatchEventProcessorDrainsEventsPublishedAfterCurrentBatchWhenHalting()
      throws Exception {
    final RingBuffer<TestEvent> ringBuffer = RingBuffer.createMultiProducer(TestEvent::new, 32);
    ringBuffer.publishEvent((event, sequence, value) -> event.value = value, 1);
    ringBuffer.publishEvent((event, sequence, value) -> event.value = value, 2);

    final SnapshotSequenceBarrier barrier = new SnapshotSequenceBarrier(0L, 1L);
    final AtomicInteger handledEventCount = new AtomicInteger();
    final AtomicReference<BatchEventProcessor<TestEvent>> processorReference =
        new AtomicReference<>();
    final BatchEventProcessor<TestEvent> processor =
        new BatchEventProcessor<>(
            ringBuffer,
            barrier,
            (event, sequence, endOfBatch) -> {
              handledEventCount.incrementAndGet();
              if (event.value == 1) {
                processorReference.get().halt();
              }
            });
    processorReference.set(processor);

    final Thread processorThread =
        new Thread(processor, "pipe-batch-event-processor-snapshot-test");
    processorThread.start();
    processorThread.join(TimeUnit.SECONDS.toMillis(5));

    Assert.assertFalse(processorThread.isAlive());
    Assert.assertEquals(2, handledEventCount.get());
    Assert.assertEquals(1L, processor.getSequence().get());
  }

  @Test
  public void testDisruptorShutdownInterruptsWaitingProcessor() throws Exception {
    final AtomicReference<Thread> processorThreadReference = new AtomicReference<>();
    final ThreadFactory threadFactory =
        runnable -> {
          final Thread thread = new Thread(runnable, "pipe-disruptor-shutdown-test");
          processorThreadReference.set(thread);
          return thread;
        };

    final Disruptor<TestEvent> disruptor = new Disruptor<>(TestEvent::new, 32, threadFactory);
    disruptor.handleEventsWith((event, sequence, endOfBatch) -> {});
    disruptor.start();

    final Thread processorThread = processorThreadReference.get();
    Assert.assertNotNull(processorThread);

    TimeUnit.MILLISECONDS.sleep(50);
    disruptor.shutdown();

    Assert.assertFalse(processorThread.isAlive());
  }

  private static class TestEvent {
    private int value;
  }

  private static class TestSequenceBarrier extends SequenceBarrier {

    private final long cursor;
    private final CountDownLatch waitForCalled = new CountDownLatch(1);
    private final CountDownLatch interruptWait = new CountDownLatch(1);

    private TestSequenceBarrier(final long cursor) {
      super(new MultiProducerSequencer(32, new Sequence[0]), new Sequence[0]);
      this.cursor = cursor;
    }

    @Override
    public long waitFor(final long sequence) throws InterruptedException {
      waitForCalled.countDown();
      interruptWait.await();
      throw new InterruptedException();
    }

    @Override
    public long getCursor() {
      return cursor;
    }

    @Override
    public long getHighestPublishedSequence(final long lowerBound, final long availableSequence) {
      return availableSequence;
    }

    private boolean awaitWaitForCall() throws InterruptedException {
      return waitForCalled.await(5, TimeUnit.SECONDS);
    }

    private void interruptWait() {
      interruptWait.countDown();
    }
  }

  private static class SnapshotSequenceBarrier extends SequenceBarrier {

    private final long waitForResult;
    private final long cursor;

    private SnapshotSequenceBarrier(final long waitForResult, final long cursor) {
      super(new MultiProducerSequencer(32, new Sequence[0]), new Sequence[0]);
      this.waitForResult = waitForResult;
      this.cursor = cursor;
    }

    @Override
    public long waitFor(final long sequence) {
      return waitForResult;
    }

    @Override
    public long getCursor() {
      return cursor;
    }

    @Override
    public long getHighestPublishedSequence(final long lowerBound, final long availableSequence) {
      return availableSequence;
    }
  }
}
