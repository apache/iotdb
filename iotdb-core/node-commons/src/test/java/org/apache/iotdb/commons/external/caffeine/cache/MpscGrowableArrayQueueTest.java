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

package org.apache.iotdb.commons.external.caffeine.cache;

import org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness.awaitUninterruptibly;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness.executor;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;
import static org.junit.Assert.assertThrows;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"ClassEscapesDefinedScope", "PMD.LooseCoupling"})
public final class MpscGrowableArrayQueueTest {
  private static final int NUM_PRODUCERS = 10;
  private static final int PRODUCE = 100;

  private static final int POPULATED_SIZE = 10;
  private static final int FULL_SIZE = 32;

  /* --------------- Constructor --------------- */

  @Test
  public void constructor_initialCapacity_tooSmall() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new MpscGrowableArrayQueue<Integer>(/* initialCapacity= */ 1, /* maxCapacity= */ 4));
  }

  @Test
  public void constructor_maxCapacity_tooSmall() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new MpscGrowableArrayQueue<Integer>(/* initialCapacity= */ 4, /* maxCapacity= */ 1));
  }

  @Test
  public void constructor_inverted() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new MpscGrowableArrayQueue<Integer>(/* initialCapacity= */ 8, /* maxCapacity= */ 4));
  }

  @Test
  public void constructor() {
    var queue = new MpscGrowableArrayQueue<Integer>(/* initialCapacity= */ 4, /* maxCapacity= */ 8);
    assertThat(queue.capacity()).isEqualTo(8);
  }

  /* --------------- Size --------------- */

  @Test
  public void size_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.size()).isEqualTo(0);
  }

  @Test
  public void size_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.size()).isEqualTo(POPULATED_SIZE);
  }

  /* --------------- Offer --------------- */

  @Test
  public void offer_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.offer(1)).isTrue();
    assertThat(queue).hasSize(1);
  }

  @Test
  public void offer_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.offer(1)).isTrue();
    assertThat(queue).hasSize(POPULATED_SIZE + 1);
  }

  @Test
  public void offer_whenFull() {
    var queue = makePopulated(FULL_SIZE);
    assertThat(queue.offer(1)).isFalse();
    assertThat(queue).hasSize(FULL_SIZE);
  }

  @Test
  public void relaxedOffer_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.relaxedOffer(1)).isTrue();
    assertThat(queue).hasSize(1);
  }

  @Test
  public void relaxedOffer_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.relaxedOffer(1)).isTrue();
    assertThat(queue).hasSize(POPULATED_SIZE + 1);
  }

  @Test
  public void relaxedOffer_whenFull() {
    var queue = makePopulated(FULL_SIZE);
    assertThat(queue.relaxedOffer(1)).isFalse();
    assertThat(queue).hasSize(FULL_SIZE);
  }

  /* --------------- Poll --------------- */

  @Test
  public void poll_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.poll()).isNull();
  }

  @Test
  public void poll_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.poll()).isNotNull();
    assertThat(queue).hasSize(POPULATED_SIZE - 1);
  }

  @Test
  @SuppressWarnings("StatementWithEmptyBody")
  public void poll_toEmpty() {
    var queue = makePopulated(FULL_SIZE);
    while (queue.poll() != null) {
      /* consume */
    }
    assertThat(queue).isEmpty();
  }

  @Test
  public void relaxedPoll_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.relaxedPoll()).isNull();
  }

  @Test
  public void relaxedPoll_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.relaxedPoll()).isNotNull();
    assertThat(queue).hasSize(POPULATED_SIZE - 1);
  }

  @Test
  @SuppressWarnings("StatementWithEmptyBody")
  public void relaxedPoll_toEmpty() {
    var queue = makePopulated(FULL_SIZE);
    while (queue.relaxedPoll() != null) {
      /* consume */
    }
    assertThat(queue).isEmpty();
  }

  /* --------------- Peek --------------- */

  @Test
  public void peek_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.peek()).isNull();
  }

  @Test
  public void peek_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.peek()).isNotNull();
    assertThat(queue).hasSize(POPULATED_SIZE);
  }

  @Test
  public void peek_toEmpty() {
    var queue = makePopulated(FULL_SIZE);
    for (int i = 0; i < FULL_SIZE; i++) {
      assertThat(queue.peek()).isNotNull();
      var item = queue.poll();
      assertThat(item).isNotNull();
    }
    assertThat(queue.peek()).isNull();
  }

  @Test
  public void relaxedPeek_whenEmpty() {
    var queue = makePopulated(0);
    assertThat(queue.relaxedPeek()).isNull();
  }

  @Test
  public void relaxedPeek_whenPopulated() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.relaxedPeek()).isNotNull();
    assertThat(queue).hasSize(POPULATED_SIZE);
  }

  @Test
  public void relaxedPeek_toEmpty() {
    var queue = makePopulated(FULL_SIZE);
    for (int i = 0; i < FULL_SIZE; i++) {
      assertThat(queue.relaxedPeek()).isNotNull();
      var item = queue.poll();
      assertThat(item).isNotNull();
    }
    assertThat(queue.relaxedPeek()).isNull();
  }

  /* --------------- Miscellaneous --------------- */

  @Test
  public void iterator() {
    var queue = makePopulated(FULL_SIZE);
    assertThrows(UnsupportedOperationException.class, queue::iterator);
  }

  @Test
  public void inspection() {
    var queue = makePopulated(POPULATED_SIZE);
    assertThat(queue.currentConsumerIndex()).isEqualTo(0);
    assertThat(queue.currentProducerIndex()).isEqualTo(POPULATED_SIZE);
  }

  @Test
  public void getNextBufferSize_invalid() {
    var queue = makePopulated(FULL_SIZE);
    var buffer = new Integer[FULL_SIZE + 1];
    assertThrows(IllegalStateException.class, () -> queue.getNextBufferSize(buffer));
  }

  @Test
  public void toString_nondescript() {
    var empty = makePopulated(0);
    var full = makePopulated(FULL_SIZE);
    assertThat(empty.toString()).contains(empty.getClass().getSimpleName());
    assertThat(full.toString()).contains(empty.getClass().getSimpleName());
  }

  /* --------------- Concurrency --------------- */

  @Test
  @SuppressWarnings("StatementWithEmptyBody")
  public void oneProducer_oneConsumer() {
    var queue = makePopulated(0);
    var latch = new CountDownLatch(2);
    var producer =
        CompletableFuture.runAsync(
            () -> {
              latch.countDown();
              awaitUninterruptibly(latch);
              for (int i = 0; i < PRODUCE; i++) {
                while (!queue.offer(i)) {
                  /* produce */
                }
              }
            },
            executor);
    var consumer =
        CompletableFuture.runAsync(
            () -> {
              latch.countDown();
              awaitUninterruptibly(latch);
              for (int i = 0; i < PRODUCE; i++) {
                while (queue.poll() == null) {
                  /* consume */
                }
              }
            },
            executor);
    CompletableFuture.allOf(producer, consumer).join();
    assertThat(queue).isEmpty();
  }

  @Test
  public void manyProducers_noConsumer() {
    var queue = makePopulated(0);
    var count = new AtomicInteger();
    ConcurrentTestHarness.timeTasks(
        NUM_PRODUCERS,
        () -> {
          for (int i = 0; i < PRODUCE; i++) {
            if (queue.offer(i)) {
              count.incrementAndGet();
            }
          }
        });
    assertThat(queue).hasSize(count.get());
  }

  @Test
  @SuppressWarnings("StatementWithEmptyBody")
  public void manyProducers_oneConsumer() {
    var queue = makePopulated(0);
    var latch = new CountDownLatch(NUM_PRODUCERS + 1);
    var consumer =
        CompletableFuture.runAsync(
            () -> {
              latch.countDown();
              awaitUninterruptibly(latch);
              for (int i = 0; i < (NUM_PRODUCERS * PRODUCE); i++) {
                while (queue.poll() == null) {
                  /* consume */
                }
              }
            },
            executor);
    ConcurrentTestHarness.timeTasks(
        NUM_PRODUCERS,
        () -> {
          latch.countDown();
          awaitUninterruptibly(latch);
          for (int i = 0; i < PRODUCE; i++) {
            while (!queue.offer(i)) {
              /* produce */
            }
          }
        });
    consumer.join();
    assertThat(queue).isEmpty();
  }

  @Test
  public void offer_unknownResult() {
    var queue =
        new MpscGrowableArrayQueue<Integer>(2, 8) {
          @Override
          int offerSlowPath(long mask, long pIndex, long producerLimit) {
            return -1;
          }
        };
    queue.producerLimit = 0;
    assertThat(queue.offer(1)).isTrue();
    assertThat(queue.poll()).isEqualTo(1);
  }

  @Test
  public void offer_resizeFails() {
    var queue =
        new MpscGrowableArrayQueue<Integer>(2, 8) {
          @Override
          protected long availableInQueue(long pIndex, long cIndex) {
            return -1;
          }

          @Override
          int offerSlowPath(long mask, long pIndex, long producerLimit) {
            return 3;
          }
        };
    queue.producerLimit = 0;
    assertThrows(IllegalStateException.class, () -> queue.offer(1));
  }

  @Test
  public void offer_resizeOutOfMemory() {
    var failOnAllocate = new AtomicBoolean();
    var queue =
        new MpscGrowableArrayQueue<>(4, 16) {
          @Override
          Object[] allocate(int capacity) {
            if (failOnAllocate.get()) {
              throw new OutOfMemoryError();
            }
            return super.allocate(capacity);
          }
        };

    // Fill until the next offer triggers a resize
    while (queue.size() < 3) {
      assertThat(queue.offer(queue.size())).isTrue();
    }

    // Fail the resize allocation and verify OOME is thrown
    failOnAllocate.set(true);
    assertThrows(OutOfMemoryError.class, () -> queue.offer(99));

    // Verify the queue is still usable after the OOME
    failOnAllocate.set(false);
    assertThat(queue.offer(99)).isTrue();
    assertThat(queue).hasSize(4);
    for (int i = 0; i < 4; i++) {
      assertThat(queue.poll()).isNotNull();
    }
    assertThat(queue).isEmpty();
  }

  @Test
  public void offer_resizeOutOfMemory_concurrent() throws Exception {
    var failOnAllocate = new AtomicBoolean();
    var resizing = new CountDownLatch(1);
    var releaseOome = new CountDownLatch(1);
    var queue =
        new MpscGrowableArrayQueue<>(4, 16) {
          @Override
          Object[] allocate(int capacity) {
            if (failOnAllocate.compareAndSet(true, false)) {
              resizing.countDown();
              awaitUninterruptibly(releaseOome);
              throw new OutOfMemoryError();
            }
            return super.allocate(capacity);
          }
        };

    while (queue.size() < 3) {
      assertThat(queue.offer(queue.size())).isTrue();
    }
    failOnAllocate.set(true);

    // Producer A: triggers resize, blocks in allocate while producerIndex is odd
    var producerA =
        executor.submit(() -> assertThrows(OutOfMemoryError.class, () -> queue.offer(99)));
    awaitUninterruptibly(resizing);

    // Producer B: offers while A holds producerIndex odd and must make progress after A throws
    var producerB = executor.submit(() -> assertThat(queue.offer(100)).isTrue());

    // Release A's OOME. The catch in resize() restores producerIndex, unblocking B.
    releaseOome.countDown();

    producerA.get(10, TimeUnit.SECONDS);
    producerB.get(10, TimeUnit.SECONDS);

    assertThat(queue).hasSize(4);
    for (int i = 0; i < 4; i++) {
      assertThat(queue.poll()).isNotNull();
    }
    assertThat(queue).isEmpty();
  }

  private static MpscGrowableArrayQueue<Integer> makePopulated(int items) {
    var queue = new MpscGrowableArrayQueue<Integer>(4, FULL_SIZE);
    for (int i = 0; i < items; i++) {
      queue.add(i);
    }
    return queue;
  }
}
