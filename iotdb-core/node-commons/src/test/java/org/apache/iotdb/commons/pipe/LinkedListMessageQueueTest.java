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

package org.apache.iotdb.commons.pipe;

import org.apache.iotdb.commons.pipe.schema.LinkedListMessageQueue;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LinkedListMessageQueueTest {

  private LinkedListMessageQueue<Integer> queue;

  @Before
  public void setUp() {
    queue = new LinkedListMessageQueue<>();
  }

  @Test
  public void testAddAndRemove() {
    Assert.assertTrue(queue.add(1));
    Assert.assertTrue(queue.add(2));
    Assert.assertEquals(2, queue.getLastIndex());

    queue.removeBefore(1);
    Assert.assertEquals(1, queue.getFirstIndex());
  }

  @Test
  public void testClear() {
    queue.add(1);
    queue.add(2);
    queue.clear();
    Assert.assertEquals(0, queue.getSubscriptionNum());
    Assert.assertEquals(0, queue.getFirstIndex());
    Assert.assertEquals(0, queue.getLastIndex());
  }

  @Test
  public void testSubscribeAndNext() {
    queue.add(1);
    queue.add(2);

    LinkedListMessageQueue<Integer>.ConsumerItr itr = queue.subscribe(0);
    Assert.assertNotNull(itr);
    Assert.assertEquals(Integer.valueOf(1), itr.next());
    Assert.assertEquals(Integer.valueOf(2), itr.next());
  }

  @Test
  public void testSubscribeEarliestAndLatest() {
    queue.add(1);
    queue.add(2);

    LinkedListMessageQueue<Integer>.ConsumerItr earliest = queue.subscribeEarliest();
    LinkedListMessageQueue<Integer>.ConsumerItr latest = queue.subscribeLatest();

    Assert.assertEquals(Integer.valueOf(1), earliest.next());
    Assert.assertFalse(latest.hasNext());
  }

  @Test
  public void testSeek() {
    queue.add(1);
    queue.add(2);
    queue.add(3);

    LinkedListMessageQueue<Integer>.ConsumerItr itr = queue.subscribe(0);
    itr.seek(2);
    Assert.assertEquals(Integer.valueOf(3), itr.next());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRemoveBeforeInvalidIndex() {
    queue.add(1);
    queue.removeBefore(0); // This should throw an IllegalArgumentException
  }

  @Test
  public void testConcurrentAdd() throws InterruptedException {
    int numberOfThreads = 10;
    int numberOfAdds = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
    AtomicInteger counter = new AtomicInteger();

    for (int i = 0; i < numberOfThreads; i++) {
      executor.submit(
          () -> {
            for (int j = 0; j < numberOfAdds; j++) {
              queue.add(counter.getAndIncrement());
            }
          });
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    assertEquals(numberOfThreads * numberOfAdds, queue.getLastIndex());
  }

  @Test
  public void testConcurrentSubscribeAndRead() throws InterruptedException {
    int numberOfThreads = 5;
    int numberOfAdds = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
    for (int i = 0; i < numberOfAdds; i++) {
      queue.add(i);
    }

    for (int i = 0; i < numberOfThreads; i++) {
      executor.submit(
          () -> {
            LinkedListMessageQueue<Integer>.ConsumerItr itr = queue.subscribeEarliest();
            while (itr.hasNext()) {
              // Processing the element
              itr.next();
            }
          });
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
  }

  @Test
  public void testConcurrentAddAndRemove() throws InterruptedException {
    int numberOfAdds = 500;
    ExecutorService executor = Executors.newFixedThreadPool(2);

    // Thread 1 adds elements to the queue
    executor.submit(
        () -> {
          for (int i = 0; i < numberOfAdds; i++) {
            queue.add(i);
          }
        });

    // Thread 2 removes elements before a certain index
    executor.submit(
        () -> {
          for (int i = 0; i < numberOfAdds; i += 10) {
            queue.removeBefore(i);
          }
        });

    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    // Validate the state of the queue
    // The actual state depends on the timing of add and remove operations
    assertTrue(queue.getFirstIndex() <= numberOfAdds);
    assertTrue(queue.getLastIndex() >= queue.getFirstIndex());
  }

  @Test(timeout = 60000)
  public void testConcurrentRemoveAndSubscription() {
    queue.add(1);
    queue.add(2);
    queue.removeBefore(1);
    LinkedListMessageQueue<Integer>.ConsumerItr it = queue.subscribeEarliest();
    Assert.assertEquals(2, (int) it.next());

    LinkedListMessageQueue<Integer>.ConsumerItr it2 = queue.subscribeLatest();
    Assert.assertEquals(2, it2.getOffset());
    AtomicInteger value = new AtomicInteger(-1);
    new Thread(() -> value.set(it2.next())).start();
    queue.add(3);
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertEquals(3, value.get()));

    Assert.assertEquals(1, it.seek(Integer.MIN_VALUE));
    Assert.assertEquals(2, (int) it.next());

    queue.clear();
    Assert.assertEquals(0, queue.getFirstIndex());
  }

  @Test(timeout = 60000)
  public void testConcurrentReadWrite() {
    AtomicBoolean failure = new AtomicBoolean(false);
    List<Thread> threadList = new ArrayList<>(102);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 10000; ++i) {
                  queue.add(1);
                }
              } catch (Exception e) {
                failure.set(true);
              }
            });
    threadList.add(thread1);
    thread1.start();

    Thread thread2 =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 10000; ++i) {
                  queue.add(2);
                }
              } catch (Exception e) {
                failure.set(true);
              }
            });
    threadList.add(thread2);
    thread2.start();

    for (int i = 0; i < 100; ++i) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  LinkedListMessageQueue<Integer>.ConsumerItr it = queue.subscribeEarliest();
                  for (int j = 0; j < 20000; ++j) {
                    it.next();
                  }
                } catch (Exception e) {
                  failure.set(true);
                }
              });
      threadList.add(thread);
      thread.start();
    }

    Awaitility.await()
        .until(
            () -> {
              for (Thread thread : threadList) {
                if (thread.isAlive()) {
                  return false;
                }
              }
              return true;
            });
    assertFalse(failure.get());
  }
}
