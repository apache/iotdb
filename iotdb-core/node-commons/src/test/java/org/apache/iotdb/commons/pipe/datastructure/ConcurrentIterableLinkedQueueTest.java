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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

public class ConcurrentIterableLinkedQueueTest {

  private ConcurrentIterableLinkedQueue<Integer> queue;

  @Before
  public void setUp() {
    queue = new ConcurrentIterableLinkedQueue<>();
  }

  @After
  public void shutDown() {
    // Help gc
    queue.clear();
    queue = null;
  }

  @Test(timeout = 60000)
  public void testSeek() {
    queue.add(1);
    queue.add(2);
    queue.add(3);

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    itr.seek(2);
    Assert.assertEquals(Integer.valueOf(3), itr.next());
  }

  @Test(timeout = 60000)
  public void testTimedGet() {
    queue.add(1);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFromEarliest();
    Assert.assertEquals(1, (int) itr.next(1000));
    Assert.assertNull(itr.next(1000));
  }

  @Test(timeout = 60000, expected = IllegalArgumentException.class)
  public void testInsertNull() {
    queue.add(null);
  }

  @Test(timeout = 60000)
  public void testConcurrentAddAndRemove() throws InterruptedException {
    final int numberOfAdds = 500;
    final ExecutorService executor = Executors.newFixedThreadPool(2);

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
            queue.tryRemoveBefore(i);
          }
        });

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    // Validate the state of the queue
    // The actual state depends on the timing of add and remove operations
    Assert.assertTrue(queue.getFirstIndex() <= numberOfAdds);
    Assert.assertTrue(queue.getTailIndex() >= queue.getFirstIndex());
  }

  @Test(timeout = 60000)
  public void testIterateFromEmptyQueue() {
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(1);

    final AtomicInteger value = new AtomicInteger(-1);
    new Thread(() -> value.set(itr.next())).start();
    queue.add(3);
    Awaitility.await().untilAsserted(() -> Assert.assertEquals(3, value.get()));
  }

  @Test(timeout = 60000)
  public void testContinuousEmptyNext() throws InterruptedException {
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    final AtomicInteger consumedValue = new AtomicInteger(0);
    new Thread(
            () -> {
              while (true) {
                Integer value = itr.next(0);
                if (value != null) {
                  consumedValue.set(value);
                }
              }
            })
        .start();
    Thread.sleep(6000);
    queue.add(1);
    Awaitility.await()
        .atMost(100, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertEquals(1, consumedValue.get()));
  }

  @Test(timeout = 60000)
  public void testRemove() {
    queue.add(1);
    queue.add(2);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(1);

    Assert.assertEquals(1, queue.tryRemoveBefore(Long.MAX_VALUE));
    Assert.assertEquals(2, (int) itr.next());
  }

  @Test(timeout = 60000)
  public void testRemoveAgainstNewestItr() {
    queue.add(1);
    queue.add(2);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFromLatest();

    Assert.assertEquals(2, queue.tryRemoveBefore(Long.MAX_VALUE));
    queue.add(3);
    Assert.assertEquals(3, (int) itr.peek(0));
    Assert.assertEquals(3, (int) itr.next(0));
  }

  @Test(timeout = 60000)
  public void testClear() {
    queue.add(1);
    queue.add(2);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(1);
    queue.clear();

    assertFalse(queue.hasAnyIterators());
    Assert.assertEquals(2, queue.getFirstIndex());
    Assert.assertEquals(2, queue.getTailIndex());

    Assert.assertTrue(itr.isClosed());
    Assert.assertNull(itr.next());
    Assert.assertFalse(itr.hasNext());
    Assert.assertEquals(-1, itr.seek(2));
    Assert.assertEquals(-1, itr.getNextIndex());
  }

  @Test(timeout = 60000)
  public void testIntegratedOperations() {
    queue.add(1);
    queue.add(2);
    Assert.assertEquals(1, queue.tryRemoveBefore(1));
    Assert.assertEquals(1, queue.getFirstIndex());
    Assert.assertEquals(2, queue.getTailIndex());

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator it = queue.iterateFromEarliest();
    Assert.assertEquals(2, (int) it.next());

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator it2 = queue.iterateFromLatest();
    Assert.assertEquals(2, it2.getNextIndex());
    final AtomicInteger value = new AtomicInteger(-1);
    new Thread(() -> value.set(it2.next())).start();
    queue.add(3);
    Awaitility.await().untilAsserted(() -> Assert.assertEquals(3, value.get()));

    Assert.assertEquals(1, it.seek(Integer.MIN_VALUE));
    Assert.assertEquals(2, (int) it.next());

    queue.clear();
    Assert.assertEquals(3, queue.getFirstIndex());
    Assert.assertEquals(3, queue.getTailIndex());
  }

  @Test(timeout = 60000)
  public void testConcurrentReadWrite() {
    final AtomicBoolean failure = new AtomicBoolean(false);
    final List<Thread> threadList = new ArrayList<>(102);

    final Thread thread1 =
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

    final Thread thread2 =
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
      final Thread thread =
          new Thread(
              () -> {
                try {
                  ConcurrentIterableLinkedQueue<Integer>.DynamicIterator it =
                      queue.iterateFromEarliest();
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

  @Test(timeout = 60000)
  public void testEmptyQueueBehavior() {
    Assert.assertEquals(0, queue.getFirstIndex());
    Assert.assertEquals(0, queue.getTailIndex());
    Assert.assertFalse(queue.iterateFrom(0).hasNext());
    Assert.assertEquals(0, queue.tryRemoveBefore(10));
    queue.clear();
    Assert.assertEquals(0, queue.getFirstIndex());
    Assert.assertEquals(0, queue.getTailIndex());
  }

  @Test(timeout = 60000)
  public void testBoundaryConditions() {
    queue.add(1);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(10);
    Assert.assertFalse(itr.hasNext());
  }

  @Test(timeout = 60000)
  public void testConcurrentExceptionHandling() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    executor.submit(
        () -> {
          queue.add(1);
          try {
            Thread.sleep(500); // Wait for the iterator to start
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          queue.clear();
        });

    final AtomicBoolean caughtException = new AtomicBoolean(false);
    executor.submit(
        () -> {
          try {
            final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr =
                queue.iterateFromEarliest();
            while (itr.hasNext()) {
              itr.next();
            }
          } catch (Exception e) {
            caughtException.set(true);
          }
        });

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    Assert.assertFalse(caughtException.get());
  }

  @Test(timeout = 60000)
  public void testHighLoadPerformance() {
    int numberOfElements = 100000;
    for (int i = 0; i < numberOfElements; i++) {
      queue.add(i);
    }

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    for (int i = 0; i < numberOfElements; i++) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertEquals(Integer.valueOf(i), itr.next());
    }
    Assert.assertFalse(itr.hasNext());
  }

  @Test(timeout = 60000)
  public void testMultiThreadedConsistency() throws InterruptedException {
    final int numberOfElements = 1000;
    final ExecutorService executor = Executors.newFixedThreadPool(10);

    for (int i = 0; i < numberOfElements; i++) {
      int finalI = i;
      executor.submit(() -> queue.add(finalI));
    }

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFromEarliest();
    final HashSet<Integer> elements = new HashSet<>();
    while (itr.hasNext()) {
      elements.add(itr.next());
    }

    Assert.assertEquals(numberOfElements, elements.size());
  }

  @Test(timeout = 60000)
  public void testIteratorBasicFunctionality() {
    for (int i = 0; i < 5; i++) {
      queue.add(i);
    }

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertEquals(Integer.valueOf(i), itr.next());
    }
    Assert.assertFalse(itr.hasNext());
  }

  @Test(timeout = 60000)
  public void testIteratorAfterRemoval() {
    for (int i = 0; i < 5; i++) {
      queue.add(i);
    }

    queue.tryRemoveBefore(3);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    for (int i = 3; i < 5; i++) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertEquals(Integer.valueOf(i), itr.next());
    }
    Assert.assertFalse(itr.hasNext());
  }

  @Test(timeout = 60000)
  public void testIteratorConcurrentAccess() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      queue.add(i);
    }

    final ExecutorService executor = Executors.newFixedThreadPool(10);
    final AtomicInteger count = new AtomicInteger(0);

    for (int i = 0; i < 10; i++) {
      executor.submit(
          () -> {
            ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
            while (itr.hasNext()) {
              itr.next();
              count.incrementAndGet();
            }
          });
    }

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    Assert.assertEquals(1000, count.get()); // 100 elements iterated by 10 threads
  }

  @Test(timeout = 60000)
  public void testIteratorDuringQueueModification() {
    for (int i = 0; i < 5; i++) {
      queue.add(i);
    }

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertEquals(Integer.valueOf(i), itr.next());
    }

    queue.add(5);

    queue.tryRemoveBefore(itr.getNextIndex());

    for (int i = 3; i <= 5; i++) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertEquals(Integer.valueOf(i), itr.next());
    }

    Assert.assertFalse(itr.hasNext());

    for (int i = 5; i < 10; i++) {
      queue.add(i);
      queue.tryRemoveBefore(100 * i);
    }

    for (int i = 5; i < 10; i++) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertEquals(Integer.valueOf(i), itr.next());
    }

    Assert.assertFalse(itr.hasNext());
  }

  @Test(timeout = 60000)
  public void testIterateAfterRemoveFromEmptyQueue() {
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    Assert.assertFalse(itr.hasNext());
    queue.tryRemoveBefore(0);
    Assert.assertFalse(itr.hasNext());
    itr.next(10);
    Assert.assertEquals(0, itr.getNextIndex());
  }

  @Test(timeout = 60000)
  public void testIteratorExceptionHandling() {
    queue.add(1);
    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    queue.clear();

    Assert.assertFalse(itr.hasNext());
    Assert.assertNull(itr.next());
    Assert.assertTrue(itr.isClosed());
  }

  @Test(timeout = 60000)
  public void testIteratorSeek() {
    for (int i = 0; i < 10; i++) {
      queue.add(i);
    }

    final ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    final long newNextIndex = itr.seek(5);
    Assert.assertEquals(5, newNextIndex);
    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(Integer.valueOf(5), itr.next());
  }
}
