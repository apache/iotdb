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

import org.awaitility.Awaitility;
import org.junit.After;
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

import static org.junit.Assert.assertFalse;

public class ConcurrentIterableLinkedQueueTest {

  ConcurrentIterableLinkedQueue<Integer> queue;

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

    ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(0);
    itr.seek(2);
    Assert.assertEquals(Integer.valueOf(3), itr.next());
  }

  @Test(timeout = 60000)
  public void testTimedGet() {
    queue.add(1);
    ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFromEarliest();
    Assert.assertEquals(1, (int) itr.next(1000));
    Assert.assertNull(itr.next(1000));
  }

  @Test(timeout = 60000, expected = IllegalArgumentException.class)
  public void testInsertNull() {
    queue.add(null);
  }

  @Test(timeout = 60000)
  public void testConcurrentAddAndRemove() throws InterruptedException {
    ConcurrentIterableLinkedQueue<Integer> queue = new ConcurrentIterableLinkedQueue<>();

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
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    // Validate the state of the queue
    // The actual state depends on the timing of add and remove operations
    Assert.assertTrue(queue.getFirstIndex() <= numberOfAdds);
    Assert.assertTrue(queue.getLastIndex() >= queue.getFirstIndex());
  }

  @Test(timeout = 60000)
  public void iterateFromEmptyQueue() {
    ConcurrentIterableLinkedQueue<Integer>.DynamicIterator itr = queue.iterateFrom(1);

    AtomicInteger value = new AtomicInteger(-1);
    new Thread(() -> value.set(itr.next())).start();
    queue.add(3);
    Awaitility.await().untilAsserted(() -> Assert.assertEquals(3, value.get()));
  }

  @Test(timeout = 60000)
  public void testIntegratedOperations() {
    queue.add(1);
    queue.add(2);
    queue.removeBefore(1);
    Assert.assertEquals(1, queue.getFirstIndex());
    Assert.assertEquals(2, queue.getLastIndex());

    ConcurrentIterableLinkedQueue<Integer>.DynamicIterator it = queue.iterateFromEarliest();
    Assert.assertEquals(2, (int) it.next());

    ConcurrentIterableLinkedQueue<Integer>.DynamicIterator it2 = queue.iterateFromLatest();
    Assert.assertEquals(2, it2.getOffset());
    AtomicInteger value = new AtomicInteger(-1);
    new Thread(() -> value.set(it2.next())).start();
    queue.add(3);
    Awaitility.await().untilAsserted(() -> Assert.assertEquals(3, value.get()));

    Assert.assertEquals(1, it.seek(Integer.MIN_VALUE));
    Assert.assertEquals(2, (int) it.next());

    queue.clear();
    Assert.assertEquals(0, queue.getFirstIndex());
    Assert.assertEquals(0, queue.getLastIndex());
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
}
