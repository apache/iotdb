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

package org.apache.iotdb.db.utils.datastructure;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class FixedPriorityBlockingQueueTest {
  private FixedPriorityBlockingQueue<Integer> queue =
      new FixedPriorityBlockingQueue<>(10, Integer::compare);

  @After
  public void tearDown() {
    queue.clear();
  }

  @Test
  public void testBlockingTake() throws InterruptedException {
    final AtomicBoolean hasTaken = new AtomicBoolean(false);
    Thread t1 =
        new Thread(
            () -> {
              try {
                queue.take();
                hasTaken.set(true);
              } catch (Exception e) {

              }
            });
    Thread t2 =
        new Thread(
            () -> {
              try {
                Assert.assertFalse(hasTaken.get());
                queue.put(1);
                Thread.sleep(500);
                Assert.assertTrue(hasTaken.get());
              } catch (InterruptedException e) {

              }
            });
    t1.start();
    Thread.sleep(100);
    t2.start();
    t1.join();
    t2.join();
  }

  @Test
  public void testPutAndOrder() throws InterruptedException {
    Integer[] integers =
        new Integer[] {
          0, -5, 11, 233, 45, 69, -249, 33, 787, -556, 762, 380, 747, 777, 22, 77, 666, 321, 456,
          575
        };

    for (int integer : integers) {
      queue.put(integer);
    }

    Assert.assertEquals(10, queue.size());
    int prev = queue.take();
    List<Integer> integerList = new ArrayList<>();
    integerList.add(prev);
    for (int i = 0; i < 9; ++i) {
      int curr = queue.take();
      Assert.assertTrue(prev <= curr);
      integerList.add(curr);
      prev = curr;
    }

    Arrays.sort(integers);
    List<Integer> expectedList = Arrays.asList(integers).subList(0, 10);
    Assert.assertArrayEquals(
        expectedList.toArray(new Integer[0]), integerList.toArray(new Integer[0]));
  }

  @Test
  public void testTakeMax() throws InterruptedException {
    Integer[] integers =
        new Integer[] {
          0, -5, 11, 233, 45, 69, -249, 33, 787, -556, 762, 380, 747, 777, 22, 77, 666, 321, 456,
          575
        };

    for (int integer : integers) {
      queue.put(integer);
    }

    Assert.assertEquals(10, queue.size());
    int prev = queue.takeMax();
    List<Integer> integerList = new ArrayList<>();
    integerList.add(prev);
    for (int i = 0; i < 9; ++i) {
      int curr = queue.takeMax();
      Assert.assertTrue(prev >= curr);
      integerList.add(curr);
      prev = curr;
    }
    Arrays.sort(integers);
    List<Integer> expectedList = Arrays.asList(integers).subList(0, 10);
    Collections.reverse(expectedList);
    Assert.assertArrayEquals(
        expectedList.toArray(new Integer[0]), integerList.toArray(new Integer[0]));
  }

  @Test
  public void testConcurrentPut() throws InterruptedException {
    final FixedPriorityBlockingQueue<Integer> testQueue =
        new FixedPriorityBlockingQueue<>(100, Integer::compare);
    List<Integer> expectedList = new CopyOnWriteArrayList<>();
    Thread t1 =
        new Thread(
            () -> {
              try {
                Random random = new Random(1);
                for (int i = 0; i < 30; ++i) {
                  int curr = random.nextInt();
                  expectedList.add(curr);
                  testQueue.put(curr);
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            });
    Thread t2 =
        new Thread(
            () -> {
              try {
                Random random = new Random(2);
                for (int i = 0; i < 30; ++i) {
                  int curr = random.nextInt();
                  expectedList.add(curr);
                  testQueue.put(curr);
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            });
    Thread t3 =
        new Thread(
            () -> {
              try {
                Random random = new Random(3);
                for (int i = 0; i < 30; ++i) {
                  int curr = random.nextInt();
                  expectedList.add(curr);
                  testQueue.put(curr);
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            });
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    Assert.assertEquals(90, testQueue.size());
    List<Integer> resultList = new ArrayList<>();
    int prev = testQueue.take();
    resultList.add(prev);
    while (testQueue.size() > 0) {
      int curr = testQueue.take();
      resultList.add(curr);
      Assert.assertTrue(prev <= curr);
      prev = curr;
    }
    Collections.sort(expectedList);
    Assert.assertArrayEquals(
        expectedList.toArray(new Integer[0]), resultList.toArray(new Integer[0]));
  }

  @Test
  public void testConcurrentTake() throws InterruptedException {
    final FixedPriorityBlockingQueue<Integer> testQueue =
        new FixedPriorityBlockingQueue<>(100, Integer::compare);
    Random random = new Random(10);
    List<Integer> resultList = new ArrayList<>();
    ReentrantLock lock = new ReentrantLock();
    List<Integer> expectedList = new ArrayList<>();
    for (int i = 0; i < 90; ++i) {
      int curr = random.nextInt();
      testQueue.put(curr);
      expectedList.add(curr);
    }
    Collections.sort(expectedList);
    Thread t1 =
        new Thread(
            () -> {
              try {
                while (resultList.size() < 90) {
                  lock.lock();
                  try {
                    if (resultList.size() >= 90) {
                      return;
                    }
                    int curr = testQueue.take();
                    resultList.add(curr);
                  } finally {
                    lock.unlock();
                  }
                }
              } catch (InterruptedException e) {

              }
            });
    Thread t2 =
        new Thread(
            () -> {
              try {
                while (resultList.size() < 90) {
                  lock.lock();
                  try {
                    if (resultList.size() >= 90) {
                      return;
                    }
                    int curr = testQueue.take();
                    resultList.add(curr);
                  } finally {
                    lock.unlock();
                  }
                }
              } catch (InterruptedException e) {

              }
            });
    Thread t3 =
        new Thread(
            () -> {
              try {
                while (resultList.size() < 90) {
                  lock.lock();
                  try {
                    if (resultList.size() >= 90) {
                      return;
                    }
                    int curr = testQueue.take();
                    resultList.add(curr);
                  } finally {
                    lock.unlock();
                  }
                }
              } catch (InterruptedException e) {

              }
            });
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    Assert.assertArrayEquals(
        expectedList.toArray(new Integer[0]), resultList.toArray(new Integer[0]));
  }
}
