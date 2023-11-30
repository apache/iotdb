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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

public class LinkedListMessageQueueTest {

  @Test(timeout = 60000)
  public void testRemoveAndSubscription() {
    LinkedListMessageQueue<Integer> queue = new LinkedListMessageQueue<>();
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
    LinkedListMessageQueue<Integer> queue = new LinkedListMessageQueue<>();
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
