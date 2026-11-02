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
package org.apache.iotdb.db.queryengine.execution.schedule.queue;

import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingQueue;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class L1PriorityQueueTest {

  @Test
  public void testPollBlocked() throws Exception {
    IndexedBlockingQueue<QueueElement> queue =
        new L1PriorityQueue<>(
            10,
            (o1, o2) -> {
              if (o1.equals(o2)) {
                return 0;
              }
              return Integer.compare(o1.getValue(), o2.getValue());
            },
            new QueueElement(new QueueElement.QueueElementID(0), 0));
    FutureTask<QueueElement> result = new FutureTask<>(queue::poll);
    Thread t1 = new Thread(result);
    t1.start();
    try {
      await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> result.isDone() || t1.getState() == Thread.State.WAITING);
      if (result.isDone()) {
        Assert.fail("poll completed before an element was pushed: " + result.get());
      }
      QueueElement e2 = new QueueElement(new QueueElement.QueueElementID(1), 1);
      queue.push(e2);
      Assert.assertSame(e2, result.get(10, TimeUnit.SECONDS));
    } finally {
      t1.interrupt();
      t1.join(TimeUnit.SECONDS.toMillis(10));
      Assert.assertFalse(t1.isAlive());
    }
  }

  @Test
  public void testPushExceedCapacity() {
    IndexedBlockingQueue<QueueElement> queue =
        new L1PriorityQueue<>(
            1,
            (o1, o2) -> {
              if (o1.equals(o2)) {
                return 0;
              }
              return Integer.compare(o1.getValue(), o2.getValue());
            },
            new QueueElement(new QueueElement.QueueElementID(0), 0));
    QueueElement e2 = new QueueElement(new QueueElement.QueueElementID(1), 1);
    queue.push(e2);
    QueueElement e3 = new QueueElement(new QueueElement.QueueElementID(2), 2);
    try {
      queue.push(e3);
      Assert.fail();
    } catch (IllegalStateException e) {
      // ignore;
    }
  }

  @Test
  public void testPushAndPoll() throws InterruptedException {
    IndexedBlockingQueue<QueueElement> queue =
        new L1PriorityQueue<>(
            10,
            (o1, o2) -> {
              if (o1.equals(o2)) {
                return 0;
              }
              return Integer.compare(o1.getValue(), o2.getValue());
            },
            new QueueElement(new QueueElement.QueueElementID(0), 0));
    QueueElement e1 = new QueueElement(new QueueElement.QueueElementID(1), 10);
    queue.push(e1);
    Assert.assertEquals(1, queue.size());
    QueueElement e2 = new QueueElement(new QueueElement.QueueElementID(2), 5);
    queue.push(e2);
    Assert.assertEquals(2, queue.size());
    Assert.assertEquals(e2.getDriverTaskId().toString(), queue.poll().getDriverTaskId().toString());
    Assert.assertEquals(1, queue.size());
    Assert.assertEquals(e1.getDriverTaskId().toString(), queue.poll().getDriverTaskId().toString());
    Assert.assertEquals(0, queue.size());
  }

  @Test
  public void testRemoveElement() {
    IndexedBlockingQueue<QueueElement> queue =
        new L1PriorityQueue<>(
            10,
            (o1, o2) -> {
              if (o1.equals(o2)) {
                return 0;
              }
              return Integer.compare(o1.getValue(), o2.getValue());
            },
            new QueueElement(new QueueElement.QueueElementID(0), 0));
    QueueElement e1 = new QueueElement(new QueueElement.QueueElementID(1), 5);
    queue.push(e1);
    Assert.assertEquals(1, queue.size());
    QueueElement e2 = new QueueElement(new QueueElement.QueueElementID(2), 10);
    queue.push(e2);
    Assert.assertEquals(2, queue.size());
    Assert.assertEquals(e2, queue.remove(new QueueElement.QueueElementID(2)));
    Assert.assertEquals(1, queue.size());
  }

  @Test
  public void testClear() {
    IndexedBlockingQueue<QueueElement> queue =
        new L1PriorityQueue<>(
            10,
            (o1, o2) -> {
              if (o1.equals(o2)) {
                return 0;
              }
              return Integer.compare(o1.getValue(), o2.getValue());
            },
            new QueueElement(new QueueElement.QueueElementID(0), 0));
    QueueElement.QueueElementID id1 = new QueueElement.QueueElementID(1);
    QueueElement e1 = new QueueElement(id1, 10);
    queue.push(e1);
    Assert.assertEquals(1, queue.size());
    QueueElement.QueueElementID id2 = new QueueElement.QueueElementID(2);
    QueueElement e2 = new QueueElement(id2, 5);
    queue.push(e2);
    Assert.assertEquals(2, queue.size());
    queue.clear();
    Assert.assertEquals(0, queue.size());
    Assert.assertNull(queue.get(id1));
    Assert.assertNull(queue.get(id2));
  }
}
