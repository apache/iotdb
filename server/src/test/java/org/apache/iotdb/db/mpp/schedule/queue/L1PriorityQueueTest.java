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
package org.apache.iotdb.db.mpp.schedule.queue;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class L1PriorityQueueTest {

  @Test
  public void testPollBlocked() throws InterruptedException {
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
    List<QueueElement> res = new ArrayList<>();
    Thread t1 =
        new Thread(
            () -> {
              try {
                QueueElement e = queue.poll();
                res.add(e);
              } catch (InterruptedException e) {
                e.printStackTrace();
                Assert.fail();
              }
            });
    t1.start();
    Thread.sleep(10);
    Assert.assertEquals(Thread.State.WAITING, t1.getState());
    QueueElement e2 = new QueueElement(new QueueElement.QueueElementID(1), 1);
    queue.push(e2);
    Thread.sleep(10);
    Assert.assertEquals(Thread.State.TERMINATED, t1.getState());
    Assert.assertEquals(1, res.size());
    Assert.assertEquals(e2.getId().toString(), res.get(0).getId().toString());
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
    QueueElement e2e = new QueueElement(new QueueElement.QueueElementID(1), 10);
    queue.push(e2e);
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
    QueueElement e1e = new QueueElement(new QueueElement.QueueElementID(1), 20);
    queue.push(e1e);
    // only 1 element with the same id can be put into
    Assert.assertEquals(1, queue.size());
    QueueElement e2 = new QueueElement(new QueueElement.QueueElementID(2), 5);
    queue.push(e2);
    Assert.assertEquals(2, queue.size());
    Assert.assertEquals(e2.getId().toString(), queue.poll().getId().toString());
    Assert.assertEquals(1, queue.size());
    Assert.assertEquals(e1e.getId().toString(), queue.poll().getId().toString());
    Assert.assertEquals(0, queue.size());
  }
}
