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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.multilevelqueue.DriverTaskHandle;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.multilevelqueue.MultilevelPriorityQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public class MultilevelPriorityQueueTest {
  @Test
  public void testPollBlocked() {
    try {
      IndexedBlockingQueue<DriverTask> queue =
          new MultilevelPriorityQueue(2, 1000, new DriverTask());
      List<DriverTask> res = new ArrayList<>();
      Thread t1 =
          new Thread(
              () -> {
                try {
                  DriverTask e = queue.poll();
                  res.add(e);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  Assert.fail();
                }
              });
      t1.start();
      Thread.sleep(100);
      Assert.assertEquals(Thread.State.WAITING, t1.getState());
      DriverTask e2 = mockDriverTask(mockDriverTaskId(), false);
      queue.push(e2);
      Thread.sleep(100);
      Assert.assertEquals(Thread.State.TERMINATED, t1.getState());
      Assert.assertEquals(1, res.size());
      Assert.assertEquals(e2.getDriverTaskId().toString(), res.get(0).getDriverTaskId().toString());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testPushExceedCapacity() {
    IndexedBlockingQueue<DriverTask> queue = new MultilevelPriorityQueue(2, 1, new DriverTask());
    DriverTask e1 = mockDriverTask(mockDriverTaskId(), false);
    queue.push(e1);
    DriverTask e2 = mockDriverTask(mockDriverTaskId(), false);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> {
          queue.push(e2);
        });
  }

  @Test
  public void testPushAndPoll() {
    try {
      IndexedBlockingQueue<DriverTask> queue =
          new MultilevelPriorityQueue(2, 1000, new DriverTask());
      DriverTask e1 = mockDriverTask(mockDriverTaskId(), false);
      queue.push(e1);
      Assert.assertEquals(1, queue.size());
      DriverTask e2 =
          mockDriverTask(
              new DriverTaskId(
                  new FragmentInstanceId(new PlanFragmentId(new QueryId("test"), 0), "inst-1"), 0),
              false);
      queue.push(e2);
      Assert.assertEquals(2, queue.size());
      Assert.assertEquals(
          e1.getDriverTaskId().toString(), queue.poll().getDriverTaskId().toString());
      Assert.assertEquals(1, queue.size());
      Assert.assertEquals(
          e2.getDriverTaskId().toString(), queue.poll().getDriverTaskId().toString());
      Assert.assertEquals(0, queue.size());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testClear() {
    IndexedBlockingQueue<DriverTask> queue = new MultilevelPriorityQueue(2, 1000, new DriverTask());
    DriverTask e1 = mockDriverTask(mockDriverTaskId(), false);
    queue.push(e1);
    DriverTask e2 =
        mockDriverTask(
            new DriverTaskId(
                new FragmentInstanceId(new PlanFragmentId(new QueryId("test"), 0), "inst-1"), 0),
            false);
    queue.push(e2);
    Assert.assertEquals(2, queue.size());
    queue.clear();
    Assert.assertEquals(0, queue.size());
  }

  @Test
  public void testIsEmpty() {
    try {
      IndexedBlockingQueue<DriverTask> queue =
          new MultilevelPriorityQueue(2, 1000, new DriverTask());
      Assert.assertTrue(queue.isEmpty());
      DriverTask e1 = mockDriverTask(mockDriverTaskId(), false);
      queue.push(e1);
      Assert.assertFalse(queue.isEmpty());
      queue.poll();
      Assert.assertTrue(queue.isEmpty());
      DriverTask e2 =
          mockDriverTask(
              new DriverTaskId(
                  new FragmentInstanceId(new PlanFragmentId(new QueryId("test"), 0), "inst-1"), 0),
              false);
      queue.push(e2);
      Assert.assertFalse(queue.isEmpty());
      queue.poll();
      Assert.assertTrue(queue.isEmpty());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testPushAndPollWithHighestLevelPriority() {
    try {
      MultilevelPriorityQueue queue = new MultilevelPriorityQueue(2, 1000, new DriverTask());
      DriverTask e1 = mockDriverTask(mockDriverTaskId(), true);
      queue.push(e1);
      Assert.assertEquals(1, queue.size());
      Assert.assertEquals(1, queue.getHighestPriorityLevelQueue().size());
      DriverTask e2 =
          mockDriverTask(
              new DriverTaskId(
                  new FragmentInstanceId(new PlanFragmentId(new QueryId("test"), 0), "inst-1"), 0),
              false);
      queue.push(e2);
      Assert.assertEquals(2, queue.size());
      Assert.assertEquals(
          e1.getDriverTaskId().toString(), queue.poll().getDriverTaskId().toString());
      Assert.assertEquals(1, queue.size());
      Assert.assertEquals(0, queue.getHighestPriorityLevelQueue().size());
      Assert.assertEquals(
          e2.getDriverTaskId().toString(), queue.poll().getDriverTaskId().toString());
      Assert.assertEquals(0, queue.size());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private DriverTask mockDriverTask(DriverTaskId driverTaskID, boolean isHighestPriority) {
    DriverScheduler manager = DriverScheduler.getInstance();
    IDriver mockDriver = Mockito.mock(IDriver.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    return new DriverTask(
        mockDriver, 100L, DriverTaskStatus.READY, driverTaskHandle, 0, isHighestPriority);
  }

  private DriverTaskId mockDriverTaskId() {
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    return new DriverTaskId(instanceId, 0);
  }
}
