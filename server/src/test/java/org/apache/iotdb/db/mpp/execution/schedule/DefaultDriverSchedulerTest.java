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
package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue.DriverTaskHandle;
import org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue.MultilevelPriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskStatus;
import org.apache.iotdb.db.utils.stats.CpuTimer;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;

import io.airlift.units.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DefaultDriverSchedulerTest {

  private final DriverScheduler manager = DriverScheduler.getInstance();

  @After
  public void tearDown() throws IOException {
    clear();
  }

  @Test
  public void testBlockedToReady() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    IDriver mockDriver = Mockito.mock(IDriver.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    DriverTaskStatus[] invalidStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.FINISHED,
          DriverTaskStatus.ABORTED,
          DriverTaskStatus.READY,
          DriverTaskStatus.RUNNING,
        };
    for (DriverTaskStatus status : invalidStates) {
      DriverTask testTask = new DriverTask(mockDriver, 100L, status, driverTaskHandle);
      manager.getBlockedTasks().add(testTask);
      Set<DriverTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId, taskSet);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.blockedToReady(testTask);
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertTrue(manager.getBlockedTasks().contains(testTask));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
      clear();
    }
    DriverTask testTask =
        new DriverTask(mockDriver, 100L, DriverTaskStatus.BLOCKED, driverTaskHandle);
    manager.getBlockedTasks().add(testTask);
    Set<DriverTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
    fragmentRelatedTask.put(instanceId, taskSet);
    manager.getQueryMap().put(queryId, fragmentRelatedTask);
    manager.getTimeoutQueue().push(testTask);
    defaultScheduler.blockedToReady(testTask);
    Assert.assertEquals(DriverTaskStatus.READY, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());
    clear();
  }

  @Test
  public void testReadyToRunning() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    IDriver mockDriver = Mockito.mock(IDriver.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));

    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    DriverTaskStatus[] invalidStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.FINISHED,
          DriverTaskStatus.ABORTED,
          DriverTaskStatus.BLOCKED,
          DriverTaskStatus.RUNNING,
        };
    for (DriverTaskStatus status : invalidStates) {
      DriverTask testTask = new DriverTask(mockDriver, 100L, status, driverTaskHandle);
      Set<DriverTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId, taskSet);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.readyToRunning(testTask);
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
      clear();
    }
    DriverTask testTask =
        new DriverTask(mockDriver, 100L, DriverTaskStatus.READY, driverTaskHandle);
    Set<DriverTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
    fragmentRelatedTask.put(instanceId, taskSet);
    manager.getQueryMap().put(queryId, fragmentRelatedTask);
    manager.getTimeoutQueue().push(testTask);
    defaultScheduler.readyToRunning(testTask);
    Assert.assertEquals(DriverTaskStatus.RUNNING, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());
    clear();
  }

  @Test
  public void testRunningToReady() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    IDriver mockDriver = Mockito.mock(IDriver.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    DriverTaskStatus[] invalidStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.FINISHED,
          DriverTaskStatus.ABORTED,
          DriverTaskStatus.BLOCKED,
          DriverTaskStatus.READY,
        };
    for (DriverTaskStatus status : invalidStates) {
      DriverTask testTask = new DriverTask(mockDriver, 100L, status, driverTaskHandle);
      Set<DriverTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId, taskSet);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.runningToReady(testTask, new ExecutionContext());
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
      clear();
    }
    DriverTask testTask =
        new DriverTask(mockDriver, 100L, DriverTaskStatus.RUNNING, driverTaskHandle);
    Set<DriverTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
    fragmentRelatedTask.put(instanceId, taskSet);
    manager.getQueryMap().put(queryId, fragmentRelatedTask);
    manager.getTimeoutQueue().push(testTask);
    ExecutionContext context = new ExecutionContext();
    context.setTimeSlice(new Duration(1, TimeUnit.SECONDS));
    context.setCpuDuration(new CpuTimer.CpuDuration());
    defaultScheduler.runningToReady(testTask, context);
    // Assert.assertEquals(0.0D, testTask.getSchedulePriority(), 0.00001);
    Assert.assertEquals(DriverTaskStatus.READY, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());
    clear();
  }

  @Test
  public void testRunningToBlocked() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    IDriver mockDriver = Mockito.mock(IDriver.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    DriverTaskStatus[] invalidStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.FINISHED,
          DriverTaskStatus.ABORTED,
          DriverTaskStatus.BLOCKED,
          DriverTaskStatus.READY,
        };
    for (DriverTaskStatus status : invalidStates) {
      DriverTask testTask = new DriverTask(mockDriver, 100L, status, driverTaskHandle);
      Set<DriverTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId, taskSet);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.runningToBlocked(testTask, new ExecutionContext());
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
      clear();
    }
    DriverTask testTask =
        new DriverTask(mockDriver, 100L, DriverTaskStatus.RUNNING, driverTaskHandle);
    Set<DriverTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
    fragmentRelatedTask.put(instanceId, taskSet);
    manager.getQueryMap().put(queryId, fragmentRelatedTask);
    manager.getTimeoutQueue().push(testTask);
    ExecutionContext context = new ExecutionContext();
    context.setTimeSlice(new Duration(1, TimeUnit.SECONDS));
    context.setCpuDuration(new CpuTimer.CpuDuration());
    defaultScheduler.runningToBlocked(testTask, context);
    // Assert.assertEquals(0.0D, testTask.getSchedulePriority(), 0.00001);
    Assert.assertEquals(DriverTaskStatus.BLOCKED, testTask.getStatus());
    Assert.assertTrue(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());
    clear();
  }

  @Test
  public void testRunningToFinished() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    IDriver mockDriver = Mockito.mock(IDriver.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    DriverTaskStatus[] invalidStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.FINISHED,
          DriverTaskStatus.ABORTED,
          DriverTaskStatus.BLOCKED,
          DriverTaskStatus.READY,
        };
    for (DriverTaskStatus status : invalidStates) {
      DriverTask testTask = new DriverTask(mockDriver, 100L, status, driverTaskHandle);
      Set<DriverTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId, taskSet);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.runningToFinished(testTask, new ExecutionContext());
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId).contains(testTask));
      clear();
    }
    DriverTask testTask =
        new DriverTask(mockDriver, 100L, DriverTaskStatus.RUNNING, driverTaskHandle);
    Set<DriverTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
    fragmentRelatedTask.put(instanceId, taskSet);
    manager.getQueryMap().put(queryId, fragmentRelatedTask);
    manager.getTimeoutQueue().push(testTask);
    ExecutionContext context = new ExecutionContext();
    context.setTimeSlice(new Duration(1, TimeUnit.SECONDS));
    context.setCpuDuration(new CpuTimer.CpuDuration());
    defaultScheduler.runningToFinished(testTask, context);
    // Assert.assertEquals(0.0D, testTask.getSchedulePriority(), 0.00001);
    Assert.assertEquals(DriverTaskStatus.FINISHED, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNull(manager.getTimeoutQueue().get(testTask.getDriverTaskId()));
    Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());
    clear();
  }

  @Test
  public void testToAbort() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    IDataNodeRPCService.Client mockMppServiceClient =
        Mockito.mock(IDataNodeRPCService.Client.class);
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            1,
            (MultilevelPriorityQueue) manager.getReadyQueue(),
            OptionalInt.of(Integer.MAX_VALUE));
    ITaskScheduler defaultScheduler = manager.getScheduler();
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId1 =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    DriverTaskId driverTaskId1 = new DriverTaskId(instanceId1, 0);
    IDriver mockDriver1 = Mockito.mock(IDriver.class);
    Mockito.when(mockDriver1.getDriverTaskId()).thenReturn(driverTaskId1);
    IDriver mockDriver2 = Mockito.mock(IDriver.class);
    FragmentInstanceId instanceId2 =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-1");
    DriverTaskId driverTaskId2 = new DriverTaskId(instanceId2, 0);
    Mockito.when(mockDriver2.getDriverTaskId()).thenReturn(driverTaskId2);
    DriverTaskStatus[] invalidStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.FINISHED, DriverTaskStatus.ABORTED,
        };
    for (DriverTaskStatus status : invalidStates) {
      DriverTask testTask1 = new DriverTask(mockDriver1, 100L, status, driverTaskHandle);
      DriverTask testTask2 =
          new DriverTask(mockDriver2, 100L, DriverTaskStatus.BLOCKED, driverTaskHandle);

      Set<DriverTask> taskSet1 = new HashSet<>();
      taskSet1.add(testTask1);
      Set<DriverTask> taskSet2 = new HashSet<>();
      taskSet2.add(testTask2);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId1, taskSet1);
      fragmentRelatedTask.put(instanceId2, taskSet2);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask1);
      manager.getTimeoutQueue().push(testTask2);
      manager.getBlockedTasks().add(testTask2);
      defaultScheduler.toAborted(testTask1);

      Assert.assertEquals(status, testTask1.getStatus());
      Assert.assertEquals(DriverTaskStatus.BLOCKED, testTask2.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask1));
      Assert.assertTrue(manager.getBlockedTasks().contains(testTask2));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask1.getDriverTaskId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask2.getDriverTaskId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId1).contains(testTask1));
      Assert.assertTrue(manager.getQueryMap().get(queryId).get(instanceId2).contains(testTask2));

      Mockito.verify(mockDriver1, Mockito.never()).failed(Mockito.any());
      Mockito.verify(mockDriver2, Mockito.never()).failed(Mockito.any());
      clear();
    }
    DriverTaskStatus[] validStates =
        new DriverTaskStatus[] {
          DriverTaskStatus.RUNNING, DriverTaskStatus.READY, DriverTaskStatus.BLOCKED,
        };
    for (DriverTaskStatus status : validStates) {
      Mockito.reset(mockDriver1);
      Mockito.when(mockDriver1.getDriverTaskId()).thenReturn(driverTaskId1);
      Mockito.reset(mockDriver2);
      Mockito.when(mockDriver2.getDriverTaskId()).thenReturn(driverTaskId2);

      DriverTask testTask1 = new DriverTask(mockDriver1, 100L, status, driverTaskHandle);

      DriverTask testTask2 =
          new DriverTask(mockDriver2, 100L, DriverTaskStatus.BLOCKED, driverTaskHandle);
      Set<DriverTask> taskSet1 = new HashSet<>();
      taskSet1.add(testTask1);
      Set<DriverTask> taskSet2 = new HashSet<>();
      taskSet2.add(testTask2);
      Map<FragmentInstanceId, Set<DriverTask>> fragmentRelatedTask = new ConcurrentHashMap<>();
      fragmentRelatedTask.put(instanceId1, taskSet1);
      fragmentRelatedTask.put(instanceId2, taskSet2);
      manager.getQueryMap().put(queryId, fragmentRelatedTask);
      manager.getTimeoutQueue().push(testTask1);
      defaultScheduler.toAborted(testTask1);

      Mockito.reset(mockMppServiceClient);
      Mockito.verify(mockMPPDataExchangeManager, Mockito.times(2))
          .forceDeregisterFragmentInstance(Mockito.any());
      Mockito.reset(mockMPPDataExchangeManager);

      // An aborted fragment may cause others in the same query aborted.
      Assert.assertEquals(DriverTaskStatus.ABORTED, testTask1.getStatus());
      Assert.assertEquals(DriverTaskStatus.ABORTED, testTask2.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask1));
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask2));
      Assert.assertNull(manager.getTimeoutQueue().get(testTask1.getDriverTaskId()));
      Assert.assertNull(manager.getTimeoutQueue().get(testTask2.getDriverTaskId()));
      Assert.assertFalse(manager.getQueryMap().containsKey(queryId));

      // The mockDriver1.failed() will be called outside the scheduler
      Mockito.verify(mockDriver1, Mockito.never()).failed(Mockito.any());
      Mockito.verify(mockDriver2, Mockito.times(1)).failed(Mockito.any());

      clear();
    }
  }

  private void clear() {
    manager.getQueryMap().clear();
    manager.getBlockedTasks().clear();
    manager.getReadyQueue().clear();
    manager.getTimeoutQueue().clear();
  }
}
