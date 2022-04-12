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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.db.mpp.buffer.IDataBlockManager;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.Driver;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;
import org.apache.iotdb.db.utils.stats.CpuTimer;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;

import io.airlift.units.Duration;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DefaultTaskSchedulerTest {

  private final FragmentInstanceScheduler manager = FragmentInstanceScheduler.getInstance();

  @After
  public void tearDown() {
    clear();
  }

  @Test
  public void testBlockedToReady() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    FragmentInstanceTaskStatus[] invalidStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.FINISHED,
          FragmentInstanceTaskStatus.ABORTED,
          FragmentInstanceTaskStatus.READY,
          FragmentInstanceTaskStatus.RUNNING,
        };
    for (FragmentInstanceTaskStatus status : invalidStates) {
      FragmentInstanceTask testTask = new FragmentInstanceTask(mockDriver, 100L, status);
      manager.getBlockedTasks().add(testTask);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.blockedToReady(testTask);
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertTrue(manager.getBlockedTasks().contains(testTask));
      Assert.assertNull(manager.getReadyQueue().get(testTask.getId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
      clear();
    }
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.BLOCKED);
    manager.getBlockedTasks().add(testTask);
    Set<FragmentInstanceTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    manager.getQueryMap().put(queryId, taskSet);
    manager.getTimeoutQueue().push(testTask);
    defaultScheduler.blockedToReady(testTask);
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getReadyQueue().get(testTask.getId()));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
    clear();
  }

  @Test
  public void testReadyToRunning() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    Driver mockDriver = Mockito.mock(Driver.class);

    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    FragmentInstanceTaskStatus[] invalidStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.FINISHED,
          FragmentInstanceTaskStatus.ABORTED,
          FragmentInstanceTaskStatus.BLOCKED,
          FragmentInstanceTaskStatus.RUNNING,
        };
    for (FragmentInstanceTaskStatus status : invalidStates) {
      FragmentInstanceTask testTask = new FragmentInstanceTask(mockDriver, 100L, status);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.readyToRunning(testTask);
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
      clear();
    }
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.READY);
    Set<FragmentInstanceTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    manager.getQueryMap().put(queryId, taskSet);
    manager.getTimeoutQueue().push(testTask);
    defaultScheduler.readyToRunning(testTask);
    Assert.assertEquals(FragmentInstanceTaskStatus.RUNNING, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
    clear();
  }

  @Test
  public void testRunningToReady() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    FragmentInstanceTaskStatus[] invalidStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.FINISHED,
          FragmentInstanceTaskStatus.ABORTED,
          FragmentInstanceTaskStatus.BLOCKED,
          FragmentInstanceTaskStatus.READY,
        };
    for (FragmentInstanceTaskStatus status : invalidStates) {
      FragmentInstanceTask testTask = new FragmentInstanceTask(mockDriver, 100L, status);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.runningToReady(testTask, new ExecutionContext());
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNull(manager.getReadyQueue().get(testTask.getId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
      clear();
    }
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.RUNNING);
    Set<FragmentInstanceTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    manager.getQueryMap().put(queryId, taskSet);
    manager.getTimeoutQueue().push(testTask);
    ExecutionContext context = new ExecutionContext();
    context.setTimeSlice(new Duration(1, TimeUnit.SECONDS));
    context.setCpuDuration(new CpuTimer.CpuDuration());
    defaultScheduler.runningToReady(testTask, context);
    Assert.assertEquals(0.0D, testTask.getSchedulePriority(), 0.00001);
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNotNull(manager.getReadyQueue().get(testTask.getId()));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
    clear();
  }

  @Test
  public void testRunningToBlocked() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    FragmentInstanceTaskStatus[] invalidStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.FINISHED,
          FragmentInstanceTaskStatus.ABORTED,
          FragmentInstanceTaskStatus.BLOCKED,
          FragmentInstanceTaskStatus.READY,
        };
    for (FragmentInstanceTaskStatus status : invalidStates) {
      FragmentInstanceTask testTask = new FragmentInstanceTask(mockDriver, 100L, status);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.runningToBlocked(testTask, new ExecutionContext());
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNull(manager.getReadyQueue().get(testTask.getId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
      clear();
    }
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.RUNNING);
    Set<FragmentInstanceTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    manager.getQueryMap().put(queryId, taskSet);
    manager.getTimeoutQueue().push(testTask);
    ExecutionContext context = new ExecutionContext();
    context.setTimeSlice(new Duration(1, TimeUnit.SECONDS));
    context.setCpuDuration(new CpuTimer.CpuDuration());
    defaultScheduler.runningToBlocked(testTask, context);
    Assert.assertEquals(0.0D, testTask.getSchedulePriority(), 0.00001);
    Assert.assertEquals(FragmentInstanceTaskStatus.BLOCKED, testTask.getStatus());
    Assert.assertTrue(manager.getBlockedTasks().contains(testTask));
    Assert.assertNull(manager.getReadyQueue().get(testTask.getId()));
    Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
    clear();
  }

  @Test
  public void testRunningToFinished() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    FragmentInstanceTaskStatus[] invalidStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.FINISHED,
          FragmentInstanceTaskStatus.ABORTED,
          FragmentInstanceTaskStatus.BLOCKED,
          FragmentInstanceTaskStatus.READY,
        };
    for (FragmentInstanceTaskStatus status : invalidStates) {
      FragmentInstanceTask testTask = new FragmentInstanceTask(mockDriver, 100L, status);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask);
      defaultScheduler.runningToFinished(testTask, new ExecutionContext());
      Assert.assertEquals(status, testTask.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
      Assert.assertNull(manager.getReadyQueue().get(testTask.getId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask.getId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask));
      clear();
    }
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.RUNNING);
    Set<FragmentInstanceTask> taskSet = new HashSet<>();
    taskSet.add(testTask);
    manager.getQueryMap().put(queryId, taskSet);
    manager.getTimeoutQueue().push(testTask);
    ExecutionContext context = new ExecutionContext();
    context.setTimeSlice(new Duration(1, TimeUnit.SECONDS));
    context.setCpuDuration(new CpuTimer.CpuDuration());
    defaultScheduler.runningToFinished(testTask, context);
    Assert.assertEquals(0.0D, testTask.getSchedulePriority(), 0.00001);
    Assert.assertEquals(FragmentInstanceTaskStatus.FINISHED, testTask.getStatus());
    Assert.assertFalse(manager.getBlockedTasks().contains(testTask));
    Assert.assertNull(manager.getReadyQueue().get(testTask.getId()));
    Assert.assertNull(manager.getTimeoutQueue().get(testTask.getId()));
    Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
    clear();
  }

  @Test
  public void testToAbort() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    InternalService.Client mockMppServiceClient = Mockito.mock(InternalService.Client.class);
    manager.setMppServiceClient(mockMppServiceClient);
    ITaskScheduler defaultScheduler = manager.getScheduler();
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId1 =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-0");
    Driver mockDriver1 = Mockito.mock(Driver.class);
    Mockito.when(mockDriver1.getInfo()).thenReturn(instanceId1);
    Driver mockDriver2 = Mockito.mock(Driver.class);
    FragmentInstanceId instanceId2 =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "inst-1");
    Mockito.when(mockDriver2.getInfo()).thenReturn(instanceId2);
    FragmentInstanceTaskStatus[] invalidStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.FINISHED, FragmentInstanceTaskStatus.ABORTED,
        };
    for (FragmentInstanceTaskStatus status : invalidStates) {
      FragmentInstanceTask testTask1 = new FragmentInstanceTask(mockDriver1, 100L, status);
      FragmentInstanceTask testTask2 =
          new FragmentInstanceTask(mockDriver2, 100L, FragmentInstanceTaskStatus.BLOCKED);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask1);
      taskSet.add(testTask2);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask1);
      manager.getTimeoutQueue().push(testTask2);
      manager.getBlockedTasks().add(testTask2);
      defaultScheduler.toAborted(testTask1);

      Assert.assertEquals(status, testTask1.getStatus());
      Assert.assertEquals(FragmentInstanceTaskStatus.BLOCKED, testTask2.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask1));
      Assert.assertTrue(manager.getBlockedTasks().contains(testTask2));
      Assert.assertNull(manager.getReadyQueue().get(testTask1.getId()));
      Assert.assertNull(manager.getReadyQueue().get(testTask2.getId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask1.getId()));
      Assert.assertNotNull(manager.getTimeoutQueue().get(testTask2.getId()));
      Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask1));
      Assert.assertTrue(manager.getQueryMap().get(queryId).contains(testTask2));
      clear();
    }
    FragmentInstanceTaskStatus[] validStates =
        new FragmentInstanceTaskStatus[] {
          FragmentInstanceTaskStatus.RUNNING,
          FragmentInstanceTaskStatus.READY,
          FragmentInstanceTaskStatus.BLOCKED,
        };
    for (FragmentInstanceTaskStatus status : validStates) {
      FragmentInstanceTask testTask1 = new FragmentInstanceTask(mockDriver1, 100L, status);

      FragmentInstanceTask testTask2 =
          new FragmentInstanceTask(mockDriver2, 100L, FragmentInstanceTaskStatus.BLOCKED);
      Set<FragmentInstanceTask> taskSet = new HashSet<>();
      taskSet.add(testTask1);
      taskSet.add(testTask2);
      manager.getQueryMap().put(queryId, taskSet);
      manager.getTimeoutQueue().push(testTask1);
      defaultScheduler.toAborted(testTask1);

      try {
        Mockito.verify(mockMppServiceClient, Mockito.times(1)).cancelQuery(Mockito.any());
      } catch (TException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Mockito.reset(mockMppServiceClient);
      Mockito.verify(mockDataBlockManager, Mockito.times(2))
          .forceDeregisterFragmentInstance(Mockito.any());
      Mockito.reset(mockDataBlockManager);

      // An aborted fragment may cause others in the same query aborted.
      Assert.assertEquals(FragmentInstanceTaskStatus.ABORTED, testTask1.getStatus());
      Assert.assertEquals(FragmentInstanceTaskStatus.ABORTED, testTask2.getStatus());
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask1));
      Assert.assertFalse(manager.getBlockedTasks().contains(testTask2));
      Assert.assertNull(manager.getReadyQueue().get(testTask1.getId()));
      Assert.assertNull(manager.getReadyQueue().get(testTask2.getId()));
      Assert.assertNull(manager.getTimeoutQueue().get(testTask1.getId()));
      Assert.assertNull(manager.getTimeoutQueue().get(testTask2.getId()));
      Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
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
