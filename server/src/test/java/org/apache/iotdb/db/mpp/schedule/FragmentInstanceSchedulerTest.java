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
import org.apache.iotdb.db.mpp.execution.ExecFragmentInstance;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskID;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FragmentInstanceSchedulerTest {

  private final FragmentInstanceScheduler manager = FragmentInstanceScheduler.getInstance();

  @After
  public void tearDown() {
    manager.getQueryMap().clear();
    manager.getBlockedTasks().clear();
    manager.getReadyQueue().clear();
    manager.getTimeoutQueue().clear();
  }

  @Test
  public void testManagingFragmentInstance() {
    IDataBlockManager mockDataBlockManager = Mockito.mock(IDataBlockManager.class);
    manager.setBlockManager(mockDataBlockManager);
    // submit 2 tasks in one query
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId1 = new FragmentInstanceId(fragmentId, "inst-0");
    ExecFragmentInstance mockExecFragmentInstance1 = Mockito.mock(ExecFragmentInstance.class);
    Mockito.when(mockExecFragmentInstance1.getInfo()).thenReturn(instanceId1);
    FragmentInstanceId instanceId2 = new FragmentInstanceId(fragmentId, "inst-1");
    ExecFragmentInstance mockExecFragmentInstance2 = Mockito.mock(ExecFragmentInstance.class);
    Mockito.when(mockExecFragmentInstance2.getInfo()).thenReturn(instanceId2);
    List<ExecFragmentInstance> instances =
        Arrays.asList(mockExecFragmentInstance1, mockExecFragmentInstance2);
    manager.submitFragmentInstances(queryId, instances);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(2, manager.getQueryMap().get(queryId).size());
    Assert.assertEquals(2, manager.getTimeoutQueue().size());
    Assert.assertEquals(2, manager.getReadyQueue().size());
    FragmentInstanceTask task1 =
        manager.getTimeoutQueue().get(new FragmentInstanceTaskID(instanceId1));
    Assert.assertNotNull(task1);
    FragmentInstanceTask task2 =
        manager.getTimeoutQueue().get(new FragmentInstanceTaskID(instanceId2));
    Assert.assertNotNull(task2);
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(task1));
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(task2));
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, task1.getStatus());
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, task2.getStatus());

    // Submit another task of the same query
    ExecFragmentInstance mockExecFragmentInstance3 = Mockito.mock(ExecFragmentInstance.class);
    FragmentInstanceId instanceId3 = new FragmentInstanceId(fragmentId, "inst-2");
    Mockito.when(mockExecFragmentInstance3.getInfo()).thenReturn(instanceId3);
    manager.submitFragmentInstances(queryId, Collections.singletonList(mockExecFragmentInstance3));
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(3, manager.getQueryMap().get(queryId).size());
    Assert.assertEquals(3, manager.getTimeoutQueue().size());
    Assert.assertEquals(3, manager.getReadyQueue().size());
    FragmentInstanceTask task3 =
        manager.getTimeoutQueue().get(new FragmentInstanceTaskID(instanceId3));
    Assert.assertNotNull(task3);
    Assert.assertTrue(manager.getQueryMap().get(queryId).contains(task3));
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, task3.getStatus());

    // Submit another task of the different query
    QueryId queryId2 = new QueryId("test2");
    PlanFragmentId fragmentId2 = new PlanFragmentId(queryId2, 0);
    FragmentInstanceId instanceId4 = new FragmentInstanceId(fragmentId2, "inst-0");
    ExecFragmentInstance mockExecFragmentInstance4 = Mockito.mock(ExecFragmentInstance.class);
    Mockito.when(mockExecFragmentInstance4.getInfo()).thenReturn(instanceId4);
    manager.submitFragmentInstances(queryId2, Collections.singletonList(mockExecFragmentInstance4));
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(2, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId2));
    Assert.assertEquals(1, manager.getQueryMap().get(queryId2).size());
    Assert.assertEquals(4, manager.getTimeoutQueue().size());
    Assert.assertEquals(4, manager.getReadyQueue().size());
    FragmentInstanceTask task4 =
        manager.getTimeoutQueue().get(new FragmentInstanceTaskID(instanceId4));
    Assert.assertNotNull(task4);
    Assert.assertTrue(manager.getQueryMap().get(queryId2).contains(task4));
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, task4.getStatus());

    // Abort the query
    manager.abortQuery(queryId);
    Mockito.verify(mockDataBlockManager, Mockito.times(3))
        .forceDeregisterFragmentInstance(Mockito.any());
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(1, manager.getTimeoutQueue().size());
    Assert.assertEquals(1, manager.getReadyQueue().size());
    Assert.assertEquals(FragmentInstanceTaskStatus.ABORTED, task1.getStatus());
    Assert.assertEquals(FragmentInstanceTaskStatus.ABORTED, task2.getStatus());
    Assert.assertEquals(FragmentInstanceTaskStatus.ABORTED, task3.getStatus());
    Assert.assertEquals(FragmentInstanceTaskStatus.READY, task4.getStatus());
  }
}
