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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskStatus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DriverSchedulerTest {

  private final DriverScheduler manager = DriverScheduler.getInstance();
  private static final long QUERY_TIMEOUT_MS =
      IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold();

  @After
  public void tearDown() {
    manager.getQueryMap().clear();
    manager.getBlockedTasks().clear();
    manager.getReadyQueue().clear();
    manager.getTimeoutQueue().clear();
  }

  @Test
  public void testManagingDriver() {
    IMPPDataExchangeManager mockMPPDataExchangeManager =
        Mockito.mock(IMPPDataExchangeManager.class);
    manager.setBlockManager(mockMPPDataExchangeManager);
    // submit 2 tasks in one query
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId1 = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskId1 = new DriverTaskId(instanceId1, 0);
    IDriver mockDriver1 = Mockito.mock(IDriver.class);
    Mockito.when(mockDriver1.getDriverTaskId()).thenReturn(driverTaskId1);
    Mockito.when(mockDriver1.getDependencyDriverIndex()).thenReturn(-1);
    FragmentInstanceId instanceId2 = new FragmentInstanceId(fragmentId, "inst-1");
    DriverTaskId driverTaskId2 = new DriverTaskId(instanceId2, 0);
    IDriver mockDriver2 = Mockito.mock(IDriver.class);
    Mockito.when(mockDriver2.getDriverTaskId()).thenReturn(driverTaskId2);
    Mockito.when(mockDriver2.getDependencyDriverIndex()).thenReturn(-1);
    List<IDriver> instances = Arrays.asList(mockDriver1, mockDriver2);
    manager.submitDrivers(queryId, instances, QUERY_TIMEOUT_MS);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(2, manager.getQueryMap().get(queryId).size());
    Assert.assertEquals(1, manager.getTimeoutQueue().size());
    Assert.assertEquals(2, manager.getReadyQueue().size());
    Assert.assertNotNull(manager.getTimeoutQueue().get(driverTaskId1));
    Assert.assertNull(manager.getTimeoutQueue().get(driverTaskId2));
    DriverTask task1 =
        (DriverTask) manager.getQueryMap().get(queryId).get(instanceId1).toArray()[0];
    DriverTask task2 =
        (DriverTask) manager.getQueryMap().get(queryId).get(instanceId2).toArray()[0];
    Assert.assertEquals(task1.getDriverTaskId(), driverTaskId1);
    Assert.assertEquals(task2.getDriverTaskId(), driverTaskId2);
    Assert.assertEquals(DriverTaskStatus.READY, task1.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task2.getStatus());

    // Submit another task of the same query
    IDriver mockDriver3 = Mockito.mock(IDriver.class);
    FragmentInstanceId instanceId3 = new FragmentInstanceId(fragmentId, "inst-2");
    DriverTaskId driverTaskId3 = new DriverTaskId(instanceId3, 0);
    Mockito.when(mockDriver3.getDriverTaskId()).thenReturn(driverTaskId3);
    Mockito.when(mockDriver3.getDependencyDriverIndex()).thenReturn(-1);
    manager.submitDrivers(queryId, Collections.singletonList(mockDriver3), QUERY_TIMEOUT_MS);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(3, manager.getQueryMap().get(queryId).size());
    Assert.assertEquals(1, manager.getTimeoutQueue().size());
    Assert.assertEquals(3, manager.getReadyQueue().size());
    Assert.assertNull(manager.getTimeoutQueue().get(driverTaskId3));
    DriverTask task3 =
        (DriverTask) manager.getQueryMap().get(queryId).get(instanceId3).toArray()[0];
    Assert.assertEquals(task3.getDriverTaskId(), driverTaskId3);
    Assert.assertEquals(DriverTaskStatus.READY, task3.getStatus());

    // Submit another task of the different query
    QueryId queryId2 = new QueryId("test2");
    PlanFragmentId fragmentId2 = new PlanFragmentId(queryId2, 0);
    FragmentInstanceId instanceId4 = new FragmentInstanceId(fragmentId2, "inst-0");
    DriverTaskId driverTaskId4 = new DriverTaskId(instanceId4, 0);
    IDriver mockDriver4 = Mockito.mock(IDriver.class);
    Mockito.when(mockDriver4.getDriverTaskId()).thenReturn(driverTaskId4);
    Mockito.when(mockDriver4.getDependencyDriverIndex()).thenReturn(-1);
    manager.submitDrivers(queryId2, Collections.singletonList(mockDriver4), QUERY_TIMEOUT_MS);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(2, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId2));
    Assert.assertEquals(1, manager.getQueryMap().get(queryId2).size());
    Assert.assertEquals(2, manager.getTimeoutQueue().size());
    Assert.assertEquals(4, manager.getReadyQueue().size());
    DriverTask task4 = manager.getTimeoutQueue().get(driverTaskId4);
    Assert.assertNotNull(task4);
    Assert.assertTrue(manager.getQueryMap().get(queryId2).get(instanceId4).contains(task4));
    Assert.assertEquals(DriverTaskStatus.READY, task4.getStatus());

    // Abort one FragmentInstance
    Mockito.reset(mockDriver1);
    Mockito.when(mockDriver1.getDriverTaskId()).thenReturn(driverTaskId1);
    manager.abortFragmentInstance(instanceId1);
    Mockito.verify(mockMPPDataExchangeManager, Mockito.times(1))
        .forceDeregisterFragmentInstance(Mockito.any());
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(2, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(1, manager.getTimeoutQueue().size());
    Assert.assertEquals(3, manager.getReadyQueue().size());
    Assert.assertEquals(DriverTaskStatus.ABORTED, task1.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task2.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task3.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task4.getStatus());
    Mockito.verify(mockDriver1, Mockito.times(1)).failed(Mockito.any());
    Assert.assertEquals(DriverTaskAbortedException.BY_FRAGMENT_ABORT_CALLED, task1.getAbortCause());

    // Abort the whole query
    Mockito.reset(mockMPPDataExchangeManager);
    Mockito.reset(mockDriver1);
    Mockito.when(mockDriver1.getDriverTaskId()).thenReturn(driverTaskId1);
    Mockito.reset(mockDriver2);
    Mockito.when(mockDriver2.getDriverTaskId()).thenReturn(driverTaskId2);
    Mockito.reset(mockDriver3);
    Mockito.when(mockDriver3.getDriverTaskId()).thenReturn(driverTaskId3);
    manager.abortQuery(queryId);
    Mockito.verify(mockMPPDataExchangeManager, Mockito.times(2))
        .forceDeregisterFragmentInstance(Mockito.any());
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(1, manager.getTimeoutQueue().size());
    Assert.assertEquals(1, manager.getReadyQueue().size());
    Assert.assertEquals(DriverTaskStatus.ABORTED, task1.getStatus());
    Assert.assertEquals(DriverTaskStatus.ABORTED, task2.getStatus());
    Assert.assertEquals(DriverTaskStatus.ABORTED, task3.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task4.getStatus());
    Mockito.verify(mockDriver1, Mockito.never()).failed(Mockito.any());
    Mockito.verify(mockDriver2, Mockito.times(1)).failed(Mockito.any());
    Mockito.verify(mockDriver3, Mockito.times(1)).failed(Mockito.any());
    Mockito.verify(mockDriver4, Mockito.never()).failed(Mockito.any());
    Assert.assertEquals(
        DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED, task2.getAbortCause());
    Assert.assertEquals(
        DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED, task3.getAbortCause());
    Assert.assertNull(task4.getAbortCause());
  }
}
