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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import io.airlift.stats.CounterStat;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FragmentInstanceExecutionTest {

  @Test
  public void testFragmentInstanceExecution() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IDriverScheduler scheduler = Mockito.mock(IDriverScheduler.class);
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      DataRegion dataRegion = Mockito.mock(DataRegion.class);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      fragmentInstanceContext.initializeNumOfDrivers(1);
      fragmentInstanceContext.setMayHaveTmpFile(true);
      fragmentInstanceContext.setDataRegion(dataRegion);
      List<IDriver> drivers = Collections.emptyList();
      ISink sinkHandle = Mockito.mock(ISink.class);
      CounterStat failedInstances = new CounterStat();
      long timeOut = -1;
      MPPDataExchangeManager exchangeManager = Mockito.mock(MPPDataExchangeManager.class);
      FragmentInstanceExecution execution =
          FragmentInstanceExecution.createFragmentInstanceExecution(
              scheduler,
              instanceId,
              fragmentInstanceContext,
              drivers,
              sinkHandle,
              stateMachine,
              failedInstances,
              timeOut,
              exchangeManager);
      assertEquals(FragmentInstanceState.RUNNING, execution.getInstanceState());
      FragmentInstanceInfo instanceInfo = execution.getInstanceInfo();
      assertEquals(FragmentInstanceState.RUNNING, instanceInfo.getState());
      assertEquals(fragmentInstanceContext.getEndTime(), instanceInfo.getEndTime());
      assertEquals(fragmentInstanceContext.getFailedCause(), instanceInfo.getMessage());
      assertEquals(fragmentInstanceContext.getFailureInfoList(), instanceInfo.getFailureInfoList());

      assertEquals(fragmentInstanceContext.getStartTime(), execution.getStartTime());
      assertEquals(timeOut, execution.getTimeoutInMs());
      assertEquals(stateMachine, execution.getStateMachine());

      fragmentInstanceContext.decrementNumOfUnClosedDriver();

      stateMachine.failed(new RuntimeException("Unknown"));

      assertTrue(execution.getInstanceState().isFailed());

      List<FragmentInstanceFailureInfo> failureInfoList =
          execution.getInstanceInfo().getFailureInfoList();

      assertEquals(1, failureInfoList.size());
      assertEquals("Unknown", failureInfoList.get(0).getMessage());
      assertEquals("Unknown", failureInfoList.get(0).toException().getMessage());

    } catch (CpuNotEnoughException | MemoryNotEnoughException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
