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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FragmentInstanceExecutionTest {

  @Test
  public void testFragmentInstanceExecution() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceExecution execution =
          createFragmentInstanceExecution(0, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext = execution.getFragmentInstanceContext();
      FragmentInstanceStateMachine stateMachine = execution.getStateMachine();

      assertEquals(FragmentInstanceState.RUNNING, execution.getInstanceState());
      FragmentInstanceInfo instanceInfo = execution.getInstanceInfo();
      assertEquals(FragmentInstanceState.RUNNING, instanceInfo.getState());
      assertEquals(fragmentInstanceContext.getEndTime(), instanceInfo.getEndTime());
      assertEquals(fragmentInstanceContext.getFailedCause(), instanceInfo.getMessage());
      assertEquals(fragmentInstanceContext.getFailureInfoList(), instanceInfo.getFailureInfoList());

      assertEquals(fragmentInstanceContext.getStartTime(), execution.getStartTime());
      assertEquals(-1, execution.getTimeoutInMs());
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

  @Test
  public void testTVListOwnerTransfer() throws InterruptedException {
    // Capture System.err to check for warning messages
    PrintStream systemOut = System.out;
    ByteArrayOutputStream logPrint = new ByteArrayOutputStream();
    System.setOut(new PrintStream(logPrint));

    try {
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);

      ExecutorService instanceNotificationExecutor =
          IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
      try {
        // TVList
        TVList tvList = buildTVList();

        // FragmentInstance Context & Execution
        FragmentInstanceExecution execution1 =
            createFragmentInstanceExecution(1, instanceNotificationExecutor);
        FragmentInstanceContext fragmentInstanceContext1 = execution1.getFragmentInstanceContext();
        fragmentInstanceContext1.addTVListToSet(ImmutableMap.of(tvList, 0));
        tvList.getQueryContextSet().add(fragmentInstanceContext1);

        FragmentInstanceExecution execution2 =
            createFragmentInstanceExecution(2, instanceNotificationExecutor);
        FragmentInstanceContext fragmentInstanceContext2 = execution2.getFragmentInstanceContext();
        fragmentInstanceContext2.addTVListToSet(ImmutableMap.of(tvList, 0));
        tvList.getQueryContextSet().add(fragmentInstanceContext2);

        // mock flush's behavior
        fragmentInstanceContext1
            .getMemoryReservationContext()
            .reserveMemoryCumulatively(tvList.calculateRamSize());
        tvList.setOwnerQuery(fragmentInstanceContext1);

        fragmentInstanceContext1.decrementNumOfUnClosedDriver();
        fragmentInstanceContext2.decrementNumOfUnClosedDriver();

        fragmentInstanceContext1.getStateMachine().finished();
        Thread.sleep(100);
        fragmentInstanceContext2.getStateMachine().finished();

        assertTrue(execution1.getInstanceState().isDone());
        assertTrue(execution2.getInstanceState().isDone());
        Thread.sleep(100);
      } catch (CpuNotEnoughException | MemoryNotEnoughException | IllegalArgumentException e) {
        fail(e.getMessage());
      } finally {
        instanceNotificationExecutor.shutdown();
      }
    } finally {
      // Restore original System.out
      System.setErr(systemOut);

      // should not contain warn message: "The memory cost to be released is larger than the memory
      // cost of memory block"
      String capturedOutput = logPrint.toString();
      assertFalse(
          "Should not contain warning message",
          capturedOutput.contains("The memory cost to be released is larger than the memory"));
    }
  }

  private FragmentInstanceExecution createFragmentInstanceExecution(int id, Executor executor)
      throws CpuNotEnoughException {
    IDriverScheduler scheduler = Mockito.mock(IDriverScheduler.class);
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, id), String.valueOf(id));
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, executor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.initializeNumOfDrivers(1);
    fragmentInstanceContext.setMayHaveTmpFile(true);
    fragmentInstanceContext.setDataRegion(dataRegion);
    List<IDriver> drivers = Collections.emptyList();
    ISink sinkHandle = Mockito.mock(ISink.class);
    long timeOut = -1;
    MPPDataExchangeManager exchangeManager = Mockito.mock(MPPDataExchangeManager.class);
    return FragmentInstanceExecution.createFragmentInstanceExecution(
        scheduler,
        instanceId,
        fragmentInstanceContext,
        drivers,
        sinkHandle,
        stateMachine,
        timeOut,
        false,
        exchangeManager);
  }

  private TVList buildTVList() {
    int columns = 200;
    int rows = 1000;
    List<TSDataType> dataTypes = new ArrayList<>();
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      dataTypes.add(TSDataType.INT64);
      values[i] = 1L;
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    for (long t = 1; t < rows; t++) {
      tvList.putAlignedValue(t, values);
    }
    return tvList;
  }
}
