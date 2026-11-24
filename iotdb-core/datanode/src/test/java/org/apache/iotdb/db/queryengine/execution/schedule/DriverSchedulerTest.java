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
package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriver;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.apache.ratis.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.thirdparty.com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.ratis.thirdparty.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

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
  public void testManagingDriver() throws CpuNotEnoughException, MemoryNotEnoughException {
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
    Mockito.when(mockDriver1.getDriverContext()).thenReturn(new DriverContext());
    FragmentInstanceId instanceId2 = new FragmentInstanceId(fragmentId, "inst-1");
    DriverTaskId driverTaskId2 = new DriverTaskId(instanceId2, 0);
    IDriver mockDriver2 = Mockito.mock(IDriver.class);
    Mockito.when(mockDriver2.getDriverTaskId()).thenReturn(driverTaskId2);
    Mockito.when(mockDriver2.getDriverContext()).thenReturn(new DriverContext());
    List<IDriver> instances = Arrays.asList(mockDriver1, mockDriver2);
    manager.submitDrivers(queryId, instances, QUERY_TIMEOUT_MS, null);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(2, manager.getQueryMap().get(queryId).size());
    Assert.assertEquals(1, manager.getTimeoutQueue().size());
    Assert.assertEquals(2, manager.getReadyQueue().size());
    Assert.assertNull(manager.getTimeoutQueue().get(driverTaskId1));
    Assert.assertNotNull(manager.getTimeoutQueue().get(driverTaskId2));
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
    Mockito.when(mockDriver3.getDriverContext()).thenReturn(new DriverContext());
    manager.submitDrivers(queryId, Collections.singletonList(mockDriver3), QUERY_TIMEOUT_MS, null);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(1, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(3, manager.getQueryMap().get(queryId).size());
    Assert.assertEquals(2, manager.getTimeoutQueue().size());
    Assert.assertEquals(3, manager.getReadyQueue().size());
    Assert.assertNotNull(manager.getTimeoutQueue().get(driverTaskId3));
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
    Mockito.when(mockDriver4.getDriverContext()).thenReturn(new DriverContext());
    manager.submitDrivers(queryId2, Collections.singletonList(mockDriver4), QUERY_TIMEOUT_MS, null);
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(2, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId2));
    Assert.assertEquals(1, manager.getQueryMap().get(queryId2).size());
    Assert.assertEquals(3, manager.getTimeoutQueue().size());
    Assert.assertEquals(4, manager.getReadyQueue().size());
    DriverTask task4 = manager.getTimeoutQueue().get(driverTaskId4);
    Assert.assertNotNull(task4);
    Assert.assertTrue(manager.getQueryMap().get(queryId2).get(instanceId4).contains(task4));
    Assert.assertEquals(DriverTaskStatus.READY, task4.getStatus());

    // Abort one FragmentInstance
    Mockito.reset(mockDriver1);
    Mockito.when(mockDriver1.getDriverTaskId()).thenReturn(driverTaskId1);
    manager.abortFragmentInstance(instanceId1, null);
    Mockito.verify(mockMPPDataExchangeManager, Mockito.times(1))
        .forceDeregisterFragmentInstance(Mockito.any());
    Assert.assertTrue(manager.getBlockedTasks().isEmpty());
    Assert.assertEquals(2, manager.getQueryMap().size());
    Assert.assertTrue(manager.getQueryMap().containsKey(queryId));
    Assert.assertEquals(3, manager.getTimeoutQueue().size());
    Assert.assertEquals(3, manager.getReadyQueue().size());
    Assert.assertEquals(DriverTaskStatus.ABORTED, task1.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task2.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task3.getStatus());
    Assert.assertEquals(DriverTaskStatus.READY, task4.getStatus());
    Mockito.verify(mockDriver1, Mockito.times(1)).failed(Mockito.any());
    Assert.assertEquals(
        "DriverTask test.0.inst-0.0 is aborted by called",
        task1.getAbortCause().get().getMessage());

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
        "DriverTask test.0.inst-1.0 is aborted by query cascading aborted",
        task2.getAbortCause().get().getMessage());
    Assert.assertEquals(
        "DriverTask test.0.inst-2.0 is aborted by query cascading aborted",
        task3.getAbortCause().get().getMessage());
    Assert.assertFalse(task4.getAbortCause().isPresent());
  }

  @Test
  public void testSetAbortCauseLetsGenuineFailureReplaceFallbackAndPreservesFirstGenuine() {
    DriverTask task = new DriverTask();
    DriverTaskAbortedException fallback1 =
        new DriverTaskAbortedException(
            "task-0", DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED);
    DriverTaskAbortedException fallback2 =
        new DriverTaskAbortedException(
            "task-0", DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED);
    ArithmeticException firstGenuine = new ArithmeticException("long overflow");
    RuntimeException secondGenuine = new RuntimeException("second genuine");

    task.setAbortCause(null);
    Assert.assertFalse(task.getAbortCause().isPresent());

    task.setAbortCause(fallback1);
    task.setAbortCause(fallback2);
    Assert.assertSame(fallback1, task.getAbortCause().get());

    // Genuine failure replaces synthetic scheduler fallback.
    task.setAbortCause(firstGenuine);
    Assert.assertSame(firstGenuine, task.getAbortCause().get());

    // Subsequent synthetic fallbacks, genuine failures, or nulls do not replace the first genuine.
    task.setAbortCause(fallback2);
    task.setAbortCause(secondGenuine);
    task.setAbortCause(null);
    Assert.assertSame(firstGenuine, task.getAbortCause().get());
  }

  @Test
  public void testToAbortedPropagatesOriginalArithmeticExceptionInsteadOfFallback()
      throws Exception {
    verifyArithmeticExceptionPropagatesToLocalResultQueue(true);
    tearDown();
    verifyArithmeticExceptionPropagatesToLocalResultQueue(false);
  }

  private void verifyArithmeticExceptionPropagatesToLocalResultQueue(boolean replaceBeforeToAborted)
      throws Exception {
    QueryId queryId =
        new QueryId("arithmetic_propagation_" + (replaceBeforeToAborted ? "replaced" : "resolved"));
    FragmentInstanceId rootId = new FragmentInstanceId(new PlanFragmentId(queryId, 1), "0");
    FragmentInstanceId leafId = new FragmentInstanceId(new PlanFragmentId(queryId, 2), "0");

    FragmentInstanceStateMachine rootStateMachine =
        new FragmentInstanceStateMachine(rootId, directExecutor());
    FragmentInstanceContext rootContext =
        FragmentInstanceContext.createFragmentInstanceContext(rootId, rootStateMachine);
    FragmentInstanceStateMachine leafStateMachine =
        new FragmentInstanceStateMachine(leafId, directExecutor());
    FragmentInstanceContext leafContext =
        FragmentInstanceContext.createFragmentInstanceContext(leafId, leafStateMachine);

    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = new MemoryManager(1024L * 1024L);
    MemoryPool memoryPool = new MemoryPool("test", memoryManager, 1024L * 1024L);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(memoryPool);
    SharedTsBlockQueue rootQueue =
        new SharedTsBlockQueue(
            rootId.toThrift(), "result_node", mockLocalMemoryManager, newDirectExecutorService());
    LocalSourceHandle resultHandle =
        new LocalSourceHandle(
            new TFragmentInstanceId(queryId.getId(), 0, "0"),
            "result_node",
            rootQueue,
            Mockito.mock(SourceHandleListener.class));
    LocalSinkChannel rootSink =
        new LocalSinkChannel(
            rootId.toThrift(),
            rootQueue,
            new SinkListener() {
              @Override
              public void onFinish(ISink sink) {}

              @Override
              public void onEndOfBlocks(ISink sink) {}

              @Override
              public Optional<Throwable> onAborted(ISink sink) {
                return rootContext.getFailureCause();
              }

              @Override
              public void onFailure(ISink sink, Throwable t) {}
            });
    ListenableFuture<?> resultBlocked = resultHandle.isBlocked();

    IMPPDataExchangeManager mockExchangeManager = Mockito.mock(IMPPDataExchangeManager.class);
    Mockito.doAnswer(
            invocation -> {
              TFragmentInstanceId fiId = invocation.getArgument(0);
              if (rootId.toThrift().equals(fiId)) {
                rootSink.abort();
              }
              return null;
            })
        .when(mockExchangeManager)
        .forceDeregisterFragmentInstance(Mockito.any());
    manager.setBlockManager(mockExchangeManager);

    DataDriverContext rootDriverContext = new DataDriverContext(rootContext, 0);
    rootDriverContext.setSink(rootSink);
    IDriver rootDriver = new DataDriver(Mockito.mock(Operator.class), rootDriverContext, 0L);

    DataDriverContext leafDriverContext = new DataDriverContext(leafContext, 0);
    leafDriverContext.setSink(Mockito.mock(ISink.class));
    IDriver leafDriver = new DataDriver(Mockito.mock(Operator.class), leafDriverContext, 0L);

    manager.submitDrivers(queryId, Collections.singletonList(rootDriver), QUERY_TIMEOUT_MS, null);
    manager.submitDrivers(queryId, Collections.singletonList(leafDriver), QUERY_TIMEOUT_MS, null);

    DriverTask leafTask = manager.getQueryMap().get(queryId).get(leafId).iterator().next();

    // Leaf driver records the real ArithmeticException on its FragmentInstanceContext first, then
    // AbstractDriverThread stores a synthetic DriverTaskAbortedException fallback on leafTask.
    ArithmeticException originalFailure = new ArithmeticException("long overflow");
    leafDriver.failed(originalFailure);
    leafTask.setAbortCause(
        new DriverTaskAbortedException(
            leafTask.getDriverTaskId().getFullId(),
            DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED));
    if (replaceBeforeToAborted) {
      // Simulates fragment cleanup supplying the original cause before toAborted propagates it.
      leafTask.setAbortCause(leafContext.getFailureCause().orElse(null));
    }

    manager.getScheduler().toAborted(leafTask);

    Assert.assertEquals(Optional.of(originalFailure), rootContext.getFailureCause());
    try {
      resultBlocked.get(5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException carrying ArithmeticException");
    } catch (ExecutionException e) {
      Assert.assertSame(originalFailure, e.getCause());
    }
  }

  @Test
  public void testAbortFragmentInstanceReplacesSchedulerFallbackOnTask() throws Exception {
    manager.setBlockManager(Mockito.mock(IMPPDataExchangeManager.class));
    QueryId queryId = new QueryId("abort_fi_replaces_fallback");
    FragmentInstanceId leafId = new FragmentInstanceId(new PlanFragmentId(queryId, 2), "0");
    FragmentInstanceStateMachine leafStateMachine =
        new FragmentInstanceStateMachine(leafId, directExecutor());
    FragmentInstanceContext leafContext =
        FragmentInstanceContext.createFragmentInstanceContext(leafId, leafStateMachine);
    DataDriverContext leafDriverContext = new DataDriverContext(leafContext, 0);
    leafDriverContext.setSink(Mockito.mock(ISink.class));
    IDriver leafDriver = new DataDriver(Mockito.mock(Operator.class), leafDriverContext, 0L);

    manager.submitDrivers(queryId, Collections.singletonList(leafDriver), QUERY_TIMEOUT_MS, null);
    DriverTask leafTask = manager.getQueryMap().get(queryId).get(leafId).iterator().next();

    ArithmeticException originalFailure = new ArithmeticException("long overflow");
    leafDriver.failed(originalFailure);
    leafTask.setAbortCause(
        new DriverTaskAbortedException(
            leafTask.getDriverTaskId().getFullId(),
            DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED));

    manager.abortFragmentInstance(leafId, leafContext.getFailureCause().orElse(null));

    Assert.assertEquals(DriverTaskStatus.ABORTED, leafTask.getStatus());
    Assert.assertEquals(Optional.of(originalFailure), leafTask.getAbortCause());
    Assert.assertEquals(1, leafStateMachine.getFailureCauses().size());
    Assert.assertEquals(Optional.of(originalFailure), leafContext.getFailureCause());
  }

  @Test
  public void testToAbortedFallsBackToTaskSyntheticCauseWhenFragmentHasNoFailure()
      throws Exception {
    manager.setBlockManager(Mockito.mock(IMPPDataExchangeManager.class));
    QueryId queryId = new QueryId("to_aborted_synthetic_fallback");
    FragmentInstanceId rootId = new FragmentInstanceId(new PlanFragmentId(queryId, 1), "0");
    FragmentInstanceId leafId = new FragmentInstanceId(new PlanFragmentId(queryId, 2), "0");

    FragmentInstanceStateMachine rootStateMachine =
        new FragmentInstanceStateMachine(rootId, directExecutor());
    FragmentInstanceContext rootContext =
        FragmentInstanceContext.createFragmentInstanceContext(rootId, rootStateMachine);
    FragmentInstanceStateMachine leafStateMachine =
        new FragmentInstanceStateMachine(leafId, directExecutor());
    FragmentInstanceContext leafContext =
        FragmentInstanceContext.createFragmentInstanceContext(leafId, leafStateMachine);

    DataDriverContext rootDriverContext = new DataDriverContext(rootContext, 0);
    rootDriverContext.setSink(Mockito.mock(ISink.class));
    IDriver rootDriver = new DataDriver(Mockito.mock(Operator.class), rootDriverContext, 0L);

    DataDriverContext leafDriverContext = new DataDriverContext(leafContext, 0);
    leafDriverContext.setSink(Mockito.mock(ISink.class));
    IDriver leafDriver = new DataDriver(Mockito.mock(Operator.class), leafDriverContext, 0L);

    manager.submitDrivers(queryId, Collections.singletonList(rootDriver), QUERY_TIMEOUT_MS, null);
    manager.submitDrivers(queryId, Collections.singletonList(leafDriver), QUERY_TIMEOUT_MS, null);

    DriverTask leafTask = manager.getQueryMap().get(queryId).get(leafId).iterator().next();
    DriverTaskAbortedException fallback =
        new DriverTaskAbortedException(
            leafTask.getDriverTaskId().getFullId(),
            DriverTaskAbortedException.BY_ALREADY_BEING_CANCELLED);
    leafTask.setAbortCause(fallback);

    manager.getScheduler().toAborted(leafTask);

    Assert.assertEquals(Optional.of(fallback), rootContext.getFailureCause());
  }

  @Test
  public void testExplicitCancellationWithFourPipelinesRetainsEmptyFailureList() throws Exception {
    int previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
    try {
      manager.setBlockManager(Mockito.mock(IMPPDataExchangeManager.class));
      QueryId queryId = new QueryId("driver_context_cancel_four_pipelines");
      FragmentInstanceId instanceId = new FragmentInstanceId(new PlanFragmentId(queryId, 1), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, directExecutor());
      FragmentInstanceContext context =
          FragmentInstanceContext.createFragmentInstanceContext(instanceId, stateMachine);
      ISink sink = Mockito.mock(ISink.class);
      List<IDriver> drivers = createFourPipelineDrivers(context, sink);
      context.initializeNumOfDrivers(drivers.size());

      FragmentInstanceExecution.createFragmentInstanceExecution(
          manager,
          instanceId,
          context,
          drivers,
          sink,
          stateMachine,
          QUERY_TIMEOUT_MS,
          false,
          Mockito.mock(MPPDataExchangeManager.class));

      context.cancel();

      Assert.assertEquals(FragmentInstanceState.CANCELLED, stateMachine.getState());
      Assert.assertTrue(stateMachine.getFailureCauses().isEmpty());
      Assert.assertTrue(context.getFailureInfoList().isEmpty());
      Assert.assertFalse(context.getFailureCause().isPresent());
      Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
    }
  }

  @Test
  public void testSingleOperatorFailureWithFourPipelinesRecordsSingleFailureEntry()
      throws Exception {
    int previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
    try {
      manager.setBlockManager(Mockito.mock(IMPPDataExchangeManager.class));
      QueryId queryId = new QueryId("driver_context_single_failure_four_pipelines");
      FragmentInstanceId instanceId = new FragmentInstanceId(new PlanFragmentId(queryId, 1), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, directExecutor());
      FragmentInstanceContext context =
          FragmentInstanceContext.createFragmentInstanceContext(instanceId, stateMachine);
      ISink sink = Mockito.mock(ISink.class);
      List<IDriver> drivers = createFourPipelineDrivers(context, sink);
      context.initializeNumOfDrivers(drivers.size());

      FragmentInstanceExecution.createFragmentInstanceExecution(
          manager,
          instanceId,
          context,
          drivers,
          sink,
          stateMachine,
          QUERY_TIMEOUT_MS,
          false,
          Mockito.mock(MPPDataExchangeManager.class));

      ArithmeticException operatorFailure = new ArithmeticException("long overflow");
      drivers.get(0).failed(operatorFailure);

      Assert.assertEquals(FragmentInstanceState.FAILED, stateMachine.getState());
      Assert.assertEquals(1, stateMachine.getFailureCauses().size());
      Assert.assertEquals(1, context.getFailureInfoList().size());
      Assert.assertEquals(Optional.of(operatorFailure), context.getFailureCause());
      Assert.assertFalse(manager.getQueryMap().containsKey(queryId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
    }
  }

  private static List<IDriver> createFourPipelineDrivers(
      FragmentInstanceContext context, ISink sink) {
    List<IDriver> drivers = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      DataDriverContext driverContext = new DataDriverContext(context, i);
      driverContext.setSink(sink);
      Operator operator = Mockito.mock(Operator.class);
      drivers.add(new DataDriver(operator, driverContext, 0L));
    }
    return drivers;
  }
}
