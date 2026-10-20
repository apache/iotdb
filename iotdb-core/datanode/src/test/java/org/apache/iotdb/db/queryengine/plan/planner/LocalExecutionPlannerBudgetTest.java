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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.CollectOperator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalExecutionPlannerBudgetTest {
  private long previousBudget;
  private int previousBlockSize;
  private boolean previousEstimation;
  private int previousDataNodeId;

  @Before
  public void setUp() {
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    previousBudget =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    previousBlockSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    previousEstimation =
        IoTDBDescriptor.getInstance().getMemoryConfig().isEnableQueryMemoryEstimation();
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(1024);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(64);
    IoTDBDescriptor.getInstance().getMemoryConfig().setEnableQueryMemoryEstimation(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(previousBudget);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(previousBlockSize);
    IoTDBDescriptor.getInstance()
        .getMemoryConfig()
        .setEnableQueryMemoryEstimation(previousEstimation);
  }

  @Test
  public void testTablePlanEnablesSharingBeforeMemoryEstimationAndRootFactory() throws Exception {
    LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
    MemoryPool pool =
        MPPDataExchangeService.getInstance()
            .getMPPDataExchangeManager()
            .getLocalMemoryManager()
            .getQueryPool();
    FragmentInstanceContext instance = instance("table_planner_budget", SqlDialect.TABLE);
    PlanNode first = mock(PlanNode.class);
    PlanNode second = mock(PlanNode.class);
    SourceHandle firstHandle = mock(SourceHandle.class);
    SourceHandle secondHandle = mock(SourceHandle.class);
    addInput(first, firstHandle, false, 100, 30);
    addInput(second, secondHandle, true, 80, 50);
    CollectNode plan =
        spy(
            new CollectNode(
                new PlanNodeId("collect"), Arrays.asList(first, second), Collections.emptyList()));
    doReturn(PlanNodeType.TABLE_COLLECT_NODE).when(plan).getType();
    when(first.getChildren()).thenReturn(Collections.emptyList());
    when(second.getChildren()).thenReturn(Collections.emptyList());
    long freeBefore = planner.getFreeMemoryForOperators();
    try {
      List<PipelineDriverFactory> factories =
          planner.plan(plan, new TypeProvider(), instance, new DataNodeQueryContext(1));
      assertEquals(1, factories.size());
      CollectOperator collect = (CollectOperator) factories.get(0).getOperation();
      // Check the actual reservation, not only the formula after planning has completed.
      assertEquals(150 + collect.ramBytesUsed(), freeBefore - planner.getFreeMemoryForOperators());
      assertEquals(1024, instance.getInputMaxBytes());
      assertEquals(0, instance.getOutputMaxBytes());
      assertTrue(collect.isBlocked().isDone());
      verify(firstHandle).setMaxBytesCanReserve(1024);
      verify(secondHandle).setMaxBytesCanReserve(1024);

      String query = instance.getId().getQueryId().getId();
      String fi = instance.getId().getFragmentInstanceId();
      pool.registerPlanNodeIdToQueryMemoryMap(query, fi, "first", MemorySide.INPUT);
      pool.registerPlanNodeIdToQueryMemoryMap(query, fi, "second", MemorySide.INPUT);
      assertTrue(
          pool.reserveWithPriority(query, fi, "first", 1024, 1024, false).isReserveSuccess());
      MemoryPool.MemoryReservationResult pending =
          pool.reserveWithPriority(query, fi, "second", 64, 1024, false);
      assertFalse(pending.getFuture().isDone());
      pool.free(query, fi, "first", 1024);
      assertTrue(pending.getFuture().isDone());
      pool.free(query, fi, "second", 64);
      collect.close();
    } finally {
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(
          instance.getId().getQueryId().getId(), instance.getId().getFragmentInstanceId(), true);
      long reserved = freeBefore - planner.getFreeMemoryForOperators();
      if (reserved > 0) {
        planner.releaseToFreeMemoryForOperators(reserved);
      }
    }
  }

  @Test
  public void testNullSessionRetainsTreeGeneratorAndIndependentInputQuotas() throws Exception {
    FragmentInstanceContext instance = instance("null_session_planner_budget", null);
    PlanNode plan = mock(PlanNode.class);
    when(plan.getType()).thenReturn(PlanNodeType.TREE_COLLECT);
    when(plan.getPlanNodeId()).thenReturn(new PlanNodeId("collect"));
    SourceHandle firstHandle = mock(SourceHandle.class);
    SourceHandle secondHandle = mock(SourceHandle.class);
    Operator first = input(false, 100, 30);
    Operator second = input(true, 80, 50);
    doAnswer(
            invocation -> {
              assertTrue(invocation.getArgument(0) instanceof OperatorTreeGenerator);
              LocalExecutionPlanContext context = invocation.getArgument(1);
              registerInput(context, firstHandle);
              registerInput(context, secondHandle);
              CollectOperator collect =
                  new CollectOperator(
                      context
                          .getDriverContext()
                          .addOperatorContext(0, new PlanNodeId("collect"), "CollectOperator"),
                      new java.util.ArrayList<>(Arrays.asList(first, second)));
              return collect;
            })
        .when(plan)
        .accept(any(), any());
    LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
    long freeBefore = planner.getFreeMemoryForOperators();
    try {
      List<PipelineDriverFactory> factories =
          planner.plan(plan, new TypeProvider(), instance, new DataNodeQueryContext(1));
      assertEquals(1, factories.size());
      CollectOperator collect = (CollectOperator) factories.get(0).getOperation();
      assertEquals(150 + collect.ramBytesUsed(), freeBefore - planner.getFreeMemoryForOperators());
      assertTrue(collect.isBlocked().isDone());
      assertEquals(0, instance.getInputMaxBytes());
      verify(firstHandle).setMaxBytesCanReserve(512);
      verify(secondHandle).setMaxBytesCanReserve(512);
      collect.close();
    } finally {
      long reserved = freeBefore - planner.getFreeMemoryForOperators();
      if (reserved > 0) {
        planner.releaseToFreeMemoryForOperators(reserved);
      }
    }
  }

  private static FragmentInstanceContext instance(String query, SqlDialect dialect) {
    FragmentInstanceContext instance =
        spy(
            FragmentInstanceContext.createFragmentInstanceContext(
                new FragmentInstanceId(new PlanFragmentId(new QueryId(query), 0), "0"),
                mock(FragmentInstanceStateMachine.class),
                mock(MemoryReservationManager.class)));
    SessionInfo session = null;
    if (dialect != null) {
      session = mock(SessionInfo.class);
      when(session.getSqlDialect()).thenReturn(dialect);
    }
    doReturn(session).when(instance).getSessionInfo();
    return instance;
  }

  private static void addInput(
      PlanNode node, SourceHandle handle, boolean ready, long peak, long retained) {
    Operator operator = input(ready, peak, retained);
    doAnswer(
            invocation -> {
              LocalExecutionPlanContext context = invocation.getArgument(1);
              registerInput(context, handle);
              return operator;
            })
        .when(node)
        .accept(any(), any());
  }

  private static void registerInput(LocalExecutionPlanContext context, SourceHandle handle) {
    ExchangeOperator exchange = mock(ExchangeOperator.class);
    when(exchange.getSourceHandle()).thenReturn(handle);
    context.addExchangeOperator(exchange);
    context.addExchangeSumNum(1);
  }

  private static Operator input(boolean ready, long peak, long retained) {
    Operator operator = mock(Operator.class);
    doReturn(ready ? Operator.NOT_BLOCKED : SettableFuture.create()).when(operator).isBlocked();
    when(operator.calculateMaxPeekMemoryWithCounter()).thenReturn(peak);
    when(operator.calculateRetainedSizeAfterCallingNext()).thenReturn(retained);
    return operator;
  }
}
