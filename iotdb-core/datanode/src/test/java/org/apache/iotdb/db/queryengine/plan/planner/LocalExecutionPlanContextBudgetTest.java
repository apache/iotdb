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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalExecutionPlanContextBudgetTest {
  private long previousBudget;
  private int previousBlockSize;
  private MemoryPool pool;
  private LocalExecutionPlanContext context;

  @Before
  public void setUp() {
    previousBudget =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    previousBlockSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(1024);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(64);
    pool = new MemoryPool("budget-test", new MemoryManager(4096), 1024);
    context = newContext();
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(previousBudget);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(previousBlockSize);
  }

  private LocalExecutionPlanContext newContext() {
    FragmentInstanceContext instance =
        FragmentInstanceContext.createFragmentInstanceContext(
            new FragmentInstanceId(new PlanFragmentId(new QueryId("budget"), 0), "0"),
            mock(FragmentInstanceStateMachine.class),
            mock(MemoryReservationManager.class));
    LocalExecutionPlanContext result =
        new LocalExecutionPlanContext(new TypeProvider(), instance, (DataNodeQueryContext) null);
    result.setExchangeSumNum(4);
    return result;
  }

  private static PlanNode node(PlanNodeType type, PlanNode... children) {
    Class<? extends PlanNode> nodeClass;
    switch (type) {
      case IDENTITY_SINK:
        nodeClass =
            org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode.class;
        break;
      case TABLE_EXCHANGE_NODE:
        nodeClass = org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode.class;
        break;
      case EXCHANGE:
        nodeClass =
            org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode.class;
        break;
      case TABLE_COLLECT_NODE:
        nodeClass = CollectNode.class;
        break;
      case TABLE_TOPK_NODE:
        nodeClass = TopKNode.class;
        break;
      case TREE_COLLECT:
        nodeClass =
            org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.CollectNode.class;
        break;
      default:
        nodeClass = PlanNode.class;
        break;
    }
    PlanNode node = mock(nodeClass);
    when(node.getType()).thenReturn(type);
    when(node.getChildren()).thenReturn(Arrays.asList(children));
    return node;
  }

  private static PlanNode collect() {
    return new CollectNode(
        new PlanNodeId("collect"),
        Arrays.asList(
            node(PlanNodeType.TABLE_EXCHANGE_NODE), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
        Collections.emptyList());
  }

  private CollectOperator generateTableCollect(PlanNode plan) {
    Operator first = mock(Operator.class);
    Operator second = mock(Operator.class);
    when(first.isBlocked()).thenReturn(SettableFuture.create());
    doReturn(Operator.NOT_BLOCKED).when(second).isBlocked();
    when(first.calculateMaxPeekMemoryWithCounter()).thenReturn(100L);
    when(first.calculateRetainedSizeAfterCallingNext()).thenReturn(30L);
    when(second.calculateMaxPeekMemoryWithCounter()).thenReturn(80L);
    when(second.calculateRetainedSizeAfterCallingNext()).thenReturn(50L);
    doReturn(first).when(plan.getChildren().get(0)).accept(any(), same(context));
    doReturn(second).when(plan.getChildren().get(1)).accept(any(), same(context));
    return (CollectOperator)
        plan.accept(new DataNodeTableOperatorGenerator(mock(Metadata.class)), context);
  }

  private ISourceHandle addSource(LocalExecutionPlanContext target) {
    ISourceHandle handle = mock(SourceHandle.class);
    ExchangeOperator operator = mock(ExchangeOperator.class);
    when(operator.getSourceHandle()).thenReturn(handle);
    target.addExchangeOperator(operator);
    return handle;
  }

  private void assertAdmissionIsShared(boolean shared) {
    assertAdmissionIsShared(shared, MemorySide.INPUT);
  }

  private void assertAdmissionIsShared(boolean shared, MemorySide side) {
    assertAdmissionIsShared(shared, side, shared ? 1024 : 256);
  }

  private void assertAdmissionIsShared(boolean shared, MemorySide side, long limit) {
    FragmentInstanceContext instance = context.getDriverContext().getFragmentInstanceContext();
    if (shared) {
      assertEquals(
          limit,
          side == MemorySide.INPUT ? instance.getInputMaxBytes() : instance.getOutputMaxBytes());
    } else {
      assertEquals(0, instance.getInputMaxBytes());
      assertEquals(0, instance.getOutputMaxBytes());
    }
    String fi = context.getFragmentInstanceId().getFragmentInstanceId();
    pool.registerPlanNodeIdToQueryMemoryMap("budget", fi, "a" + side, side);
    pool.registerPlanNodeIdToQueryMemoryMap("budget", fi, "b" + side, side);
    assertTrue(
        pool.reserveWithPriority("budget", fi, "a" + side, limit, limit, false).isReserveSuccess());
    MemoryPool.MemoryReservationResult second =
        pool.reserveWithPriority("budget", fi, "b" + side, limit, limit, false);
    if (shared) {
      assertFalse(second.getFuture().isDone());
      pool.free("budget", fi, "a" + side, limit);
      assertTrue(second.getFuture().isDone());
    } else {
      assertTrue(second.isReserveSuccess());
      pool.free("budget", fi, "a" + side, limit);
    }
    pool.free("budget", fi, "b" + side, limit);
  }

  @Test
  public void testRealSourceHandlesShareCanonicalFragmentAccountAndCleanup() throws Exception {
    FragmentInstanceId id = context.getFragmentInstanceId();
    String fi = FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id.toThrift());
    LocalMemoryManager manager = mock(LocalMemoryManager.class);
    when(manager.getQueryPool()).thenReturn(pool);
    SourceHandle first = realSourceHandle(id, "first", manager);
    SourceHandle second = realSourceHandle(id, "second", manager);
    for (SourceHandle source : Arrays.asList(first, second)) {
      ExchangeOperator exchange = mock(ExchangeOperator.class);
      when(exchange.getSourceHandle()).thenReturn(source);
      context.addExchangeOperator(exchange);
    }
    try {
      context.setExchangeMemoryLimits(collect(), pool);
      assertEquals(1024, context.getInstanceContext().getInputMaxBytes());
      first.isBlocked();
      second.isBlocked();
      first.updatePendingDataBlockInfo(0, List.of(1024L));
      second.updatePendingDataBlockInfo(0, List.of(1024L));
      assertEquals(1024, pool.getFragmentMemoryReservedBytes("budget", fi, MemorySide.INPUT));
      assertEquals(0, pool.getPlanNodeMemoryReservedBytes("budget", fi, "second"));
      assertEquals(1, pool.getMemoryReservationSize());
      first.abort();
      assertEquals(1024, pool.getPlanNodeMemoryReservedBytes("budget", fi, "second"));
    } finally {
      first.abort();
      second.abort();
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("budget", fi, true);
    }
    assertEquals(0, pool.getMemoryReservationSize());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  private static SourceHandle realSourceHandle(
      FragmentInstanceId id, String planNode, LocalMemoryManager manager) {
    return new SourceHandle(
        new TEndPoint("remote", 12345),
        new TFragmentInstanceId(id.getQueryId().getId(), 1, "0"),
        id.toThrift(),
        planNode,
        0,
        manager,
        mock(ExecutorService.class),
        new TsBlockSerde(),
        mock(SourceHandleListener.class),
        mock(IClientManager.class));
  }

  @Test
  public void testPureCollectUsesSharedSideBudgets() {
    ISourceHandle source = addSource(context);
    ISourceHandle secondSource = addSource(context);
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(source).setMaxBytesCanReserve(512);
    verify(secondSource).setMaxBytesCanReserve(512);
    verify(sink).setMaxBytesCanReserve(512);
    assertAdmissionIsShared(true, MemorySide.INPUT, 512);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 512);
  }

  @Test
  public void testOddBudgetIsInitializedAfterBothDirectionsAreKnown() {
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(1025);
    pool = new MemoryPool("odd-budget", new MemoryManager(4096), 1025);
    context = newContext();
    ISourceHandle source = addSource(context);
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(source).setMaxBytesCanReserve(512);
    verify(sink).setMaxBytesCanReserve(513);
    assertAdmissionIsShared(true, MemorySide.INPUT, 512);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 513);
  }

  @Test
  public void testInputOnlyCollectUsesFullBudgetEvenForOneFullBlock() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    context.setExchangeSumNum(1);
    ISourceHandle source = addSource(context);
    context.setExchangeMemoryLimits(
        node(PlanNodeType.TABLE_COLLECT_NODE, node(PlanNodeType.TABLE_EXCHANGE_NODE)), pool);
    verify(source).setMaxBytesCanReserve(1024);
    assertEquals(0, context.getDriverContext().getFragmentInstanceContext().getOutputMaxBytes());
    assertAdmissionIsShared(true);
  }

  @Test
  public void testOutputOnlyCollectUsesFullBudgetEvenForOneFullBlock() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    context.setExchangeSumNum(1);
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setISink(sink);
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.IDENTITY_SINK,
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.SERIES_SCAN),
                node(PlanNodeType.SERIES_SCAN))),
        pool);
    verify(sink).setMaxBytesCanReserve(1024);
    assertEquals(0, context.getDriverContext().getFragmentInstanceContext().getInputMaxBytes());
    assertAdmissionIsShared(true, MemorySide.OUTPUT);
  }

  private void addLocalInput() {
    ExchangeOperator exchange = mock(ExchangeOperator.class);
    when(exchange.getSourceHandle()).thenReturn(mock(LocalSourceHandle.class));
    context.addExchangeOperator(exchange);
  }

  @Test
  public void testLocalInputsDoNotSplitOutputBudget() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    addLocalInput();
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(sink).setMaxBytesCanReserve(1024);
    assertEquals(0, context.getDriverContext().getFragmentInstanceContext().getInputMaxBytes());
    assertAdmissionIsShared(true, MemorySide.OUTPUT);
  }

  @Test
  public void testMixedLocalAndRemoteInputsStillSplitBidirectionalBudget() {
    addLocalInput();
    ISourceHandle source = addSource(context);
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(source).setMaxBytesCanReserve(512);
    verify(sink).setMaxBytesCanReserve(512);
    assertAdmissionIsShared(true, MemorySide.INPUT, 512);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 512);
  }

  @Test
  public void testLocalInputsWithoutSinkDoNotRegisterUnusedSharedBudget() {
    addLocalInput();
    context.setExchangeMemoryLimits(collect(), pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testMappingCollectUsesSharedBudget() {
    addSource(context);
    PlanNode union =
        new UnionNode(
            new PlanNodeId("union"),
            Arrays.asList(
                node(PlanNodeType.TABLE_EXCHANGE_NODE), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
            ArrayListMultimap.create(),
            Collections.emptyList());
    context.setExchangeMemoryLimits(union, pool);
    assertAdmissionIsShared(true);
  }

  @Test
  public void testTreePlanningRetainsLegacySourceQuota() {
    ISourceHandle source = addSource(context);
    context.setMaxBytesOneHandleCanReserve();
    verify(source).setMaxBytesCanReserve(256);
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TREE_COLLECT,
                node(PlanNodeType.EXCHANGE),
                node(PlanNodeType.EXCHANGE))));
    assertAdmissionIsShared(false);
  }

  @Test
  public void testEligibleTableCollectEnablesReadyConsumptionAndSharedBudget() {
    PlanNode plan = collect();
    CollectOperator collect = generateTableCollect(plan);
    addSource(context);
    context.setExchangeMemoryLimits(plan, pool);
    assertTrue(collect.isBlocked().isDone());
    assertEquals(80, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(150, collect.calculateMaxPeekMemory());
    assertAdmissionIsShared(true);
  }

  @Test
  public void testEligibleTableUnionEnablesReadyConsumptionAndSharedBudget() {
    PlanNode union =
        new UnionNode(
            new PlanNodeId("union"),
            Arrays.asList(
                node(PlanNodeType.TABLE_EXCHANGE_NODE), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
            ArrayListMultimap.create(),
            Collections.emptyList());
    CollectOperator collect = generateTableCollect(union);
    addSource(context);
    context.setExchangeMemoryLimits(union, pool);
    assertTrue(collect.isBlocked().isDone());
    assertEquals(80, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(150, collect.calculateMaxPeekMemory());
    assertAdmissionIsShared(true);
  }

  @Test
  public void testSingleChildTopKRetainsLegacyBudgetAndConsumption() {
    PlanNode child = collect();
    CollectOperator collect = generateTableCollect(child);
    ISourceHandle source = addSource(context);
    PlanNode topK = node(PlanNodeType.TABLE_TOPK_NODE, child);
    context.setExchangeMemoryLimits(topK, pool);
    verify(source).setMaxBytesCanReserve(256);
    assertTrue(collect.isBlocked().isDone());
    assertAdmissionIsShared(false);
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.TABLE_COLLECT_NODE, topK, collect())));
    assertTrue(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE, topK),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))));
  }

  @Test
  public void testMultiplePipelinesKeepLegacyBudgetWithReadyCollectConsumption() {
    PlanNode plan = collect();
    CollectOperator collect = generateTableCollect(plan);
    LocalExecutionPlanContext child = context.createSubContext();
    child.addPipelineDriverFactory(mock(Operator.class), child.getDriverContext(), 0);
    addSource(context);
    context.setExchangeMemoryLimits(plan, pool);
    assertTrue(collect.isBlocked().isDone());
    assertAdmissionIsShared(false);
  }

  @Test
  public void testNestedDependencyDisablesSharingForEntireFragment() {
    for (PlanNodeType dependency :
        new PlanNodeType[] {
          PlanNodeType.TABLE_MERGESORT_NODE, PlanNodeType.MERGE_SORT, PlanNodeType.AGG_MERGE_SORT,
          PlanNodeType.TABLE_JOIN_NODE, PlanNodeType.TABLE_SEMI_JOIN_NODE,
              PlanNodeType.TABLE_TOPK_NODE,
          PlanNodeType.COUNT_MERGE, PlanNodeType.DEVICE_VIEW
        }) {
      PlanNode dependent =
          node(
              dependency,
              node(PlanNodeType.TABLE_EXCHANGE_NODE),
              node(PlanNodeType.TABLE_EXCHANGE_NODE));
      assertFalse(
          SharedExchangeBudgetEligibility.isEligible(
              node(PlanNodeType.TABLE_COLLECT_NODE, dependent, collect())));
      assertFalse(
          SharedExchangeBudgetEligibility.isEligible(node(dependency, collect(), collect())));
    }
    ISourceHandle source = addSource(context);
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setExchangeSumNum(2);
    context.setISink(sink);
    context.setExchangeSumNum(4);
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.TABLE_COLLECT_NODE,
            collect(),
            node(
                PlanNodeType.TABLE_MERGESORT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))),
        pool);
    verify(source).setMaxBytesCanReserve(256);
    // The legacy sink quota is captured at its original point in operator generation.
    verify(sink).setMaxBytesCanReserve(512);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testSequentialAndUnknownMultiInputOperatorsDisableSharing() {
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.TABLE_COLLECT_NODE, collect(), collect())));
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.IDENTITY_SINK, collect(), collect())));
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.SCHEMA_QUERY_MERGE, collect(), collect())));
    for (PlanNodeType multiChildSetOperation :
        new PlanNodeType[] {PlanNodeType.TABLE_INTERSECT_NODE, PlanNodeType.TABLE_EXCEPT_NODE}) {
      assertFalse(
          SharedExchangeBudgetEligibility.isEligible(
              node(
                  multiChildSetOperation,
                  node(PlanNodeType.TABLE_EXCHANGE_NODE),
                  node(PlanNodeType.TABLE_EXCHANGE_NODE))));
    }
  }

  @Test
  public void testExchangeBoundaryDoesNotInspectUpstreamFragment() {
    PlanNode remote = node(PlanNodeType.TABLE_MERGESORT_NODE, collect(), collect());
    assertTrue(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE, remote),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))));
  }

  @Test
  public void testSourceFragmentRootedAtExchangeKeepsLegacyBudget() {
    // The source fragment's root is an ExchangeNode; its upstream child belongs to another
    // fragment and must not make this fragment eligible for a shared budget.
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TABLE_EXCHANGE_NODE,
                node(PlanNodeType.TABLE_COLLECT_NODE, collect(), collect()))));
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.TABLE_EXCHANGE_NODE,
            node(PlanNodeType.TABLE_COLLECT_NODE, collect(), collect())),
        pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testNoCollectKeepsLegacyBudget() {
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.TABLE_MERGESORT_NODE,
            node(PlanNodeType.TABLE_EXCHANGE_NODE),
            node(PlanNodeType.TABLE_EXCHANGE_NODE)),
        pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testInternalPipelineDisablesSharingForEntireFragment() {
    LocalExecutionPlanContext child = context.createSubContext();
    child.addPipelineDriverFactory(mock(Operator.class), child.getDriverContext(), 0);
    ISourceHandle source = addSource(child);
    PipelineSourceHandle pipelineSource = mock(PipelineSourceHandle.class);
    ExchangeOperator exchange = mock(ExchangeOperator.class);
    when(exchange.getSourceHandle()).thenReturn(pipelineSource);
    context.addExchangeOperator(exchange);
    context.setExchangeMemoryLimits(collect(), pool);
    verify(source).setMaxBytesCanReserve(256);
    verify(pipelineSource).setMaxBytesCanReserve(256);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testPipelineSchedulingDependencyDisablesSharing() {
    context.getDriverContext().setDependencyDriverIndex(0);
    context.setExchangeMemoryLimits(collect(), pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testSideSmallerThanOneBlockKeepsLegacyBudget() {
    addSource(context);
    context.setISink(mock(ISinkHandle.class));
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    context.setExchangeMemoryLimits(collect(), pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testSinkOnlyFragmentCanStillReserveOneFullBlock() {
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(64);
    context = newContext();
    context.setExchangeSumNum(1);
    ISinkHandle sink = mock(ISinkHandle.class);
    context.setISink(sink);
    context.setExchangeMemoryLimits(
        node(PlanNodeType.IDENTITY_SINK, node(PlanNodeType.SERIES_SCAN)), pool);
    verify(sink).setMaxBytesCanReserve(64);
  }
}
