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

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;

import com.google.common.collect.ArrayListMultimap;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
    FragmentInstanceContext instance = mock(FragmentInstanceContext.class);
    when(instance.getId())
        .thenReturn(new FragmentInstanceId(new PlanFragmentId(new QueryId("budget"), 0), "0"));
    LocalExecutionPlanContext result =
        new LocalExecutionPlanContext(new TypeProvider(), instance, (DataNodeQueryContext) null);
    result.addPipelineDriverFactory(mock(Operator.class), result.getDriverContext(), 0);
    result.setExchangeSumNum(4);
    return result;
  }

  private static PlanNode node(PlanNodeType type, PlanNode... children) {
    Class<? extends PlanNode> nodeClass;
    switch (type) {
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
    String fi = context.getFragmentInstanceId().getFullId();
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
  public void testPureCollectUsesSharedSideBudgets() {
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
  public void testTreeCollectUsesSharedBudget() {
    addSource(context);
    context.setExchangeMemoryLimits(
        node(PlanNodeType.TREE_COLLECT, node(PlanNodeType.EXCHANGE), node(PlanNodeType.EXCHANGE)),
        pool);
    assertAdmissionIsShared(true);
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
          SharedExchangeMemoryEligibility.isEligible(
              node(PlanNodeType.TABLE_COLLECT_NODE, dependent, collect())));
      assertFalse(
          SharedExchangeMemoryEligibility.isEligible(node(dependency, collect(), collect())));
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
        SharedExchangeMemoryEligibility.isEligible(
            node(PlanNodeType.IDENTITY_SINK, collect(), collect())));
    assertFalse(
        SharedExchangeMemoryEligibility.isEligible(
            node(PlanNodeType.SCHEMA_QUERY_MERGE, collect(), collect())));
  }

  @Test
  public void testExchangeBoundaryDoesNotInspectUpstreamFragment() {
    PlanNode remote = node(PlanNodeType.TABLE_MERGESORT_NODE, collect(), collect());
    assertTrue(
        SharedExchangeMemoryEligibility.isEligible(
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE, remote),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))));
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
    context.getCurrentPipelineDriverFactory().setDependencyPipeline(0);
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

  @Test
  public void testMergeSortGetsBothHeadsDespitePrefetchOnOneInput() throws Exception {
    context.setExchangeMemoryLimits(
        node(PlanNodeType.TABLE_MERGESORT_NODE, collect(), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
        pool);
    String fi = context.getFragmentInstanceId().getFullId();
    pool.registerPlanNodeIdToQueryMemoryMap("budget", fi, "a", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap("budget", fi, "b", MemorySide.INPUT);
    for (int i = 0; i < 4; i++) {
      assertTrue(pool.reserveWithPriority("budget", fi, "a", 64, 256, false).isReserveSuccess());
    }
    MemoryPool.MemoryReservationResult prefetch =
        pool.reserveWithPriority("budget", fi, "a", 64, 256, false);
    assertFalse(prefetch.getFuture().isDone());
    MemoryPool.MemoryReservationResult headB =
        pool.reserveWithPriority("budget", fi, "b", 64, 256, false);
    assertTrue(headB.isReserveSuccess());
    Operator a = mock(Operator.class);
    Operator b = mock(Operator.class);
    doReturn(Operator.NOT_BLOCKED).when(a).isBlocked();
    doReturn(headB.getFuture()).when(b).isBlocked();
    when(a.hasNextWithTimer()).thenReturn(true);
    when(b.hasNextWithTimer()).thenReturn(true);
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(1);
    builder.getColumnBuilder(0).writeInt(1);
    builder.declarePosition();
    TsBlock first = builder.build();
    builder.reset();
    builder.getTimeColumnBuilder().writeLong(2);
    builder.getColumnBuilder(0).writeInt(2);
    builder.declarePosition();
    TsBlock second = builder.build();
    when(a.nextWithTimer())
        .thenAnswer(
            call -> {
              pool.free("budget", fi, "a", 64);
              return first;
            });
    when(b.nextWithTimer())
        .thenAnswer(
            call -> {
              pool.free("budget", fi, "b", 64);
              return second;
            });
    TableMergeSortOperator merge =
        new TableMergeSortOperator(
            mock(CommonOperatorContext.class),
            new ArrayList<>(Arrays.asList(a, b)),
            Collections.singletonList(TSDataType.INT32),
            Comparator.comparingLong(key -> key.tsBlock.getTimeByIndex(key.rowIndex)));
    assertTrue(merge.isBlocked().isDone());
    TsBlock result = merge.next();
    assertTrue(result != null && !result.isEmpty());
    assertTrue(prefetch.getFuture().isDone());
    pool.free("budget", fi, "a", 256);
    merge.close();
  }
}
