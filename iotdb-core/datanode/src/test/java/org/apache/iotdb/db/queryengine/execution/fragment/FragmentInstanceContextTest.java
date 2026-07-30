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

import org.apache.iotdb.calc.execution.aggregation.Accumulator;
import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.calc.metric.QueryExecutionMetricSet;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.TagAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import javax.management.ObjectName;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;
import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FragmentInstanceContextTest {

  @Test
  public void testDrainAggregationCostsSeparatelyOnce() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext context = createFragmentInstanceContext(instanceId, stateMachine);

      context.recordScanAggregationFromRawDataCost(10);
      context.recordScanAggregationFromRawDataCost(20);
      context.recordScanAggregationFromStatisticsCost(30);
      context.recordAggregationOperatorFromRawDataCost(40);

      assertEquals(30, context.drainScanAggregationFromRawDataCost());
      assertEquals(30, context.drainScanAggregationFromStatisticsCost());
      assertEquals(40, context.drainAggregationOperatorFromRawDataCost());

      assertEquals(0, context.drainScanAggregationFromRawDataCost());
      assertEquals(0, context.drainScanAggregationFromStatisticsCost());
      assertEquals(0, context.drainAggregationOperatorFromRawDataCost());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testOperatorContextForwardsAggregationCostsToFragmentInstanceContext() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);
      DriverContext driverContext = new DriverContext(context, 0);
      OperatorContext operatorContext =
          driverContext.addOperatorContext(1, new PlanNodeId("forward"), "forward");

      operatorContext.recordScanAggregationFromRawDataCost(11);
      operatorContext.recordScanAggregationFromStatisticsCost(13);
      operatorContext.recordAggregationOperatorFromRawDataCost(17);

      assertEquals(11, context.drainScanAggregationFromRawDataCost());
      assertEquals(13, context.drainScanAggregationFromStatisticsCost());
      assertEquals(17, context.drainAggregationOperatorFromRawDataCost());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testOperatorContextWithoutFragmentInstanceAndCommonContextUseNoOpFallback() {
    OperatorContext operatorContext =
        new OperatorContext(1, new PlanNodeId("no-op"), "no-op", new DriverContext());
    operatorContext.recordScanAggregationFromRawDataCost(11);
    operatorContext.recordScanAggregationFromStatisticsCost(13);
    operatorContext.recordAggregationOperatorFromRawDataCost(17);

    TestCommonOperatorContext commonOperatorContext = new TestCommonOperatorContext();
    commonOperatorContext.recordScanAggregationFromRawDataCost(19);
    commonOperatorContext.recordScanAggregationFromStatisticsCost(23);
    commonOperatorContext.recordAggregationOperatorFromRawDataCost(29);

    assertEquals(0, commonOperatorContext.getTotalExecutionTimeInNanos());
    assertNull(commonOperatorContext.getMemoryReservationContext());
  }

  @Test
  public void testAggregationCostsFromOperatorsFlushToMetricStages() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    RecordingMetricService metricService = new RecordingMetricService();
    QueryExecutionMetricSet metricSet = QueryExecutionMetricSet.getInstance();
    metricSet.bindTo(metricService);
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);
      DriverContext driverContext = new DriverContext(context, 0);
      OperatorContext scanOperatorContext =
          driverContext.addOperatorContext(1, new PlanNodeId("scan"), "scan");
      TestSeriesAggregationScanOperator scanOperator =
          new TestSeriesAggregationScanOperator(scanOperatorContext);

      scanOperator.calculateRaw(longTsBlock());
      scanOperator.calculateStatistics(
          Statistics.getStatsByType(TSDataType.INT64),
          new Statistics[] {Statistics.getStatsByType(TSDataType.INT64)});

      OperatorContext aggregationOperatorContext =
          driverContext.addOperatorContext(2, new PlanNodeId("aggregation"), "aggregation");
      org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationOperator
          aggregationOperator = newAggregationOperator(aggregationOperatorContext);
      aggregationOperator.next();

      context.recordAggregationCostToMetric();

      assertEquals(1, metricService.count("raw_data"));
      assertTrue(metricService.sum("raw_data") > 0);
      assertEquals(1, metricService.count("statistics"));
      assertTrue(metricService.sum("statistics") > 0);
      assertEquals(1, metricService.count("raw_data_operator"));
      assertTrue(metricService.sum("raw_data_operator") > 0);

      context.recordAggregationCostToMetric();

      assertEquals(1, metricService.count("raw_data"));
      assertEquals(1, metricService.count("statistics"));
      assertEquals(1, metricService.count("raw_data_operator"));
    } finally {
      metricSet.unbindFrom(metricService);
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testAggregationOperatorRecordsTsBlockCostForIntermediateInput() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);
      DriverContext driverContext = new DriverContext(context, 0);
      OperatorContext operatorContext =
          driverContext.addOperatorContext(1, new PlanNodeId("aggregation"), "aggregation");
      org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationOperator
          aggregationOperator = newAggregationOperator(operatorContext);

      assertNull(aggregationOperator.next());

      assertTrue(context.drainAggregationOperatorFromRawDataCost() > 0);
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testTreeAggregationOperatorRecordsTsBlockCost() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);
      DriverContext driverContext = new DriverContext(context, 0);
      OperatorContext operatorContext =
          driverContext.addOperatorContext(1, new PlanNodeId("tree-aggregation"), "aggregation");
      org.apache.iotdb.db.queryengine.execution.operator.process.AggregationOperator
          aggregationOperator =
              new org.apache.iotdb.db.queryengine.execution.operator.process.AggregationOperator(
                  operatorContext,
                  singletonList(newTreeFinalAggregator()),
                  new SingleTimeRangeIterator(new TimeRange(0, 10)),
                  singletonList(new SingleTsBlockOperator(operatorContext, longTsBlock())),
                  false,
                  1024);

      aggregationOperator.isBlocked().get();
      aggregationOperator.next();

      assertTrue(context.drainAggregationOperatorFromRawDataCost() > 0);
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testTagAggregationOperatorRecordsTsBlockCost() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);
      DriverContext driverContext = new DriverContext(context, 0);
      OperatorContext operatorContext =
          driverContext.addOperatorContext(1, new PlanNodeId("tag-aggregation"), "tag-aggregation");
      TagAggregationOperator tagAggregationOperator =
          new TagAggregationOperator(
              operatorContext,
              singletonList(singletonList("tag")),
              singletonList(singletonList(newTreeFinalAggregator())),
              singletonList(new SingleTsBlockOperator(operatorContext, longTsBlock())),
              1024);

      tagAggregationOperator.isBlocked().get();
      tagAggregationOperator.next();

      assertTrue(context.drainAggregationOperatorFromRawDataCost() > 0);
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private static FragmentInstanceContext newFragmentInstanceContext(
      ExecutorService instanceNotificationExecutor) {
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    return createFragmentInstanceContext(instanceId, stateMachine);
  }

  private static TsBlock longTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(singletonList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeLong(1);
    builder.declarePosition();
    return builder.build();
  }

  private static org.apache.iotdb.calc.execution.operator.source.relational.aggregation
          .AggregationOperator
      newAggregationOperator(OperatorContext operatorContext) {
    TableAggregator aggregator =
        new TableAggregator(
            new TestTableAccumulator(),
            AggregationNode.Step.INTERMEDIATE,
            TSDataType.INT64,
            singletonList(0),
            OptionalInt.empty());
    return new org.apache.iotdb.calc.execution.operator.source.relational.aggregation
        .AggregationOperator(
        operatorContext,
        new SingleTsBlockOperator(operatorContext, longTsBlock()),
        singletonList(aggregator));
  }

  private static TreeAggregator newTreeFinalAggregator() {
    return new TreeAggregator(
        new TestTreeAccumulator(),
        AggregationStep.FINAL,
        singletonList(new InputLocation[] {new InputLocation(0, 0)}));
  }

  private static class TestSeriesAggregationScanOperator
      extends AbstractSeriesAggregationScanOperator {

    private TestSeriesAggregationScanOperator(OperatorContext operatorContext) {
      super(
          new PlanNodeId("scan"),
          operatorContext,
          null,
          1,
          singletonList(new TreeAggregator(new TestTreeAccumulator(), AggregationStep.SINGLE)),
          new SingleTimeRangeIterator(new TimeRange(0, 10)),
          true,
          false,
          null,
          1024,
          0,
          true);
      this.curTimeRange = new TimeRange(0, 10);
    }

    private void calculateRaw(TsBlock tsBlock) {
      this.inputTsBlock = tsBlock;
      calcFromCachedData();
    }

    private void calculateStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
      calcFromStatistics(timeStatistics, valueStatistics);
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  private static class SingleTimeRangeIterator implements ITimeRangeIterator {
    private final TimeRange timeRange;
    private boolean consumed;

    private SingleTimeRangeIterator(TimeRange timeRange) {
      this.timeRange = timeRange;
    }

    @Override
    public TimeRange getFirstTimeRange() {
      return timeRange;
    }

    @Override
    public boolean hasNextTimeRange() {
      return !consumed;
    }

    @Override
    public TimeRange nextTimeRange() {
      consumed = true;
      return timeRange;
    }

    @Override
    public boolean isAscending() {
      return true;
    }

    @Override
    public long currentOutputTime() {
      return timeRange.getMin();
    }

    @Override
    public long getTotalIntervalNum() {
      return 1;
    }
  }

  private static class RecordingMetricService extends DoNothingMetricService {
    private final Map<String, RecordingTimer> timers = new HashMap<>();

    @Override
    public Timer getOrCreateTimer(String metric, MetricLevel metricLevel, String... tags) {
      if (!Metric.AGGREGATION.toString().equals(metric)
          || tags.length != 2
          || !Tag.FROM.toString().equals(tags[0])) {
        return super.getOrCreateTimer(metric, metricLevel, tags);
      }
      return timers.computeIfAbsent(tags[1], key -> new RecordingTimer());
    }

    @Override
    public void remove(MetricType type, String metric, String... tags) {
      if (Metric.AGGREGATION.toString().equals(metric)
          && tags.length == 2
          && Tag.FROM.toString().equals(tags[0])) {
        timers.remove(tags[1]);
      }
    }

    long count(String from) {
      RecordingTimer timer = timers.get(from);
      return timer == null ? 0 : timer.getCount();
    }

    long sum(String from) {
      RecordingTimer timer = timers.get(from);
      return timer == null ? 0 : timer.sum.get();
    }
  }

  private static class RecordingTimer implements Timer {
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong sum = new AtomicLong();

    @Override
    public void update(long duration, TimeUnit unit) {
      count.incrementAndGet();
      sum.addAndGet(unit.toNanos(duration));
    }

    @Override
    public HistogramSnapshot takeSnapshot() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCount() {
      return count.get();
    }

    @Override
    public void setObjectName(ObjectName objectName) {
      // no-op
    }
  }

  private static class SingleTsBlockOperator implements Operator {
    private final CommonOperatorContext operatorContext;
    private final TsBlock tsBlock;
    private boolean consumed;

    private SingleTsBlockOperator(CommonOperatorContext operatorContext, TsBlock tsBlock) {
      this.operatorContext = operatorContext;
      this.tsBlock = tsBlock;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public TsBlock next() {
      consumed = true;
      return tsBlock;
    }

    @Override
    public boolean hasNext() {
      return !consumed;
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public boolean isFinished() {
      return consumed;
    }

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  private static class TestTableAccumulator implements TableAccumulator {
    @Override
    public long getEstimatedSize() {
      return 0;
    }

    @Override
    public TableAccumulator copy() {
      return new TestTableAccumulator();
    }

    @Override
    public void addInput(
        Column[] arguments,
        org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask
            mask) {
      // no-op
    }

    @Override
    public void addIntermediate(Column argument) {
      // no-op
    }

    @Override
    public void evaluateIntermediate(ColumnBuilder columnBuilder) {
      columnBuilder.writeLong(0);
    }

    @Override
    public void evaluateFinal(ColumnBuilder columnBuilder) {
      columnBuilder.writeLong(0);
    }

    @Override
    public boolean hasFinalResult() {
      return false;
    }

    @Override
    public void addStatistics(Statistics[] statistics) {
      // no-op
    }

    @Override
    public void reset() {
      // no-op
    }
  }

  private static class TestTreeAccumulator implements Accumulator {
    @Override
    public void addInput(Column[] columns, org.apache.tsfile.utils.BitMap bitMap) {
      // no-op
    }

    @Override
    public void addIntermediate(Column[] partialResult) {
      // no-op
    }

    @Override
    public void addStatistics(Statistics statistics) {
      // no-op
    }

    @Override
    public void setFinal(Column finalResult) {
      // no-op
    }

    @Override
    public void outputIntermediate(ColumnBuilder[] tsBlockBuilder) {
      tsBlockBuilder[0].writeLong(0);
    }

    @Override
    public void outputFinal(ColumnBuilder tsBlockBuilder) {
      tsBlockBuilder.writeLong(0);
    }

    @Override
    public void reset() {
      // no-op
    }

    @Override
    public boolean hasFinalResult() {
      return false;
    }

    @Override
    public TSDataType[] getIntermediateType() {
      return new TSDataType[] {TSDataType.INT64};
    }

    @Override
    public TSDataType getFinalType() {
      return TSDataType.INT64;
    }
  }

  private static class TestCommonOperatorContext extends CommonOperatorContext {
    private TestCommonOperatorContext() {
      super(0, new PlanNodeId("common"), "common");
    }

    @Override
    public MemoryReservationManager getMemoryReservationContext() {
      return null;
    }

    @Override
    public int getFragmentId() {
      return 0;
    }

    @Override
    public int getPipelineId() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }
}
