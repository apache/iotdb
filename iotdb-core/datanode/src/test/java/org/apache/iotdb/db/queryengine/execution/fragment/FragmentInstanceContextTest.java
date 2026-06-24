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
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;

import javax.management.ObjectName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;

public class FragmentInstanceContextTest {

  @Test
  public void testDrainAggregationCostsSeparatelyOnce() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);

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
          driverContext.addOperatorContext(1, new PlanNodeId("aggregation"), "aggregation");

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
  public void testAggregationCostsFlushToMetricStagesOnce() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    RecordingMetricService metricService = new RecordingMetricService();
    QueryExecutionMetricSet metricSet = QueryExecutionMetricSet.getInstance();
    metricSet.bindTo(metricService);
    try {
      FragmentInstanceContext context = newFragmentInstanceContext(instanceNotificationExecutor);

      context.recordScanAggregationFromRawDataCost(10);
      context.recordScanAggregationFromStatisticsCost(20);
      context.recordAggregationOperatorFromRawDataCost(30);

      context.recordAggregationCostToMetric();

      assertEquals(1, metricService.count("raw_data"));
      assertEquals(10, metricService.sum("raw_data"));
      assertEquals(1, metricService.count("statistics"));
      assertEquals(20, metricService.sum("statistics"));
      assertEquals(1, metricService.count("raw_data_operator"));
      assertEquals(30, metricService.sum("raw_data_operator"));

      context.recordAggregationCostToMetric();

      assertEquals(1, metricService.count("raw_data"));
      assertEquals(1, metricService.count("statistics"));
      assertEquals(1, metricService.count("raw_data_operator"));
    } finally {
      metricSet.unbindFrom(metricService);
      instanceNotificationExecutor.shutdown();
    }
  }

  private static FragmentInstanceContext newFragmentInstanceContext(
      ExecutorService instanceNotificationExecutor) {
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("test_query"), 0), "0");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    return createFragmentInstanceContext(instanceId, stateMachine);
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
}
