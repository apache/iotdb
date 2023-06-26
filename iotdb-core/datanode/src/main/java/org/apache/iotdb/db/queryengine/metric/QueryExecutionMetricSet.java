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

package org.apache.iotdb.db.queryengine.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class QueryExecutionMetricSet implements IMetricSet {
  private static final QueryExecutionMetricSet INSTANCE = new QueryExecutionMetricSet();

  private QueryExecutionMetricSet() {
    // empty constructor
  }

  // region read dispatch
  public static final String WAIT_FOR_DISPATCH = "wait_for_dispatch";
  public static final String DISPATCH_READ = "dispatch_read";
  private Timer waitForDispatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer dispatchReadTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindQueryDispatch(AbstractMetricService metricService) {
    waitForDispatchTimer =
        metricService.getOrCreateTimer(
            Metric.DISPATCHER.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            WAIT_FOR_DISPATCH);
    dispatchReadTimer =
        metricService.getOrCreateTimer(
            Metric.DISPATCHER.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            DISPATCH_READ);
  }

  private void unbindQueryDispatch(AbstractMetricService metricService) {
    waitForDispatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    dispatchReadTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(WAIT_FOR_DISPATCH, DISPATCH_READ)
        .forEach(
            stage -> {
              metricService.remove(
                  MetricType.TIMER, Metric.DISPATCHER.toString(), Tag.STAGE.toString(), stage);
            });
  }
  // endregion

  // region read execution
  public static final String LOCAL_EXECUTION_PLANNER = "local_execution_planner";
  public static final String QUERY_RESOURCE_INIT = "query_resource_init";
  public static final String GET_QUERY_RESOURCE_FROM_MEM = "get_query_resource_from_mem";
  public static final String DRIVER_INTERNAL_PROCESS = "driver_internal_process";
  public static final String WAIT_FOR_RESULT = "wait_for_result";

  private Timer localExecutionPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer queryResourceInitTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer getQueryResourceFromMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer driverInternalProcessTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer waitForResultTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindQueryExecution(AbstractMetricService metricService) {
    localExecutionPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOCAL_EXECUTION_PLANNER);
    queryResourceInitTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            QUERY_RESOURCE_INIT);
    getQueryResourceFromMemTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            GET_QUERY_RESOURCE_FROM_MEM);
    driverInternalProcessTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            DRIVER_INTERNAL_PROCESS);
    waitForResultTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            WAIT_FOR_RESULT);
  }

  private void unbindQueryExecution(AbstractMetricService metricService) {
    localExecutionPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    queryResourceInitTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    getQueryResourceFromMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    driverInternalProcessTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    waitForResultTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(
            LOCAL_EXECUTION_PLANNER,
            QUERY_RESOURCE_INIT,
            GET_QUERY_RESOURCE_FROM_MEM,
            DRIVER_INTERNAL_PROCESS,
            WAIT_FOR_RESULT)
        .forEach(
            stage -> {
              metricService.remove(
                  MetricType.TIMER, Metric.QUERY_EXECUTION.toString(), Tag.STAGE.toString(), stage);
            });
  }
  // endregion

  // region read aggregation
  public static final String AGGREGATION_FROM_RAW_DATA = "aggregation_from_raw_data";
  public static final String AGGREGATION_FROM_STATISTICS = "aggregation_from_statistics";
  private Timer aggregationFromRawDataTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer aggregationFromStatisticsTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindQueryAggregation(AbstractMetricService metricService) {
    aggregationFromRawDataTimer =
        metricService.getOrCreateTimer(
            Metric.AGGREGATION.toString(), MetricLevel.IMPORTANT, Tag.FROM.toString(), "raw_data");
    aggregationFromStatisticsTimer =
        metricService.getOrCreateTimer(
            Metric.AGGREGATION.toString(),
            MetricLevel.IMPORTANT,
            Tag.FROM.toString(),
            "statistics");
  }

  private void unbindQueryAggregation(AbstractMetricService metricService) {
    aggregationFromRawDataTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    aggregationFromStatisticsTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList("raw_data", "statistics")
        .forEach(
            from -> {
              metricService.remove(
                  MetricType.TIMER, Metric.AGGREGATION.toString(), Tag.FROM.toString(), from);
            });
  }
  // endregion

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindQueryExecution(metricService);
    bindQueryDispatch(metricService);
    bindQueryAggregation(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindQueryExecution(metricService);
    unbindQueryDispatch(metricService);
    unbindQueryAggregation(metricService);
  }

  public void recordExecutionCost(String stage, long costTimeInNanos) {
    switch (stage) {
      case WAIT_FOR_DISPATCH:
        waitForDispatchTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case DISPATCH_READ:
        dispatchReadTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case LOCAL_EXECUTION_PLANNER:
        localExecutionPlannerTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case QUERY_RESOURCE_INIT:
        queryResourceInitTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case GET_QUERY_RESOURCE_FROM_MEM:
        getQueryResourceFromMemTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case DRIVER_INTERNAL_PROCESS:
        driverInternalProcessTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case WAIT_FOR_RESULT:
        waitForResultTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case AGGREGATION_FROM_RAW_DATA:
        aggregationFromRawDataTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      case AGGREGATION_FROM_STATISTICS:
        aggregationFromStatisticsTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
        break;
      default:
        break;
    }
  }

  public static QueryExecutionMetricSet getInstance() {
    return INSTANCE;
  }
}
