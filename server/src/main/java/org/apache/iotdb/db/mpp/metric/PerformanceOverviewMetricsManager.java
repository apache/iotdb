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

package org.apache.iotdb.db.mpp.metric;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.TimeUnit;

public class PerformanceOverviewMetricsManager {
  private final MetricService metricService = MetricService.getInstance();

  public void recordAuthCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.PERFORMANCE_OVERVIEW_DETAIL.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        PerformanceOverviewDetailMetrics.AUTHORITY);
  }

  public void recordParseCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.PERFORMANCE_OVERVIEW_DETAIL.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        PerformanceOverviewDetailMetrics.PARSER);
  }

  public void recordAnalyzeCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.PERFORMANCE_OVERVIEW_DETAIL.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        PerformanceOverviewDetailMetrics.ANALYZER);
  }

  public void recordPlanCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.PERFORMANCE_OVERVIEW_DETAIL.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        PerformanceOverviewDetailMetrics.PLANNER);
  }

  public void recordScheduleCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.PERFORMANCE_OVERVIEW_DETAIL.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        PerformanceOverviewDetailMetrics.SCHEDULER);
  }

  public static PerformanceOverviewMetricsManager getInstance() {
    return PerformanceOverviewMetricsManager.PerformanceOverviewMetricsManagerHolder.INSTANCE;
  }

  private static class PerformanceOverviewMetricsManagerHolder {

    private static final PerformanceOverviewMetricsManager INSTANCE =
        new PerformanceOverviewMetricsManager();

    private PerformanceOverviewMetricsManagerHolder() {
      // empty constructor
    }
  }
}
