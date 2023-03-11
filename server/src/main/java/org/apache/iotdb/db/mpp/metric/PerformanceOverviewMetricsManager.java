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
import org.apache.iotdb.commons.service.metric.enums.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;

public class PerformanceOverviewMetricsManager {
  private static final MetricService metricService = MetricService.getInstance();

  // region overview
  private static final String PERFORMANCE_OVERVIEW_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_DETAIL.toString();
  private static final Timer AUTH_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.AUTHORITY);
  private static final Timer PARSER_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.PARSER);
  private static final Timer ANALYZE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.ANALYZER);
  private static final Timer PLAN_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.PLANNER);
  private static final Timer SCHEDULE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.SCHEDULER);

  /** Record the time cost in authority stage. */
  public static void recordAuthCost(long costTimeInNanos) {
    AUTH_TIMER.updateNanos(costTimeInNanos);
  }

  /** Record the time cost in parse stage. */
  public static void recordParseCost(long costTimeInNanos) {
    PARSER_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordAnalyzeCost(long costTimeInNanos) {
    ANALYZE_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordPlanCost(long costTimeInNanos) {
    PLAN_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleCost(long costTimeInNanos) {
    SCHEDULE_TIMER.updateNanos(costTimeInNanos);
  }
  // endregion

  // region schedule
  private static final String PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL.toString();
  private static final Timer LOCAL_SCHEDULE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.LOCAL_SCHEDULE);
  private static final Timer REMOTE_SCHEDULE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.REMOTE_SCHEDULE);

  public static void recordScheduleLocalCost(long costTimeInNanos) {
    LOCAL_SCHEDULE_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleRemoteCost(long costTimeInNanos) {
    REMOTE_SCHEDULE_TIMER.updateNanos(costTimeInNanos);
  }

  // endregion

  // region local schedule
  private static final String PERFORMANCE_OVERVIEW_LOCAL_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_LOCAL_DETAIL.toString();
  private static final Timer SCHEMA_VALIDATE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.SCHEMA_VALIDATE);
  private static final Timer TRIGGER_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.TRIGGER);
  private static final Timer STORAGE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.STORAGE);

  public static void recordScheduleSchemaValidateCost(long costTimeInNanos) {
    SCHEMA_VALIDATE_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleTriggerCost(long costTimeInNanos) {
    TRIGGER_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleStorageCost(long costTimeInNanos) {
    STORAGE_TIMER.updateNanos(costTimeInNanos);
  }

  // endregion

  // region engine
  private static final String PERFORMANCE_OVERVIEW_ENGINE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_ENGINE_DETAIL.toString();
  private static final Timer LOCK_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.LOCK);
  private static final Timer CREATE_MEMTABLE_BLOCK_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.CREATE_MEMTABLE_BLOCK);
  private static final Timer MEMORY_BLOCK_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.MEMORY_BLOCK);
  private static final Timer WAL_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.WAL);
  private static final Timer MEMTABLE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.MEMTABLE);
  private static final Timer LAST_CACHE_TIMER =
      metricService.getOrCreateTimer(
          PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
          MetricLevel.IMPORTANT,
          Tag.STAGE.toString(),
          PerformanceOverviewMetrics.LAST_CACHE);

  public static void recordScheduleLockCost(long costTimeInNanos) {
    LOCK_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordCreateMemtableBlockCost(long costTimeInNanos) {
    CREATE_MEMTABLE_BLOCK_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleMemoryBlockCost(long costTimeInNanos) {
    MEMORY_BLOCK_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleWalCost(long costTimeInNanos) {
    WAL_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleMemTableCost(long costTimeInNanos) {
    MEMTABLE_TIMER.updateNanos(costTimeInNanos);
  }

  public static void recordScheduleUpdateLastCacheCost(long costTimeInNanos) {
    LAST_CACHE_TIMER.updateNanos(costTimeInNanos);
  }
}
