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

package org.apache.iotdb.commons.service.metric.enums;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.HashMap;
import java.util.Map;

public class PerformanceOverviewMetrics implements IMetricSet {
  private static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  private PerformanceOverviewMetrics() {
    // empty constructor
  }

  // region overview

  private static final String PERFORMANCE_OVERVIEW_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_DETAIL.toString();
  private static final String AUTHORITY = "authority";
  private static final String PARSER = "parser";
  private static final String ANALYZER = "analyzer";
  private static final String PLANNER = "planner";
  private static final String SCHEDULER = "scheduler";

  static {
    metricInfoMap.put(
        AUTHORITY,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), AUTHORITY));
    metricInfoMap.put(
        PARSER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), PARSER));
    metricInfoMap.put(
        ANALYZER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), ANALYZER));
    metricInfoMap.put(
        PLANNER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), PLANNER));
    metricInfoMap.put(
        SCHEDULER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), SCHEDULER));
  }

  private Timer authTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer parseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer analyzeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer planTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer scheduleTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  /** Record the time cost in authority stage. */
  public void recordAuthCost(long costTimeInNanos) {
    authTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost in parse stage. */
  public void recordParseCost(long costTimeInNanos) {
    parseTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost in analyze stage. */
  public void recordAnalyzeCost(long costTimeInNanos) {
    analyzeTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost in plan stage. */
  public void recordPlanCost(long costTimeInNanos) {
    planTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost in schedule stage. */
  public void recordScheduleCost(long costTimeInNanos) {
    scheduleTimer.updateNanos(costTimeInNanos);
  }

  // endregion

  // region schedule

  private static final String PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL.toString();
  private static final String LOCAL_SCHEDULE = "local_scheduler";
  private static final String REMOTE_SCHEDULE = "remote_scheduler";

  static {
    metricInfoMap.put(
        LOCAL_SCHEDULE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
            Tag.STAGE.toString(),
            LOCAL_SCHEDULE));
    metricInfoMap.put(
        REMOTE_SCHEDULE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
            Tag.STAGE.toString(),
            REMOTE_SCHEDULE));
  }

  private Timer localScheduleTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer remoteScheduleTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  /** Record the time cost of local schedule. */
  public void recordScheduleLocalCost(long costTimeInNanos) {
    localScheduleTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of remote schedule. */
  public void recordScheduleRemoteCost(long costTimeInNanos) {
    remoteScheduleTimer.updateNanos(costTimeInNanos);
  }

  // endregion

  // region local schedule
  private static final String PERFORMANCE_OVERVIEW_LOCAL_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_LOCAL_DETAIL.toString();
  private static final String SCHEMA_VALIDATE = "schema_validate";
  private static final String TRIGGER = "trigger";
  private static final String STORAGE = "storage";

  static {
    metricInfoMap.put(
        SCHEMA_VALIDATE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
            Tag.STAGE.toString(),
            SCHEMA_VALIDATE));
    metricInfoMap.put(
        TRIGGER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_LOCAL_DETAIL, Tag.STAGE.toString(), TRIGGER));
    metricInfoMap.put(
        STORAGE,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_LOCAL_DETAIL, Tag.STAGE.toString(), STORAGE));
  }

  private Timer schemaValidateTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer triggerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer storageTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  /** Record the time cost of schema validate stage in local schedule. */
  public void recordScheduleSchemaValidateCost(long costTimeInNanos) {
    schemaValidateTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of trigger stage in local schedule. */
  public void recordScheduleTriggerCost(long costTimeInNanos) {
    triggerTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of storage stage in local schedule. */
  public void recordScheduleStorageCost(long costTimeInNanos) {
    storageTimer.updateNanos(costTimeInNanos);
  }

  // endregion

  // region storage
  private static final String PERFORMANCE_OVERVIEW_STORAGE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_STORAGE_DETAIL.toString();
  private static final String ENGINE = "engine";

  static {
    metricInfoMap.put(
        ENGINE,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_STORAGE_DETAIL, Tag.STAGE.toString(), ENGINE));
  }

  private Timer engineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordEngineCost(long costTimeInNanos) {
    engineTimer.updateNanos(costTimeInNanos);
  }

  // endregion

  // region engine

  private static final String PERFORMANCE_OVERVIEW_ENGINE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_ENGINE_DETAIL.toString();
  private static final String LOCK = "lock";
  private static final String MEMORY_BLOCK = "memory_block";
  private static final String CREATE_MEMTABLE_BLOCK = "create_memtable_block";
  private static final String WAL = "wal";
  private static final String MEMTABLE = "memtable";
  private static final String LAST_CACHE = "last_cache";

  static {
    metricInfoMap.put(
        LOCK,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_ENGINE_DETAIL, Tag.STAGE.toString(), LOCK));
    metricInfoMap.put(
        CREATE_MEMTABLE_BLOCK,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            Tag.STAGE.toString(),
            CREATE_MEMTABLE_BLOCK));
    metricInfoMap.put(
        MEMORY_BLOCK,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            Tag.STAGE.toString(),
            MEMORY_BLOCK));
    metricInfoMap.put(
        WAL,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_ENGINE_DETAIL, Tag.STAGE.toString(), WAL));
    metricInfoMap.put(
        MEMTABLE,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_ENGINE_DETAIL, Tag.STAGE.toString(), MEMTABLE));
    metricInfoMap.put(
        LAST_CACHE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            Tag.STAGE.toString(),
            LAST_CACHE));
  }

  private Timer lockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer createMemtableBlockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer memoryBlockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer walTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer memtableTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer lastCacheTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  /** Record the time cost of lock in engine. */
  public void recordScheduleLockCost(long costTimeInNanos) {
    lockTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of create memtable block in engine. */
  public void recordCreateMemtableBlockCost(long costTimeInNanos) {
    createMemtableBlockTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of memory block in engine. */
  public void recordScheduleMemoryBlockCost(long costTimeInNanos) {
    memoryBlockTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of wal in engine. */
  public void recordScheduleWalCost(long costTimeInNanos) {
    walTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of memtable in engine. */
  public void recordScheduleMemTableCost(long costTimeInNanos) {
    memtableTimer.updateNanos(costTimeInNanos);
  }

  /** Record the time cost of update last cache in engine. */
  public void recordScheduleUpdateLastCacheCost(long costTimeInNanos) {
    lastCacheTimer.updateNanos(costTimeInNanos);
  }

  // endregion

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // bind overview metrics
    authTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), AUTHORITY);
    parseTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), PARSER);
    analyzeTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), ANALYZER);
    planTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), PLANNER);
    scheduleTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), SCHEDULER);
    // bind schedule metrics
    localScheduleTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
            MetricLevel.CORE,
            Tag.STAGE.toString(),
            LOCAL_SCHEDULE);
    remoteScheduleTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
            MetricLevel.CORE,
            Tag.STAGE.toString(),
            REMOTE_SCHEDULE);
    // bind local schedule metrics
    schemaValidateTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
            MetricLevel.CORE,
            Tag.STAGE.toString(),
            SCHEMA_VALIDATE);
    triggerTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_LOCAL_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), TRIGGER);
    storageTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_LOCAL_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), STORAGE);
    // bind storage metrics
    engineTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_STORAGE_DETAIL,
            MetricLevel.CORE,
            Tag.STAGE.toString(),
            PerformanceOverviewMetrics.ENGINE);
    // bind engine metrics
    lockTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), LOCK);
    createMemtableBlockTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            MetricLevel.CORE,
            Tag.STAGE.toString(),
            CREATE_MEMTABLE_BLOCK);
    memoryBlockTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            MetricLevel.CORE,
            Tag.STAGE.toString(),
            MEMORY_BLOCK);
    walTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), WAL);
    memtableTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), MEMTABLE);
    lastCacheTimer =
        metricService.getOrCreateTimer(
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL, MetricLevel.CORE, Tag.STAGE.toString(), LAST_CACHE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.remove(MetricType.TIMER, metricInfo.getName(), metricInfo.getTagsInArray());
    }
  }

  private static class PerformanceOverviewMetricsHolder {

    private static final PerformanceOverviewMetrics INSTANCE = new PerformanceOverviewMetrics();

    private PerformanceOverviewMetricsHolder() {
      // empty constructor
    }
  }

  public static PerformanceOverviewMetrics getInstance() {
    return PerformanceOverviewMetricsHolder.INSTANCE;
  }
}
