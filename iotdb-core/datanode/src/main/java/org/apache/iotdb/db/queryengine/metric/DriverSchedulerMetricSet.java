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
import org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class DriverSchedulerMetricSet implements IMetricSet {
  private static final DriverSchedulerMetricSet INSTANCE = new DriverSchedulerMetricSet();

  private DriverSchedulerMetricSet() {
    // empty constructor
  }

  public static final String READY_QUEUED_TIME = "ready_queued_time";
  public static final String BLOCK_QUEUED_TIME = "block_queued_time";
  public static final String READY_QUEUE_TASK_COUNT = "ready_queue_task_count";
  public static final String BLOCK_QUEUE_TASK_COUNT = "block_queue_task_count";
  private static final String TIMEOUT_QUEUE_SIZE = "timeout_queue_task_count";
  private static final String QUERY_MAP_SIZE = "query_map_size";

  private Timer readyQueuedTimeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer blockQueuedTimeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    readyQueuedTimeTimer =
        metricService.getOrCreateTimer(
            Metric.DRIVER_SCHEDULER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            READY_QUEUED_TIME);
    blockQueuedTimeTimer =
        metricService.getOrCreateTimer(
            Metric.DRIVER_SCHEDULER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            BLOCK_QUEUED_TIME);
    metricService.createAutoGauge(
        Metric.DRIVER_SCHEDULER.toString(),
        MetricLevel.IMPORTANT,
        DriverScheduler.getInstance(),
        DriverScheduler::getReadyQueueTaskCount,
        Tag.NAME.toString(),
        READY_QUEUE_TASK_COUNT);
    metricService.createAutoGauge(
        Metric.DRIVER_SCHEDULER.toString(),
        MetricLevel.IMPORTANT,
        DriverScheduler.getInstance(),
        DriverScheduler::getBlockQueueTaskCount,
        Tag.NAME.toString(),
        BLOCK_QUEUE_TASK_COUNT);
    metricService.createAutoGauge(
        Metric.DRIVER_SCHEDULER.toString(),
        MetricLevel.IMPORTANT,
        DriverScheduler.getInstance(),
        DriverScheduler::getTimeoutQueueTaskCount,
        Tag.NAME.toString(),
        TIMEOUT_QUEUE_SIZE);
    metricService.createAutoGauge(
        Metric.DRIVER_SCHEDULER.toString(),
        MetricLevel.IMPORTANT,
        DriverScheduler.getInstance(),
        DriverScheduler::getQueryMapSize,
        Tag.NAME.toString(),
        QUERY_MAP_SIZE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    readyQueuedTimeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    blockQueuedTimeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    metricService.remove(
        MetricType.TIMER,
        Metric.DRIVER_SCHEDULER.toString(),
        Tag.NAME.toString(),
        READY_QUEUED_TIME);
    metricService.remove(
        MetricType.TIMER,
        Metric.DRIVER_SCHEDULER.toString(),
        Tag.NAME.toString(),
        BLOCK_QUEUED_TIME);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DRIVER_SCHEDULER.toString(),
        Tag.NAME.toString(),
        READY_QUEUE_TASK_COUNT);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DRIVER_SCHEDULER.toString(),
        Tag.NAME.toString(),
        BLOCK_QUEUE_TASK_COUNT);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DRIVER_SCHEDULER.toString(),
        Tag.NAME.toString(),
        TIMEOUT_QUEUE_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DRIVER_SCHEDULER.toString(),
        Tag.NAME.toString(),
        QUERY_MAP_SIZE);
  }

  public void recordTaskQueueTime(String name, long queueTimeInNanos) {
    switch (name) {
      case READY_QUEUED_TIME:
        readyQueuedTimeTimer.updateNanos(queueTimeInNanos);
        break;
      case BLOCK_QUEUED_TIME:
        blockQueuedTimeTimer.updateNanos(queueTimeInNanos);
        break;
      default:
        break;
    }
  }

  public static DriverSchedulerMetricSet getInstance() {
    return INSTANCE;
  }
}
