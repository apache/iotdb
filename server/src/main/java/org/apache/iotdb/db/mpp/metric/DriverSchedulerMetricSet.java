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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.mpp.execution.schedule.DriverScheduler;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class DriverSchedulerMetricSet implements IMetricSet {

  private static final String metric = Metric.DRIVER_SCHEDULER.toString();

  public static final String READY_QUEUED_TIME = "ready_queued_time";
  public static final String BLOCK_QUEUED_TIME = "block_queued_time";

  public static final String READY_QUEUE_TASK_COUNT = "ready_queue_task_count";
  public static final String BLOCK_QUEUE_TASK_COUNT = "block_queue_task_count";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.getOrCreateTimer(
        metric, MetricLevel.IMPORTANT, Tag.NAME.toString(), READY_QUEUED_TIME);
    metricService.getOrCreateTimer(
        metric, MetricLevel.IMPORTANT, Tag.NAME.toString(), BLOCK_QUEUED_TIME);
    metricService.createAutoGauge(
        metric,
        MetricLevel.IMPORTANT,
        DriverScheduler.getInstance(),
        DriverScheduler::getReadyQueueTaskCount,
        Tag.NAME.toString(),
        READY_QUEUE_TASK_COUNT);
    metricService.createAutoGauge(
        metric,
        MetricLevel.IMPORTANT,
        DriverScheduler.getInstance(),
        DriverScheduler::getBlockQueueTaskCount,
        Tag.NAME.toString(),
        BLOCK_QUEUE_TASK_COUNT);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(MetricType.TIMER, metric, Tag.NAME.toString(), READY_QUEUED_TIME);
    metricService.remove(MetricType.TIMER, metric, Tag.NAME.toString(), BLOCK_QUEUED_TIME);
    metricService.remove(
        MetricType.AUTO_GAUGE, metric, Tag.NAME.toString(), READY_QUEUE_TASK_COUNT);
    metricService.remove(
        MetricType.AUTO_GAUGE, metric, Tag.NAME.toString(), BLOCK_QUEUE_TASK_COUNT);
  }
}
