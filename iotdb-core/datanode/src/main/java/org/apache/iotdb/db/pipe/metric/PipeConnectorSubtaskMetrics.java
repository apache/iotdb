/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeConnectorSubtaskMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtaskMetrics.class);

  private AbstractMetricService metricService;

  private final Map<String, PipeConnectorSubtask> taskMap = new HashMap<>();

  private static class PipeConnectorSubtaskMetricsHolder {

    private static final PipeConnectorSubtaskMetrics INSTANCE = new PipeConnectorSubtaskMetrics();

    private PipeConnectorSubtaskMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeConnectorSubtaskMetrics getInstance() {
    return PipeConnectorSubtaskMetrics.PipeConnectorSubtaskMetricsHolder.INSTANCE;
  }

  private PipeConnectorSubtaskMetrics() {
    // empty constructor
  }

  public void register(@NonNull PipeConnectorSubtask pipeConnectorSubtask) {
    String taskID = pipeConnectorSubtask.getTaskID();
    synchronized (this) {
      if (!taskMap.containsKey(taskID)) {
        taskMap.put(taskID, pipeConnectorSubtask);
      }
      if (Objects.nonNull(metricService)) {
        createMetrics(taskID);
      }
    }
  }

  private void createMetrics(String taskID) {
    metricService.createAutoGauge(
        Metric.TRANSFERRED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.TRANSFERRED_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.TRANSFERRED_PIPE_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        taskID);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String taskID : taskMap.keySet()) {
        createMetrics(taskID);
      }
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing
  }
}
