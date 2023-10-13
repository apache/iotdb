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
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeConnectorMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorMetrics.class);

  private AbstractMetricService metricService;

  private final Map<String, PipeConnectorSubtask> taskMap = new HashMap<>();

  private final Map<String, Rate> tabletRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tsFileRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> pipeHeartbeatRateMap = new ConcurrentHashMap<>();

  private static class PipeConnectorMetricsHolder {

    private static final PipeConnectorMetrics INSTANCE = new PipeConnectorMetrics();

    private PipeConnectorMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeConnectorMetrics getInstance() {
    return PipeConnectorMetrics.PipeConnectorMetricsHolder.INSTANCE;
  }

  private PipeConnectorMetrics() {
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

  private void createAutoGauge(String taskID) {
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        taskID);
  }

  private void createRate(String taskID) {
    tabletRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_TABLET_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
    tsFileRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_TS_FILE_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
    pipeHeartbeatRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_HEARTBEAT_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
  }

  private void createMetrics(String taskID) {
    createAutoGauge(taskID);
    createRate(taskID);
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

  public Rate getTabletRate(String taskID) {
    return tabletRateMap.get(taskID);
  }

  public Rate getTsFileRate(String taskID) {
    return tsFileRateMap.get(taskID);
  }

  public Rate getPipeHeartbeatRate(String taskID) {
    return pipeHeartbeatRateMap.get(taskID);
  }
}
