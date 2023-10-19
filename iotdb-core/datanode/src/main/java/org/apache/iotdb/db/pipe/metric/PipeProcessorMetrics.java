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
import org.apache.iotdb.db.pipe.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeProcessorMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorMetrics.class);

  private AbstractMetricService metricService;

  private final Map<String, PipeProcessorSubtask> processorMap = new HashMap<>();

  private final Map<String, Rate> tabletRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tsFileRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> pipeHeartbeatRateMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String taskID : processorMap.keySet()) {
        createMetrics(taskID);
      }
    }
  }

  private void createMetrics(String taskID) {
    createAutoGauge(taskID);
    createRate(taskID);
  }

  private void createAutoGauge(String taskID) {
    metricService.createAutoGauge(
        Metric.BUFFERED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        processorMap.get(taskID),
        PipeProcessorSubtask::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.BUFFERED_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        processorMap.get(taskID),
        PipeProcessorSubtask::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.BUFFERED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        processorMap.get(taskID),
        PipeProcessorSubtask::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        taskID);
  }

  private void createRate(String taskID) {
    tabletRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_PROCESSOR_TABLET_PROCESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
    tsFileRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_PROCESSOR_TSFILE_PROCESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
    pipeHeartbeatRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_PROCESSOR_HEARTBEAT_PROCESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    ImmutableSet<String> taskIDs = ImmutableSet.copyOf(processorMap.keySet());
    for (String taskID : taskIDs) {
      deregister(taskID);
    }
    if (!processorMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe processor metrics, processor map not empty");
    }
  }

  private void removeMetrics(String taskID) {
    removeAutoGauge(taskID);
    removeRate(taskID);
  }

  private void removeAutoGauge(String taskID) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.BUFFERED_TABLET_COUNT.toString(),
        Tag.NAME.toString(),
        taskID);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.BUFFERED_TSFILE_COUNT.toString(),
        Tag.NAME.toString(),
        taskID);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.BUFFERED_HEARTBEAT_COUNT.toString(),
        Tag.NAME.toString(),
        taskID);
  }

  private void removeRate(String taskID) {
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_PROCESSOR_TABLET_PROCESS.toString(),
        Tag.NAME.toString(),
        taskID);
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_PROCESSOR_TSFILE_PROCESS.toString(),
        Tag.NAME.toString(),
        taskID);
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_PROCESSOR_HEARTBEAT_PROCESS.toString(),
        Tag.NAME.toString(),
        taskID);
    tabletRateMap.remove(taskID);
    tsFileRateMap.remove(taskID);
    pipeHeartbeatRateMap.remove(taskID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull PipeProcessorSubtask pipeProcessorSubtask) {
    String taskID = pipeProcessorSubtask.getTaskID();
    synchronized (this) {
      processorMap.putIfAbsent(taskID, pipeProcessorSubtask);
      if (Objects.nonNull(metricService)) {
        createMetrics(taskID);
      }
    }
  }

  public void deregister(String taskID) {
    synchronized (this) {
      if (!processorMap.containsKey(taskID)) {
        LOGGER.info(
            "Failed to deregister pipe processor metrics, PipeProcessorSubtask({}) does not exist",
            taskID);
        return;
      }
      if (Objects.nonNull(metricService)) {
        removeMetrics(taskID);
      }
      processorMap.remove(taskID);
    }
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

  //////////////////////////// singleton ////////////////////////////

  private static class PipeProcessorMetricsHolder {

    private static final PipeProcessorMetrics INSTANCE = new PipeProcessorMetrics();

    private PipeProcessorMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeProcessorMetrics getInstance() {
    return PipeProcessorMetrics.PipeProcessorMetricsHolder.INSTANCE;
  }

  private PipeProcessorMetrics() {
    // empty constructor
  }
}
