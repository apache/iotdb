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
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
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

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, PipeProcessorSubtask> processorMap = new HashMap<>();

  private final Map<String, Rate> tabletRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tsFileRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> pipeHeartbeatRateMap = new ConcurrentHashMap<>();

  private final Map<String, Counter> tsFileSizeMap = new ConcurrentHashMap<>();

  private final Map<String, Counter> tsFileCountMap = new ConcurrentHashMap<>();

  private long byteUsed = 0;
  private long tabletTotal = 0;

  private static final String TRANSFORMED_TOTAL_SIZE = "total_transformed_size";

  private static final String TRANSFORMED_TOTAL_COUNT = "total_transformed_count";

  public long getByteUsed() {
    return byteUsed;
  }

  public void addByteUsed(long byteUsed) {
    this.byteUsed += byteUsed;
  }

  public long getTabletTotal() {
    return tabletTotal;
  }

  public void addTabletTotal(long tabletTotal) {
    this.tabletTotal += tabletTotal;
  }

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> taskIDs = ImmutableSet.copyOf(processorMap.keySet());
    for (final String taskID : taskIDs) {
      createMetrics(taskID);
    }
    // tsfile length and tablet total
    createAutoGaugeMetrics();
  }

  private void createAutoGaugeMetrics() {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        PipeProcessorMetrics::getByteUsed,
        Tag.NAME.toString(),
        TRANSFORMED_TOTAL_SIZE);
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        PipeProcessorMetrics::getTabletTotal,
        Tag.NAME.toString(),
        TRANSFORMED_TOTAL_COUNT);
  }

  private void createMetrics(final String taskID) {
    createRate(taskID);
    createCounter(taskID);
  }

  private void createCounter(String taskID) {
    final PipeProcessorSubtask processor = processorMap.get(taskID);
    tsFileSizeMap.put(
        taskID,
        metricService.getOrCreateCounter(
            TRANSFORMED_TOTAL_SIZE,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            processor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(processor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(processor.getCreationTime())));
    tsFileCountMap.put(
        taskID,
        metricService.getOrCreateCounter(
            TRANSFORMED_TOTAL_COUNT,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            processor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(processor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(processor.getCreationTime())));
  }

  private void createRate(final String taskID) {
    final PipeProcessorSubtask processor = processorMap.get(taskID);
    // process event rate
    tabletRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_PROCESSOR_TABLET_PROCESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            processor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(processor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(processor.getCreationTime())));
    tsFileRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_PROCESSOR_TSFILE_PROCESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            processor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(processor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(processor.getCreationTime())));
    pipeHeartbeatRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_PROCESSOR_HEARTBEAT_PROCESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            processor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(processor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(processor.getCreationTime())));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> taskIDs = ImmutableSet.copyOf(processorMap.keySet());
    for (final String taskID : taskIDs) {
      deregister(taskID);
    }
    if (!processorMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe processor metrics, processor map not empty");
    }
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.FILE_SIZE.toString(),
        Tag.NAME.toString(),
        TRANSFORMED_TOTAL_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.FILE_COUNT.toString(),
        Tag.NAME.toString(),
        TRANSFORMED_TOTAL_COUNT);
  }

  private void removeMetrics(final String taskID) {
    removeRate(taskID);
    removeCount(taskID);
  }

  private void removeCount(String taskID) {
    PipeProcessorSubtask processor = processorMap.get(taskID);
    // process tsfile count
    metricService.remove(
        MetricType.COUNTER,
        TRANSFORMED_TOTAL_SIZE,
        Tag.NAME.toString(),
        processor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(processor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(processor.getCreationTime()));
    metricService.remove(
        MetricType.COUNTER,
        TRANSFORMED_TOTAL_COUNT,
        Tag.NAME.toString(),
        processor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(processor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(processor.getCreationTime()));

    tsFileSizeMap.remove(taskID);
    tsFileCountMap.remove(taskID);
  }

  private void removeRate(final String taskID) {
    PipeProcessorSubtask processor = processorMap.get(taskID);
    // process event rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_PROCESSOR_TABLET_PROCESS.toString(),
        Tag.NAME.toString(),
        processor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(processor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(processor.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_PROCESSOR_TSFILE_PROCESS.toString(),
        Tag.NAME.toString(),
        processor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(processor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(processor.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_PROCESSOR_HEARTBEAT_PROCESS.toString(),
        Tag.NAME.toString(),
        processor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(processor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(processor.getCreationTime()));
    tabletRateMap.remove(taskID);
    tsFileRateMap.remove(taskID);
    pipeHeartbeatRateMap.remove(taskID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final PipeProcessorSubtask pipeProcessorSubtask) {
    final String taskID = pipeProcessorSubtask.getTaskID();
    processorMap.putIfAbsent(taskID, pipeProcessorSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!processorMap.containsKey(taskID)) {
      // Allow calls from schema region tasks
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    processorMap.remove(taskID);
  }

  public void markTabletEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tabletRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe processor tablet event, PipeProcessorSubtask({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  public void markTsFileEvent(final String taskID) {

    LOGGER.info("here now");
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tsFileRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe processor tsfile event, PipeProcessorSubtask({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  public void markPipeHeartbeatEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = pipeHeartbeatRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe processor heartbeat event, PipeProcessorSubtask({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  public void incTsFileSize(final String taskID, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Counter counter = tsFileSizeMap.get(taskID);
    if (counter == null) {
      LOGGER.info(
          "Failed to inc pipe processor tsfile size, PipeProcessorSubtask({}) does not exist",
          taskID);
      return;
    }
    counter.inc(size);
  }

  public void incTsFileCount(final String taskID, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Counter counter = tsFileCountMap.get(taskID);
    if (counter == null) {
      LOGGER.info(
          "Failed to inc pipe processor tsfile count, PipeProcessorSubtask({}) does not exist",
          taskID);
      return;
    }
    counter.inc(size);
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
