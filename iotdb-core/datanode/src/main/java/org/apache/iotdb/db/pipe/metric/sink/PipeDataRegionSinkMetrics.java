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

package org.apache.iotdb.db.pipe.metric.sink;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeDataRegionSinkMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataRegionSinkMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, PipeSinkSubtask> connectorMap = new HashMap<>();

  private final Map<String, Rate> tabletRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tsFileRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> pipeHeartbeatRateMap = new ConcurrentHashMap<>();

  private final Map<String, Timer> compressionTimerMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> taskIDs = ImmutableSet.copyOf(connectorMap.keySet());
    for (String taskID : taskIDs) {
      createMetrics(taskID);
    }
  }

  private void createMetrics(final String taskID) {
    createAutoGauge(taskID);
    createRate(taskID);
    createTimer(taskID);
    createHistogram(taskID);
  }

  private void createAutoGauge(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    // Pending event count
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    // Metrics related to IoTDBThriftAsyncConnector
    metricService.createAutoGauge(
        Metric.PIPE_ASYNC_CONNECTOR_RETRY_EVENT_QUEUE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getAsyncConnectorRetryEventQueueSize,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.createAutoGauge(
        Metric.PIPE_PENDING_HANDLERS_SIZE.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getPendingHandlersSize,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    // Metrics related to IoTDB connector
    metricService.createAutoGauge(
        Metric.PIPE_TOTAL_UNCOMPRESSED_SIZE.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getTotalUncompressedSize,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.createAutoGauge(
        Metric.PIPE_TOTAL_COMPRESSED_SIZE.toString(),
        MetricLevel.IMPORTANT,
        connector,
        PipeSinkSubtask::getTotalCompressedSize,
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
  }

  private void createRate(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    // Transfer event rate
    tabletRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_TABLET_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.INDEX.toString(),
            String.valueOf(connector.getConnectorIndex()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime())));
    tsFileRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_TSFILE_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.INDEX.toString(),
            String.valueOf(connector.getConnectorIndex()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime())));
    pipeHeartbeatRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_HEARTBEAT_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.INDEX.toString(),
            String.valueOf(connector.getConnectorIndex()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime())));
  }

  private void createTimer(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    compressionTimerMap.putIfAbsent(
        connector.getAttributeSortedString(),
        metricService.getOrCreateTimer(
            Metric.PIPE_COMPRESSION_TIME.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime())));
  }

  private void createHistogram(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);

    final Histogram tabletBatchSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.PIPE_INSERT_NODE_BATCH_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime()));
    connector.setTabletBatchSizeHistogram(tabletBatchSizeHistogram);

    final Histogram tsFileBatchSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.PIPE_TSFILE_BATCH_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime()));
    connector.setTsFileBatchSizeHistogram(tsFileBatchSizeHistogram);

    final Histogram tabletBatchTimeIntervalHistogram =
        metricService.getOrCreateHistogram(
            Metric.PIPE_INSERT_NODE_BATCH_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime()));
    connector.setTabletBatchTimeIntervalHistogram(tabletBatchTimeIntervalHistogram);

    final Histogram tsFileBatchTimeIntervalHistogram =
        metricService.getOrCreateHistogram(
            Metric.PIPE_TSFILE_BATCH_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime()));
    connector.setTsFileBatchTimeIntervalHistogram(tsFileBatchTimeIntervalHistogram);

    Histogram eventSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.PIPE_CONNECTOR_BATCH_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString());
    connector.setEventSizeHistogram(eventSizeHistogram);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> taskIDs = ImmutableSet.copyOf(connectorMap.keySet());
    for (final String taskID : taskIDs) {
      deregister(taskID);
    }
    if (!connectorMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe data region connector metrics, connector map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeAutoGauge(taskID);
    removeRate(taskID);
    removeTimer(taskID);
    removeHistogram(taskID);
  }

  private void removeAutoGauge(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    // Pending event count
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNTRANSFERRED_TABLET_COUNT.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNTRANSFERRED_TSFILE_COUNT.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNTRANSFERRED_HEARTBEAT_COUNT.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    // Metrics related to IoTDBThriftAsyncConnector
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_ASYNC_CONNECTOR_RETRY_EVENT_QUEUE_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_PENDING_HANDLERS_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    // Metrics related to IoTDB connector
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_TOTAL_UNCOMPRESSED_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_TOTAL_COMPRESSED_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
  }

  private void removeRate(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    // Transfer event rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_CONNECTOR_TABLET_TRANSFER.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_CONNECTOR_TSFILE_TRANSFER.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_CONNECTOR_HEARTBEAT_TRANSFER.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.INDEX.toString(),
        String.valueOf(connector.getConnectorIndex()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    tabletRateMap.remove(taskID);
    tsFileRateMap.remove(taskID);
    pipeHeartbeatRateMap.remove(taskID);
  }

  private void removeTimer(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_COMPRESSION_TIME.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    compressionTimerMap.remove(connector.getAttributeSortedString());
  }

  private void removeHistogram(final String taskID) {
    final PipeSinkSubtask connector = connectorMap.get(taskID);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.PIPE_INSERT_NODE_BATCH_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.PIPE_TSFILE_BATCH_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.PIPE_INSERT_NODE_BATCH_TIME_COST.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.PIPE_TSFILE_BATCH_TIME_COST.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));

    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.PIPE_CONNECTOR_BATCH_SIZE.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString());
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final PipeSinkSubtask pipeSinkSubtask) {
    final String taskID = pipeSinkSubtask.getTaskID();
    connectorMap.putIfAbsent(taskID, pipeSinkSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!connectorMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe data region connector metrics, PipeConnectorSubtask({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    connectorMap.remove(taskID);
  }

  public void markTabletEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tabletRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe data region connector tablet event, PipeConnectorSubtask({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  public void markTsFileEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tsFileRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe data region connector tsfile event, PipeConnectorSubtask({}) does not exist",
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
      // Do not warn for the schema region events
      return;
    }
    rate.mark();
  }

  public Timer getCompressionTimer(final String attributeSortedString) {
    return Objects.isNull(metricService) ? null : compressionTimerMap.get(attributeSortedString);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeSinkMetricsHolder {

    private static final PipeDataRegionSinkMetrics INSTANCE = new PipeDataRegionSinkMetrics();

    private PipeSinkMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeDataRegionSinkMetrics getInstance() {
    return PipeSinkMetricsHolder.INSTANCE;
  }

  private PipeDataRegionSinkMetrics() {
    // Empty constructor
  }
}
