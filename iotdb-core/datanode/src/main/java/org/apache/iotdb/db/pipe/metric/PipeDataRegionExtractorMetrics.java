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
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeDataRegionExtractorMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeDataRegionExtractorMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, IoTDBDataRegionExtractor> extractorMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tabletRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tsFileRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> pipeHeartbeatRateMap = new ConcurrentHashMap<>();

  private final Map<String, Gauge> recentProcessedTsFileEpochStateMap = new ConcurrentHashMap<>();

  public Map<String, IoTDBDataRegionExtractor> getExtractorMap() {
    return extractorMap;
  }

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> taskIDs = ImmutableSet.copyOf(extractorMap.keySet());
    for (final String taskID : taskIDs) {
      createMetrics(taskID);
    }
  }

  private void createMetrics(final String taskID) {
    createAutoGauge(taskID);
    createRate(taskID);
    createGauge(taskID);
  }

  private void createAutoGauge(final String taskID) {
    final IoTDBDataRegionExtractor extractor = extractorMap.get(taskID);
    // Pending event count
    metricService.createAutoGauge(
        Metric.UNPROCESSED_HISTORICAL_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractor,
        IoTDBDataRegionExtractor::getHistoricalTsFileInsertionEventCount,
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.createAutoGauge(
        Metric.UNPROCESSED_REALTIME_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractor,
        IoTDBDataRegionExtractor::getRealtimeTsFileInsertionEventCount,
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.createAutoGauge(
        Metric.UNPROCESSED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractor,
        IoTDBDataRegionExtractor::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.createAutoGauge(
        Metric.UNPROCESSED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractor,
        IoTDBDataRegionExtractor::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
  }

  private void createRate(final String taskID) {
    final IoTDBDataRegionExtractor extractor = extractorMap.get(taskID);
    // Supply event rate
    tabletRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_EXTRACTOR_TABLET_SUPPLY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            extractor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(extractor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(extractor.getCreationTime())));
    tsFileRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_EXTRACTOR_TSFILE_SUPPLY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            extractor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(extractor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(extractor.getCreationTime())));
    pipeHeartbeatRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_EXTRACTOR_HEARTBEAT_SUPPLY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            extractor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(extractor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(extractor.getCreationTime())));
  }

  private void createGauge(final String taskID) {
    final IoTDBDataRegionExtractor extractor = extractorMap.get(taskID);
    // Tsfile epoch state
    recentProcessedTsFileEpochStateMap.put(
        taskID,
        metricService.getOrCreateGauge(
            Metric.PIPE_EXTRACTOR_TSFILE_EPOCH_STATE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            extractor.getPipeName(),
            Tag.REGION.toString(),
            String.valueOf(extractor.getRegionId()),
            Tag.CREATION_TIME.toString(),
            String.valueOf(extractor.getCreationTime())));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> taskIDs = ImmutableSet.copyOf(extractorMap.keySet());
    for (final String taskID : taskIDs) {
      deregister(taskID);
    }
    if (!extractorMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe extractor metrics, extractor map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeAutoGauge(taskID);
    removeRate(taskID);
    removeGauge(taskID);
  }

  private void removeAutoGauge(final String taskID) {
    final IoTDBDataRegionExtractor extractor = extractorMap.get(taskID);
    // pending event count
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNPROCESSED_HISTORICAL_TSFILE_COUNT.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNPROCESSED_REALTIME_TSFILE_COUNT.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNPROCESSED_TABLET_COUNT.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNPROCESSED_HEARTBEAT_COUNT.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
  }

  private void removeRate(final String taskID) {
    final IoTDBDataRegionExtractor extractor = extractorMap.get(taskID);
    // supply event rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_EXTRACTOR_TABLET_SUPPLY.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_EXTRACTOR_TSFILE_SUPPLY.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_EXTRACTOR_HEARTBEAT_SUPPLY.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
    tabletRateMap.remove(taskID);
    tsFileRateMap.remove(taskID);
    pipeHeartbeatRateMap.remove(taskID);
  }

  private void removeGauge(final String taskID) {
    final IoTDBDataRegionExtractor extractor = extractorMap.get(taskID);
    // Tsfile epoch state
    metricService.remove(
        MetricType.GAUGE,
        Metric.PIPE_EXTRACTOR_TSFILE_EPOCH_STATE.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.REGION.toString(),
        String.valueOf(extractor.getRegionId()),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final IoTDBDataRegionExtractor extractor) {
    final String taskID = extractor.getTaskID();
    extractorMap.putIfAbsent(taskID, extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!extractorMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe data region extractor metrics, IoTDBDataRegionExtractor({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    extractorMap.remove(taskID);
  }

  public void markTabletEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tabletRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe data region extractor tablet event, IoTDBDataRegionExtractor({}) does not exist",
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
          "Failed to mark pipe data region extractor tsfile event, IoTDBDataRegionExtractor({}) does not exist",
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
          "Failed to mark pipe data region extractor heartbeat event, IoTDBDataRegionExtractor({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  public void setRecentProcessedTsFileEpochState(
      final String taskID, final TsFileEpoch.State state) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Gauge gauge = recentProcessedTsFileEpochStateMap.get(taskID);
    if (gauge == null) {
      LOGGER.info(
          "Failed to set recent processed tsfile epoch state, PipeRealtimeDataRegionExtractor({}) does not exist",
          taskID);
      return;
    }
    gauge.set(state.getId());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeExtractorMetricsHolder {

    private static final PipeDataRegionExtractorMetrics INSTANCE =
        new PipeDataRegionExtractorMetrics();

    private PipeExtractorMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeDataRegionExtractorMetrics getInstance() {
    return PipeDataRegionExtractorMetrics.PipeExtractorMetricsHolder.INSTANCE;
  }

  private PipeDataRegionExtractorMetrics() {
    // Empty constructor
  }
}
