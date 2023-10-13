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
import org.apache.iotdb.db.pipe.extractor.IoTDBDataRegionExtractor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeExtractorMetrics implements IMetricSet {

  private final Map<String, IoTDBDataRegionExtractor> extractorMap = new HashMap<>();

  private final Map<String, Rate> tabletRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> tsFileRateMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> pipeHeartbeatRateMap = new ConcurrentHashMap<>();

  private AbstractMetricService metricService;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String taskID : extractorMap.keySet()) {
        createMetrics(taskID);
      }
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing
  }

  private static class PipeExtractorMetricsHolder {

    private static final PipeExtractorMetrics INSTANCE = new PipeExtractorMetrics();

    private PipeExtractorMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeExtractorMetrics getInstance() {
    return PipeExtractorMetrics.PipeExtractorMetricsHolder.INSTANCE;
  }

  private PipeExtractorMetrics() {
    // empty constructor
  }

  public void register(@NonNull IoTDBDataRegionExtractor extractor) {
    String taskID = extractor.getTaskID();
    synchronized (this) {
      if (!extractorMap.containsKey(taskID)) {
        extractorMap.put(taskID, extractor);
      }
      if (Objects.nonNull(metricService)) {
        createMetrics(taskID);
      }
    }
  }

  private void createAutoGauge(String taskID) {
    metricService.createAutoGauge(
        Metric.UNPROCESSED_HISTORICAL_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractorMap.get(taskID),
        IoTDBDataRegionExtractor::getHistoricalTsFileInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.UNPROCESSED_REALTIME_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractorMap.get(taskID),
        IoTDBDataRegionExtractor::getRealtimeTsFileInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.UNPROCESSED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractorMap.get(taskID),
        IoTDBDataRegionExtractor::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.UNPROCESSED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractorMap.get(taskID),
        IoTDBDataRegionExtractor::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        taskID);
  }

  private void createMetrics(String taskID) {
    createAutoGauge(taskID);
    createRate(taskID);
  }

  private void createRate(String taskID) {
    tabletRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_EXTRACTOR_TABLET_SUPPLY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
    tsFileRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_EXTRACTOR_TS_FILE_SUPPLY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
    pipeHeartbeatRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_EXTRACTOR_HEARTBEAT_SUPPLY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            taskID));
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
