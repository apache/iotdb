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
import org.apache.iotdb.db.pipe.extractor.historical.PipeHistoricalDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeDataRegionExtractorMetrics implements IMetricSet {

  private final Map<String, PipeHistoricalDataRegionExtractor> historicalExtractorMap =
      new HashMap<>();

  private final Map<String, PipeRealtimeDataRegionExtractor> realtimeExtractorMap = new HashMap<>();

  private AbstractMetricService metricService;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String pipeName : realtimeExtractorMap.keySet()) {
        createMetrics(pipeName);
      }
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing
  }

  private static class PipeDataRegionExtractorMetricsHolder {

    private static final PipeDataRegionExtractorMetrics INSTANCE =
        new PipeDataRegionExtractorMetrics();

    private PipeDataRegionExtractorMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeDataRegionExtractorMetrics getInstance() {
    return PipeDataRegionExtractorMetrics.PipeDataRegionExtractorMetricsHolder.INSTANCE;
  }

  private PipeDataRegionExtractorMetrics() {
    // empty constructor
  }

  public void register(
      @NonNull PipeHistoricalDataRegionExtractor pipeHistoricalDataRegionExtractor,
      @NonNull PipeRealtimeDataRegionExtractor pipeRealtimeDataRegionExtractor) {
    String pipeName = pipeRealtimeDataRegionExtractor.getPipeName();
    synchronized (this) {
      if (!historicalExtractorMap.containsKey(pipeName)) {
        historicalExtractorMap.put(pipeName, pipeHistoricalDataRegionExtractor);
      }
      if (!realtimeExtractorMap.containsKey(pipeName)) {
        realtimeExtractorMap.put(pipeName, pipeRealtimeDataRegionExtractor);
      }
      if (Objects.nonNull(metricService)) {
        createMetrics(pipeName);
      }
    }
  }

  private void createMetrics(String pipeName) {
    metricService.createAutoGauge(
        Metric.UNPROCESSED_HISTORICAL_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        historicalExtractorMap.get(pipeName),
        PipeHistoricalDataRegionExtractor::getPendingQueueSize,
        Tag.NAME.toString(),
        pipeName);
    metricService.createAutoGauge(
        Metric.UNPROCESSED_REALTIME_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        realtimeExtractorMap.get(pipeName),
        PipeRealtimeDataRegionExtractor::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        pipeName);
    metricService.createAutoGauge(
        Metric.UNPROCESSED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        realtimeExtractorMap.get(pipeName),
        PipeRealtimeDataRegionExtractor::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        pipeName);
    metricService.createAutoGauge(
        Metric.UNPROCESSED_PIPE_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        realtimeExtractorMap.get(pipeName),
        PipeRealtimeDataRegionExtractor::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        pipeName);
  }
}
