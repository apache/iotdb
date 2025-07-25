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

package org.apache.iotdb.confignode.manager.pipe.metric.source;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.pipe.source.IoTDBConfigRegionSource;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeConfigRegionSourceMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigRegionSourceMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, IoTDBConfigRegionSource> extractorMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(extractorMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String taskID) {
    createAutoGauge(taskID);
  }

  private void createAutoGauge(final String taskID) {
    final IoTDBConfigRegionSource extractor = extractorMap.get(taskID);
    metricService.createAutoGauge(
        Metric.UNTRANSFERRED_CONFIG_COUNT.toString(),
        MetricLevel.IMPORTANT,
        extractorMap.get(taskID),
        IoTDBConfigRegionSource::getUnTransferredEventCount,
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(extractorMap.keySet()).forEach(this::deregister);
    if (!extractorMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe config region extractor metrics, extractor map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeAutoGauge(taskID);
  }

  private void removeAutoGauge(final String taskID) {
    final IoTDBConfigRegionSource extractor = extractorMap.get(taskID);
    // Pending event count
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNTRANSFERRED_CONFIG_COUNT.toString(),
        Tag.NAME.toString(),
        extractor.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(extractor.getCreationTime()));
  }

  //////////////////////////// pipe integration ////////////////////////////

  public void register(final IoTDBConfigRegionSource extractor) {
    final String taskID = extractor.getTaskID();
    extractorMap.putIfAbsent(taskID, extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!extractorMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe config region extractor metrics, IoTDBConfigRegionExtractor({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    extractorMap.remove(taskID);
  }

  //////////////////////////// Show pipes ////////////////////////////

  public long getRemainingEventCount(final String pipeName, final long creationTime) {
    final String taskID = pipeName + "_" + creationTime;
    final IoTDBConfigRegionSource extractor = extractorMap.get(taskID);
    // Do not print log to allow collection when config region extractor does not exists
    if (Objects.isNull(extractor)) {
      return 0;
    }
    return extractor.getUnTransferredEventCount();
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConfigRegionExtractorMetricsHolder {

    private static final PipeConfigRegionSourceMetrics INSTANCE =
        new PipeConfigRegionSourceMetrics();

    private PipeConfigRegionExtractorMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeConfigRegionSourceMetrics getInstance() {
    return PipeConfigRegionExtractorMetricsHolder.INSTANCE;
  }

  private PipeConfigRegionSourceMetrics() {
    // Empty constructor
  }
}
