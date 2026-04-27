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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class PipeTsFileToTabletsMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileToTabletsMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final ConcurrentSkipListSet<String> pipe = new ConcurrentSkipListSet<>();
  private final Map<String, Timer> pipeTimerMap = new ConcurrentHashMap<>();
  private final Map<String, Rate> pipeRateMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> pipeTabletCountMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> pipeTabletMemoryMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> pipeParseFileCountMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(pipe).forEach(this::createMetrics);
  }

  private void createMetrics(final String pipeID) {
    pipeTimerMap.putIfAbsent(
        pipeID,
        metricService.getOrCreateTimer(
            Metric.PIPE_TSFILE_TO_TABLETS_TIME.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeID));
    pipeRateMap.putIfAbsent(
        pipeID,
        metricService.getOrCreateRate(
            Metric.PIPE_TSFILE_TO_TABLETS_RATE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeID));
    pipeTabletCountMap.putIfAbsent(
        pipeID,
        metricService.getOrCreateCounter(
            Metric.PIPE_TSFILE_TO_TABLETS_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeID));
    pipeTabletMemoryMap.putIfAbsent(
        pipeID,
        metricService.getOrCreateCounter(
            Metric.PIPE_TSFILE_TO_TABLETS_TOTAL_MEMORY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeID));
    pipeParseFileCountMap.putIfAbsent(
        pipeID,
        metricService.getOrCreateCounter(
            Metric.PIPE_TSFILE_PARSE_FILE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeID));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(pipe).forEach(this::deregister);
    if (!pipe.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe tsfile to tablets metrics, pipe map is not empty, pipe: {}",
          pipe);
    }
  }

  private void removeMetrics(final String pipeID) {
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_TSFILE_TO_TABLETS_TIME.toString(),
        Tag.NAME.toString(),
        pipeID);
    pipeTimerMap.remove(pipeID);

    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TSFILE_TO_TABLETS_RATE.toString(),
        Tag.NAME.toString(),
        pipeID);
    pipeRateMap.remove(pipeID);

    metricService.remove(
        MetricType.COUNTER,
        Metric.PIPE_TSFILE_TO_TABLETS_COUNT.toString(),
        Tag.NAME.toString(),
        pipeID);
    pipeTabletCountMap.remove(pipeID);

    metricService.remove(
        MetricType.COUNTER,
        Metric.PIPE_TSFILE_TO_TABLETS_TOTAL_MEMORY.toString(),
        Tag.NAME.toString(),
        pipeID);
    pipeTabletMemoryMap.remove(pipeID);

    metricService.remove(
        MetricType.COUNTER,
        Metric.PIPE_TSFILE_PARSE_FILE_COUNT.toString(),
        Tag.NAME.toString(),
        pipeID);
    pipeParseFileCountMap.remove(pipeID);
  }

  //////////////////////////// register & deregister ////////////////////////////

  public void register(final IoTDBDataRegionSource extractor) {
    final String pipeID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    pipe.add(pipeID);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void deregister(final String pipeID) {
    if (!pipe.contains(pipeID)) {
      LOGGER.warn(
          "Failed to deregister pipe tsfile to tablets metrics, pipeID({}) does not exist", pipeID);
      return;
    }
    try {
      if (Objects.nonNull(metricService)) {
        removeMetrics(pipeID);
      }
    } finally {
      pipe.remove(pipeID);
    }
  }

  //////////////////////////// pipe integration ////////////////////////////

  public void markTsFileToTabletInvocation(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = pipeRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe tsfile to tablets invocation, pipeID({}) does not exist", taskID);
      return;
    }
    rate.mark();
  }

  public void recordTsFileToTabletTime(final String taskID, long costTimeInNanos) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Timer timer = pipeTimerMap.get(taskID);
    if (timer == null) {
      LOGGER.info(
          "Failed to record pipe tsfile to tablets time, pipeID({}) does not exist", taskID);
      return;
    }
    timer.updateNanos(costTimeInNanos);
    // Increment file count for this pipe when parsing ends
    final Counter fileCount = pipeParseFileCountMap.get(taskID);
    if (fileCount != null) {
      fileCount.inc();
    }
  }

  public void recordTabletGenerated(final String taskID, long tabletMemorySize) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Counter tabletCount = pipeTabletCountMap.get(taskID);
    if (tabletCount == null) {
      LOGGER.info("Failed to record tablet generated, pipeID({}) does not exist", taskID);
      return;
    }
    tabletCount.inc();
    final Counter tabletMemory = pipeTabletMemoryMap.get(taskID);
    if (tabletMemory != null) {
      tabletMemory.inc(tabletMemorySize);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class Holder {

    private static final PipeTsFileToTabletsMetrics INSTANCE = new PipeTsFileToTabletsMetrics();

    private Holder() {
      // Empty constructor
    }
  }

  public static PipeTsFileToTabletsMetrics getInstance() {
    return Holder.INSTANCE;
  }
}
