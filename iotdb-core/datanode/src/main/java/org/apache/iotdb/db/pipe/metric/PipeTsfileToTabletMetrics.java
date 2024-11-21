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

public class PipeTsfileToTabletMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsfileToTabletMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, PipeProcessorSubtask> pipeMap = new ConcurrentHashMap<>();
  private final Map<String, Rate> tsFileSizeMap = new ConcurrentHashMap<>();
  private final Map<String, Rate> tabletCountMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> pipeIDs = ImmutableSet.copyOf(pipeMap.keySet());
    for (final String pipeID : pipeIDs) {
      createMetrics(pipeID);
    }
  }

  private void createMetrics(final String pipeID) {
    createRate(pipeID);
  }

  private void createRate(final String pipeID) {
    final PipeProcessorSubtask pipeProcessorSubtask = pipeMap.get(pipeID);
    // process event rate
    tsFileSizeMap.put(
        pipeID,
        metricService.getOrCreateRate(
            Metric.PIPE_TOTABLET_TSFILE_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeProcessorSubtask.getPipeName(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(pipeProcessorSubtask.getCreationTime())));
    tabletCountMap.put(
        pipeID,
        metricService.getOrCreateRate(
            Metric.PIPE_TOTABLET_TABLET_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeProcessorSubtask.getPipeName(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(pipeProcessorSubtask.getCreationTime())));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> pipeIDs = ImmutableSet.copyOf(pipeMap.keySet());
    for (final String pipeID : pipeIDs) {
      deregister(pipeID);
    }
    if (!pipeMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe processor metrics, processor map not empty");
    }
  }

  private void removeMetrics(final String pipeID) {
    removeRate(pipeID);
  }

  private void removeRate(final String pipeID) {
    PipeProcessorSubtask pipeProcessorSubtask = pipeMap.get(pipeID);
    // process event rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TOTABLET_TSFILE_SIZE.toString(),
        Tag.NAME.toString(),
        pipeProcessorSubtask.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(pipeProcessorSubtask.getCreationTime()));
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TOTABLET_TABLET_COUNT.toString(),
        Tag.NAME.toString(),
        pipeProcessorSubtask.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(pipeProcessorSubtask.getCreationTime()));
    tsFileSizeMap.remove(pipeID);
    tabletCountMap.remove(pipeID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final PipeProcessorSubtask pipeProcessorSubtask) {
    final String pipeID =
        pipeProcessorSubtask.getPipeName() + "_" + pipeProcessorSubtask.getCreationTime();
    pipeMap.putIfAbsent(pipeID, pipeProcessorSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void deregister(final String pipeID) {
    if (!pipeMap.containsKey(pipeID)) {
      // Allow calls from schema region tasks
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(pipeID);
    }
    pipeMap.remove(pipeID);
  }

  public void markTabletCount(final String pipeID, final long count) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tabletCountMap.get(pipeID);
    if (rate == null) {
      LOGGER.info("Failed to mark tablet count, Pipe({}) does not exist", pipeID);
      return;
    }
    rate.mark(count);
  }

  public void markTsfileSize(final String pipeID, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tsFileSizeMap.get(pipeID);
    if (rate == null) {
      LOGGER.info("Failed to mark tsfile size, Pipe({}) does not exist", pipeID);
      return;
    }
    rate.mark(size);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTsfileToTabletMetricsHolder {

    private static final PipeTsfileToTabletMetrics INSTANCE = new PipeTsfileToTabletMetrics();

    private PipeTsfileToTabletMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeTsfileToTabletMetrics getInstance() {
    return PipeTsfileToTabletMetrics.PipeTsfileToTabletMetricsHolder.INSTANCE;
  }

  private PipeTsfileToTabletMetrics() {
    // empty constructor
  }
}
