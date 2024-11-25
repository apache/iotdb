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
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsfileToTabletMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsfileToTabletMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, Set<PipeID>> pipeIDSet = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Rate>> tsFileSizeMap = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Rate>> tabletCountMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> pipeFullNames = ImmutableSet.copyOf(pipeIDSet.keySet());
    for (final String pipeFullName : pipeFullNames) {
      for (final PipeID pipeID : pipeIDSet.get(pipeFullName)) {
        createMetrics(pipeID);
      }
    }
  }

  private void createMetrics(final PipeID pipeID) {
    createRate(pipeID);
  }

  private void createRate(final PipeID pipeID) {

    tsFileSizeMap.putIfAbsent(pipeID.getPipeFullName(), new ConcurrentHashMap<>());
    tsFileSizeMap
        .get(pipeID.getPipeFullName())
        .put(
            pipeID.getCallerName(),
            metricService.getOrCreateRate(
                Metric.PIPE_TOTABLET_TSFILE_SIZE.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                pipeID.getPipeName(),
                Tag.CREATION_TIME.toString(),
                pipeID.getCreationTime(),
                Tag.FROM.toString(),
                pipeID.getCallerName()));

    tabletCountMap.putIfAbsent(pipeID.getPipeFullName(), new ConcurrentHashMap<>());
    tabletCountMap
        .get(pipeID.getPipeFullName())
        .put(
            pipeID.getCallerName(),
            metricService.getOrCreateRate(
                Metric.PIPE_TOTABLET_TABLET_COUNT.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                pipeID.getPipeName(),
                Tag.CREATION_TIME.toString(),
                pipeID.getCreationTime(),
                Tag.FROM.toString(),
                pipeID.getCallerName()));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> pipeFullNames = ImmutableSet.copyOf(pipeIDSet.keySet());
    for (final String pipeFullName : pipeFullNames) {
      for (final PipeID pipeID : pipeIDSet.get(pipeFullName)) {
        removeMetrics(pipeID);
      }
      pipeIDSet.remove((pipeFullName));
    }
    if (!pipeIDSet.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe processor metrics, processor map not empty");
    }
  }

  private void removeMetrics(final PipeID pipeID) {
    removeRate(pipeID);
  }

  private void removeRate(final PipeID pipeID) {
    // process event rate
    final Map<String, Rate> tsFileSizeRates = tsFileSizeMap.get(pipeID.getPipeFullName());
    if (tsFileSizeRates != null) {
      tsFileSizeRates.remove(pipeID.getCallerName());
      metricService.remove(
          MetricType.RATE,
          Metric.PIPE_TOTABLET_TSFILE_SIZE.toString(),
          Tag.NAME.toString(),
          pipeID.getPipeName(),
          Tag.CREATION_TIME.toString(),
          pipeID.getCreationTime(),
          Tag.FROM.toString(),
          pipeID.getCallerName());
    }

    final Map<String, Rate> tabletCountRates = tabletCountMap.get(pipeID.getPipeFullName());
    if (tabletCountRates != null) {
      tabletCountRates.remove(pipeID.getCallerName());
      metricService.remove(
          MetricType.RATE,
          Metric.PIPE_TOTABLET_TABLET_COUNT.toString(),
          Tag.NAME.toString(),
          pipeID.getPipeName(),
          Tag.CREATION_TIME.toString(),
          pipeID.getCreationTime(),
          Tag.FROM.toString(),
          pipeID.getCallerName());
    }
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final PipeID pipeID) {
    pipeIDSet.putIfAbsent(
        pipeID.getPipeFullName(), Collections.newSetFromMap(new ConcurrentHashMap<>()));
    if (pipeIDSet.get(pipeID.getPipeFullName()).add(pipeID) && Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void deregister(final PipeID pipeID) {
    if (!pipeIDSet.containsKey(pipeID.getPipeFullName())
        || !pipeIDSet.get(pipeID.getPipeFullName()).contains(pipeID)) {
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(pipeID);
    }
    pipeIDSet.get(pipeID.getPipeFullName()).remove(pipeID);
  }

  public void deregisterPipe(final String pipeFullName) {
    if (!pipeIDSet.containsKey(pipeFullName)) {
      return;
    }
    if (Objects.nonNull(metricService)) {
      for (PipeID pipeID : pipeIDSet.get(pipeFullName)) {
        removeMetrics(pipeID);
      }
    }
    pipeIDSet.remove(pipeFullName);
  }

  public void markTabletCount(final PipeID pipeID, final long count) {
    LOGGER.info("Marking Pipe({}) ", pipeID);
    if (Objects.isNull(metricService)) {
      return;
    }
    final Map<String, Rate> tabletCountRates = tabletCountMap.get(pipeID.getPipeFullName());
    if (tabletCountRates == null) {
      LOGGER.info("Failed to mark tablet count, Pipe({}) does not exist", pipeID);
      return;
    }
    final Rate rate = tabletCountRates.get(pipeID.getCallerName());
    if (rate != null) {
      rate.mark(count);
    }
  }

  public void markTsfileSize(final PipeID pipeID, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Map<String, Rate> tsFileSizeRates = tsFileSizeMap.get(pipeID.getPipeFullName());
    if (tsFileSizeRates == null) {
      LOGGER.info("Failed to mark tsfile size, Pipe({}) does not exist", pipeID);
      return;
    }
    Rate rate = tsFileSizeRates.get(pipeID.getCallerName());
    if (rate != null) {
      rate.mark(size);
    }
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

  public static class PipeID {
    private final String pipeName;
    private final String creationTime;
    private final String callerName;

    public PipeID(String pipeName, String creationTime, String callerName) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.callerName = callerName;
    }

    public String getPipeName() {
      return pipeName;
    }

    public String getCreationTime() {
      return creationTime;
    }

    public String getCallerName() {
      return callerName;
    }

    public String getPipeFullName() {
      return pipeName + "_" + creationTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PipeID pipeID = (PipeID) o;
      return pipeName.equals(pipeID.pipeName)
          && creationTime.equals(pipeID.creationTime)
          && callerName.equals(pipeID.callerName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeName, creationTime, callerName);
    }

    @Override
    public String toString() {
      return pipeName + "_" + creationTime + "_" + callerName;
    }
  }
}
