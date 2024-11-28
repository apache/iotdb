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

public class PipeTsFileToTabletMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileToTabletMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, Set<PipeID>> pipeIDMap = new ConcurrentHashMap<>();
  private final Map<PipeID, Rate> tsFileSizeMap = new ConcurrentHashMap<>();
  private final Map<PipeID, Rate> tabletCountMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> pipeFullNames = ImmutableSet.copyOf(pipeIDMap.keySet());
    for (final String pipeFullName : pipeFullNames) {
      for (final PipeID pipeID : pipeIDMap.get(pipeFullName)) {
        createMetrics(pipeID);
      }
    }
  }

  private void createMetrics(final PipeID pipeID) {
    createRate(pipeID);
  }

  private void createRate(final PipeID pipeID) {
    tsFileSizeMap.put(
        pipeID,
        metricService.getOrCreateRate(
            Metric.PIPE_TSFILETOTABLET_TSFILE_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeID.getPipeName(),
            Tag.CREATION_TIME.toString(),
            pipeID.getCreationTime(),
            Tag.FROM.toString(),
            pipeID.getCallerName()));
    tabletCountMap.put(
        pipeID,
        metricService.getOrCreateRate(
            Metric.PIPE_TSFILETOTABLET_TABLET_COUNT.toString(),
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
    final ImmutableSet<String> pipeFullNames = ImmutableSet.copyOf(pipeIDMap.keySet());
    for (final String pipeFullName : pipeFullNames) {
      for (final PipeID pipeID : pipeIDMap.get(pipeFullName)) {
        removeMetrics(pipeID);
      }
      pipeIDMap.remove((pipeFullName));
    }
    if (!pipeIDMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipeTsFileToTablet metrics,  pipeIDMap not empty");
    }
  }

  private void removeMetrics(final PipeID pipeID) {
    removeRate(pipeID);
  }

  private void removeRate(final PipeID pipeID) {
    tsFileSizeMap.remove(pipeID);
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TSFILETOTABLET_TSFILE_SIZE.toString(),
        Tag.NAME.toString(),
        pipeID.getPipeName(),
        Tag.CREATION_TIME.toString(),
        pipeID.getCreationTime(),
        Tag.FROM.toString(),
        pipeID.getCallerName());
    tabletCountMap.remove(pipeID);
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TSFILETOTABLET_TABLET_COUNT.toString(),
        Tag.NAME.toString(),
        pipeID.getPipeName(),
        Tag.CREATION_TIME.toString(),
        pipeID.getCreationTime(),
        Tag.FROM.toString(),
        pipeID.getCallerName());
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final PipeID pipeID) {
    pipeIDMap.putIfAbsent(
        pipeID.getPipeFullName(), Collections.newSetFromMap(new ConcurrentHashMap<>()));
    if (pipeIDMap.get(pipeID.getPipeFullName()).add(pipeID) && Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void deregister(final PipeID pipeID) {
    if (!pipeIDMap.containsKey(pipeID.getPipeFullName())
        || !pipeIDMap.get(pipeID.getPipeFullName()).contains(pipeID)) {
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(pipeID);
    }
    pipeIDMap.get(pipeID.getPipeFullName()).remove(pipeID);
  }

  public void deregisterPipe(final String pipeFullName) {
    if (!pipeIDMap.containsKey(pipeFullName)) {
      return;
    }
    if (Objects.nonNull(metricService)) {
      for (PipeID pipeID : pipeIDMap.get(pipeFullName)) {
        removeMetrics(pipeID);
      }
    }
    pipeIDMap.remove(pipeFullName);
  }

  public void markTsFileSize(final PipeID pipeID, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tsFileSizeMap.get(pipeID);
    if (rate == null) {
      LOGGER.info("Failed to mark pipe tsfile size, PipeID({}) does not exist", pipeID);
      return;
    }
    rate.mark(size);
  }

  public void markTabletCount(final PipeID pipeID, final long count) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tabletCountMap.get(pipeID);
    if (rate == null) {
      LOGGER.info("Failed to mark pipe tablet count, PipeID({}) does not exist", pipeID);
      return;
    }
    rate.mark(count);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTsFileToTabletMetricsHolder {

    private static final PipeTsFileToTabletMetrics INSTANCE = new PipeTsFileToTabletMetrics();

    private PipeTsFileToTabletMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeTsFileToTabletMetrics getInstance() {
    return PipeTsFileToTabletMetrics.PipeTsFileToTabletMetricsHolder.INSTANCE;
  }

  private PipeTsFileToTabletMetrics() {
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
    public String toString() {
      return pipeName + "_" + creationTime + "_" + callerName;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      PipeID pipeID = (PipeID) o;
      return Objects.equals(pipeName, pipeID.pipeName)
          && Objects.equals(creationTime, pipeID.creationTime)
          && Objects.equals(callerName, pipeID.callerName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeName, creationTime, callerName);
    }
  }
}
