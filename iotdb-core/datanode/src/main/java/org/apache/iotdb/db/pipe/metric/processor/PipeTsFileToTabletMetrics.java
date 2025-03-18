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

package org.apache.iotdb.db.pipe.metric.processor;

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

  private final Map<String, Set<PipeCallerID>> pipeIDMap = new ConcurrentHashMap<>();
  private final Map<PipeCallerID, Rate> tsFileSizeMap = new ConcurrentHashMap<>();
  private final Map<PipeCallerID, Rate> tabletCountMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> pipeFullNames = ImmutableSet.copyOf(pipeIDMap.keySet());
    for (final String pipeFullName : pipeFullNames) {
      for (final PipeCallerID pipeCallerID : pipeIDMap.get(pipeFullName)) {
        createMetrics(pipeCallerID);
      }
    }
  }

  private void createMetrics(final PipeCallerID pipeCallerID) {
    createRate(pipeCallerID);
  }

  private void createRate(final PipeCallerID pipeCallerID) {
    tsFileSizeMap.put(
        pipeCallerID,
        metricService.getOrCreateRate(
            Metric.PIPE_TSFILE_TO_TABLET_TSFILE_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeCallerID.getPipeName(),
            Tag.CREATION_TIME.toString(),
            pipeCallerID.getCreationTime(),
            Tag.FROM.toString(),
            pipeCallerID.getCallerName()));
    tabletCountMap.put(
        pipeCallerID,
        metricService.getOrCreateRate(
            Metric.PIPE_TSFILE_TO_TABLET_TABLET_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            pipeCallerID.getPipeName(),
            Tag.CREATION_TIME.toString(),
            pipeCallerID.getCreationTime(),
            Tag.FROM.toString(),
            pipeCallerID.getCallerName()));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> pipeFullNames = ImmutableSet.copyOf(pipeIDMap.keySet());
    for (final String pipeFullName : pipeFullNames) {
      deregister(pipeFullName);
    }
    if (!pipeIDMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipeTsFileToTablet metrics,  pipeIDMap not empty");
    }
  }

  private void removeMetrics(final PipeCallerID pipeCallerID) {
    removeRate(pipeCallerID);
  }

  private void removeRate(final PipeCallerID pipeCallerID) {
    tsFileSizeMap.remove(pipeCallerID);
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TSFILE_TO_TABLET_TSFILE_SIZE.toString(),
        Tag.NAME.toString(),
        pipeCallerID.getPipeName(),
        Tag.CREATION_TIME.toString(),
        pipeCallerID.getCreationTime(),
        Tag.FROM.toString(),
        pipeCallerID.getCallerName());
    tabletCountMap.remove(pipeCallerID);
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_TSFILE_TO_TABLET_TABLET_COUNT.toString(),
        Tag.NAME.toString(),
        pipeCallerID.getPipeName(),
        Tag.CREATION_TIME.toString(),
        pipeCallerID.getCreationTime(),
        Tag.FROM.toString(),
        pipeCallerID.getCallerName());
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final PipeCallerID pipeCallerID) {
    pipeIDMap.putIfAbsent(
        pipeCallerID.getPipeFullName(), Collections.newSetFromMap(new ConcurrentHashMap<>()));
    if (pipeIDMap.get(pipeCallerID.getPipeFullName()).add(pipeCallerID)
        && Objects.nonNull(metricService)) {
      createMetrics(pipeCallerID);
    }
  }

  public void deregister(final String pipeFullName) {
    if (!pipeIDMap.containsKey(pipeFullName)) {
      return;
    }
    if (Objects.nonNull(metricService)) {
      for (PipeCallerID pipeCallerID : pipeIDMap.get(pipeFullName)) {
        removeMetrics(pipeCallerID);
      }
    }
    pipeIDMap.remove(pipeFullName);
  }

  public void markTsFileSize(final PipeCallerID pipeCallerID, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tsFileSizeMap.get(pipeCallerID);
    if (rate == null) {
      LOGGER.info("Failed to mark pipe tsfile size, PipeCallerID({}) does not exist", pipeCallerID);
      return;
    }
    rate.mark(size);
  }

  public void markTabletCount(final PipeCallerID pipeCallerID, final long count) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = tabletCountMap.get(pipeCallerID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe tablet count, PipeCallerID({}) does not exist", pipeCallerID);
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

  public static class PipeCallerID {
    private final String pipeName;
    private final String creationTime;
    private final String callerName;
    private final String pipeFullName;

    public PipeCallerID(String pipeName, String creationTime, String callerName) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.callerName = callerName;
      this.pipeFullName = callerName + "_" + creationTime;
    }

    public static PipeCallerID getPipeCallerID(final String pipeName, final long creationTime) {
      String callerClassName = Thread.currentThread().getStackTrace()[3].getClassName();
      String callerMethodName = Thread.currentThread().getStackTrace()[3].getMethodName();
      if (callerMethodName.equals("toTabletInsertionEvents")) {
        callerClassName = Thread.currentThread().getStackTrace()[4].getClassName();
        callerMethodName = Thread.currentThread().getStackTrace()[4].getMethodName();
      }
      return new PipeCallerID(
          pipeName,
          String.valueOf(creationTime),
          callerClassName.substring(callerClassName.lastIndexOf('.') + 1) + ":" + callerMethodName);
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
      return pipeFullName;
    }

    @Override
    public String toString() {
      return pipeName + "_" + creationTime + "_" + callerName;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      PipeCallerID pipeCallerID = (PipeCallerID) o;
      return Objects.equals(pipeName, pipeCallerID.pipeName)
          && Objects.equals(creationTime, pipeCallerID.creationTime)
          && Objects.equals(callerName, pipeCallerID.callerName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeName, creationTime, callerName);
    }
  }
}
