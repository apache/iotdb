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

package org.apache.iotdb.db.pipe.metric.source;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeAssignerMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeAssignerMetrics.class);

  private AbstractMetricService metricService;

  private final Map<Integer, PipeDataRegionAssigner> assignerMap = new HashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (Integer dataRegionId : assignerMap.keySet()) {
        createMetrics(dataRegionId);
      }
    }
  }

  private void createMetrics(int dataRegionId) {
    createAutoGauge(dataRegionId);
  }

  private void createAutoGauge(int dataRegionId) {
    metricService.createAutoGauge(
        Metric.UNASSIGNED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getPipeHeartbeatEventCount,
        Tag.REGION.toString(),
        Integer.toString(dataRegionId));
    metricService.createAutoGauge(
        Metric.UNASSIGNED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getTabletInsertionEventCount,
        Tag.REGION.toString(),
        Integer.toString(dataRegionId));
    metricService.createAutoGauge(
        Metric.UNASSIGNED_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getTsFileInsertionEventCount,
        Tag.REGION.toString(),
        Integer.toString(dataRegionId));
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    ImmutableSet<Integer> dataRegionIds = ImmutableSet.copyOf(assignerMap.keySet());
    for (int dataRegionId : dataRegionIds) {
      deregister(dataRegionId);
    }
    if (!assignerMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe assigner metrics, assigner map not empty");
    }
  }

  private void removeMetrics(int dataRegionId) {
    removeAutoGauge(dataRegionId);
  }

  private void removeAutoGauge(int dataRegionId) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNASSIGNED_HEARTBEAT_COUNT.toString(),
        Tag.REGION.toString(),
        Integer.toString(dataRegionId));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNASSIGNED_TABLET_COUNT.toString(),
        Tag.REGION.toString(),
        Integer.toString(dataRegionId));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.UNASSIGNED_TSFILE_COUNT.toString(),
        Tag.REGION.toString(),
        Integer.toString(dataRegionId));
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(PipeDataRegionAssigner pipeDataRegionAssigner) {
    int dataRegionId = pipeDataRegionAssigner.getDataRegionId();
    synchronized (this) {
      assignerMap.putIfAbsent(dataRegionId, pipeDataRegionAssigner);
      if (Objects.nonNull(metricService)) {
        createMetrics(dataRegionId);
      }
    }
  }

  public void deregister(int dataRegionId) {
    synchronized (this) {
      if (!assignerMap.containsKey(dataRegionId)) {
        LOGGER.warn(
            "Failed to deregister pipe assigner metrics, PipeDataRegionAssigner({}) does not exist",
            dataRegionId);
        return;
      }
      if (Objects.nonNull(metricService)) {
        removeMetrics(dataRegionId);
      }
      assignerMap.remove(dataRegionId);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeAssignerMetricsHolder {

    private static final PipeAssignerMetrics INSTANCE = new PipeAssignerMetrics();

    private PipeAssignerMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeAssignerMetrics getInstance() {
    return PipeAssignerMetrics.PipeAssignerMetricsHolder.INSTANCE;
  }

  private PipeAssignerMetrics() {
    // empty constructor
  }
}
