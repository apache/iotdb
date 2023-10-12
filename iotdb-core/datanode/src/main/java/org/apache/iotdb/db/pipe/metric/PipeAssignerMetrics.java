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
import org.apache.iotdb.db.pipe.extractor.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeAssignerMetrics implements IMetricSet {

  private AbstractMetricService metricService;

  private final Map<String, PipeDataRegionAssigner> assignerMap = new HashMap<>();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String dataRegionId : assignerMap.keySet()) {
        createMetrics(dataRegionId);
      }
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing
  }

  private static class PipeDataRegionAssignerMetricsHolder {

    private static final PipeAssignerMetrics INSTANCE = new PipeAssignerMetrics();

    private PipeDataRegionAssignerMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeAssignerMetrics getInstance() {
    return PipeAssignerMetrics.PipeDataRegionAssignerMetricsHolder.INSTANCE;
  }

  private PipeAssignerMetrics() {
    // empty constructor
  }

  public void register(@NonNull PipeDataRegionAssigner pipeDataRegionAssigner) {
    String dataRegionId = pipeDataRegionAssigner.getDataRegionId();
    synchronized (this) {
      if (!assignerMap.containsKey(dataRegionId)) {
        assignerMap.put(dataRegionId, pipeDataRegionAssigner);
      }
      if (Objects.nonNull(metricService)) {
        createMetrics(dataRegionId);
      }
    }
  }

  private void createMetrics(String dataRegionId) {
    metricService.createAutoGauge(
        Metric.UNASSIGNED_HEARTBEAT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getPipeHeartbeatEventCount,
        Tag.REGION.toString(),
        dataRegionId);
    metricService.createAutoGauge(
        Metric.UNASSIGNED_TABLET_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getTabletInsertionEventCount,
        Tag.REGION.toString(),
        dataRegionId);
    metricService.createAutoGauge(
        Metric.UNASSIGNED_TS_FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getTsFileInsertionEventCount,
        Tag.REGION.toString(),
        dataRegionId);
  }
}
