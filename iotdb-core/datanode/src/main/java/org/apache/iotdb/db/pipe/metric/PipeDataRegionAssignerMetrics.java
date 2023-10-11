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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeDataRegionAssignerMetrics implements IMetricSet {

  private static final String PIPE_DATA_REGION_ASSIGNER = "PipeDataRegionAssigner";

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

    private static final PipeDataRegionAssignerMetrics INSTANCE =
        new PipeDataRegionAssignerMetrics();

    private PipeDataRegionAssignerMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeDataRegionAssignerMetrics getInstance() {
    return PipeDataRegionAssignerMetrics.PipeDataRegionAssignerMetricsHolder.INSTANCE;
  }

  private PipeDataRegionAssignerMetrics() {
    // empty constructor
  }

  public void register(PipeDataRegionAssigner pipeDataRegionAssigner) {
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
        Metric.PIPE_HEARTBEAT_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getPipeHeartbeatEventCount,
        Tag.NAME.toString(),
        dataRegionId,
        Tag.FROM.toString(),
        PIPE_DATA_REGION_ASSIGNER);
    metricService.createAutoGauge(
        Metric.TABLET_INSERTION_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        dataRegionId,
        Tag.FROM.toString(),
        PIPE_DATA_REGION_ASSIGNER);
    metricService.createAutoGauge(
        Metric.TS_FILE_INSERTION_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        assignerMap.get(dataRegionId),
        PipeDataRegionAssigner::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        dataRegionId,
        Tag.FROM.toString(),
        PIPE_DATA_REGION_ASSIGNER);
  }
}
