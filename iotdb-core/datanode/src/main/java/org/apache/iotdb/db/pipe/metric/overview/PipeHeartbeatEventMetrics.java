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
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeHeartbeatEventMetrics implements IMetricSet {

  private Timer publishedToAssignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Timer assignedToProcessedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Timer processedToTransferredTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindStageTimer(metricService);
  }

  private void bindStageTimer(AbstractMetricService metricService) {
    publishedToAssignedTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_HEARTBEAT_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            "publishedToAssigned");
    assignedToProcessedTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_HEARTBEAT_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            "assignedToProcessed");
    processedToTransferredTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_HEARTBEAT_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            "processedToTransferred");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindStageTimer(metricService);
  }

  private void unbindStageTimer(AbstractMetricService metricService) {
    publishedToAssignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    assignedToProcessedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    processedToTransferredTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_HEARTBEAT_EVENT.toString(),
        Tag.STAGE.toString(),
        "publishedToAssigned");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_HEARTBEAT_EVENT.toString(),
        Tag.STAGE.toString(),
        "assignedToProcessed");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_HEARTBEAT_EVENT.toString(),
        Tag.STAGE.toString(),
        "processedToTransferred");
  }

  //////////////////////////// pipe integration ////////////////////////////

  public void recordPublishedToAssignedTime(long costTimeInMillis) {
    publishedToAssignedTimer.updateMillis(costTimeInMillis);
  }

  public void recordAssignedToProcessedTime(long costTimeInMillis) {
    assignedToProcessedTimer.updateMillis(costTimeInMillis);
  }

  public void recordProcessedToTransferredTime(long costTimeInMillis) {
    processedToTransferredTimer.updateMillis(costTimeInMillis);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeHeartbeatEventMetricsHolder {

    private static final PipeHeartbeatEventMetrics INSTANCE = new PipeHeartbeatEventMetrics();

    private PipeHeartbeatEventMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeHeartbeatEventMetrics getInstance() {
    return PipeHeartbeatEventMetrics.PipeHeartbeatEventMetricsHolder.INSTANCE;
  }

  private PipeHeartbeatEventMetrics() {
    // empty constructor
  }
}
