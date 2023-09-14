/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class LogDispatcherThreadMetrics implements IMetricSet {
  private final LogDispatcher.LogDispatcherThread logDispatcherThread;
  private final String peerGroupId;

  private Timer constructBatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer syncLogTimePerRequestTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public LogDispatcherThreadMetrics(LogDispatcher.LogDispatcherThread logDispatcherThread) {
    this.logDispatcherThread = logDispatcherThread;
    this.peerGroupId = logDispatcherThread.getPeer().getGroupId().toString();
  }

  public void recordConstructBatchTime(long costTimeInNanos) {
    constructBatchTimer.updateNanos(costTimeInNanos);
  }

  public void recordSyncLogTimePerRequest(long costTimeInNanos) {
    syncLogTimePerRequestTimer.updateNanos(costTimeInNanos);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindAutoGauge(metricService);
    bindStageTimer(metricService);
  }

  private void bindAutoGauge(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        logDispatcherThread,
        LogDispatcher.LogDispatcherThread::getCurrentSyncIndex,
        Tag.NAME.toString(),
        formatName(),
        Tag.REGION.toString(),
        logDispatcherThread.getPeer().getGroupId().toString(),
        Tag.TYPE.toString(),
        "currentSyncIndex");
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        logDispatcherThread,
        x -> x.getSyncStatus().getPendingBatches().size(),
        Tag.NAME.toString(),
        formatName(),
        Tag.REGION.toString(),
        logDispatcherThread.getPeer().getGroupId().toString(),
        Tag.TYPE.toString(),
        "pipelineNum");
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        logDispatcherThread,
        x -> x.getPendingEntriesSize() + x.getBufferRequestSize(),
        Tag.NAME.toString(),
        formatName(),
        Tag.REGION.toString(),
        logDispatcherThread.getPeer().getGroupId().toString(),
        Tag.TYPE.toString(),
        "cachedRequestInMemoryQueue");
  }

  private void bindStageTimer(AbstractMetricService metricService) {
    constructBatchTimer =
        metricService.getOrCreateTimer(
            Metric.IOT_SEND_LOG.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.STAGE.toString(),
            "constructBatch",
            Tag.REGION.toString(),
            peerGroupId);
    syncLogTimePerRequestTimer =
        metricService.getOrCreateTimer(
            Metric.IOT_SEND_LOG.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.STAGE.toString(),
            "syncLogTimePerRequest",
            Tag.REGION.toString(),
            peerGroupId);
  }

  private void unbindStageTimer(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.TIMER,
        Metric.IOT_SEND_LOG.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.STAGE.toString(),
        "constructBatch",
        Tag.REGION.toString(),
        peerGroupId);
    metricService.remove(
        MetricType.TIMER,
        Metric.IOT_SEND_LOG.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.STAGE.toString(),
        "syncLogTimePerRequest",
        Tag.REGION.toString(),
        peerGroupId);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindAutoGauge(metricService);
    unbindStageTimer(metricService);
  }

  private void unbindAutoGauge(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        formatName(),
        Tag.REGION.toString(),
        logDispatcherThread.getPeer().getGroupId().toString(),
        Tag.TYPE.toString(),
        "currentSyncIndex");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        formatName(),
        Tag.REGION.toString(),
        logDispatcherThread.getPeer().getGroupId().toString(),
        Tag.TYPE.toString(),
        "pipelineNum");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        formatName(),
        Tag.REGION.toString(),
        logDispatcherThread.getPeer().getGroupId().toString(),
        Tag.TYPE.toString(),
        "cachedRequestInMemoryQueue");
  }

  private String formatName() {
    return String.format(
        "logDispatcher-%s:%s",
        logDispatcherThread.getPeer().getEndpoint().getIp(),
        logDispatcherThread.getPeer().getEndpoint().getPort());
  }
}
