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

package org.apache.iotdb.db.pipe.consensus.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeConsensusConnectorMetrics implements IMetricSet {
  private final PipeConsensusAsyncConnector pipeConsensusAsyncConnector;

  private Timer connectorEnqueueTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer connectorWALTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer connectorTsFileTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer connectorTsFilePieceTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer retryWALTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer retryTsFileTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Counter retryCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private static final String CONNECTOR = "pipeConsensusAsyncConnector";

  public PipeConsensusConnectorMetrics(PipeConsensusAsyncConnector pipeConsensusAsyncConnector) {
    this.pipeConsensusAsyncConnector = pipeConsensusAsyncConnector;
  }

  public void recordConnectorEnqueueTimer(long costTimeInNanos) {
    connectorEnqueueTimer.updateNanos(costTimeInNanos);
  }

  public void recordConnectorWalTransferTimer(long costTimeInNanos) {
    connectorWALTransferTimer.updateNanos(costTimeInNanos);
  }

  public void recordConnectorTsFileTransferTimer(long costTimeInNanos) {
    connectorTsFileTransferTimer.updateNanos(costTimeInNanos);
  }

  public void recordConnectorTsFilePieceTransferTimer(long costTimeInNanos) {
    connectorTsFilePieceTransferTimer.updateNanos(costTimeInNanos);
  }

  public void recordRetryWALTransferTimer(long costTimeInNanos) {
    retryWALTransferTimer.updateNanos(costTimeInNanos);
  }

  public void recordRetryTsFileTransferTimer(long costTimeInNanos) {
    retryTsFileTransferTimer.updateNanos(costTimeInNanos);
  }

  public void recordRetryCounter() {
    retryCounter.inc();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindCounter(metricService);
    bindAutoGauge(metricService);
    bindTimer(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindCounter(metricService);
    unbindAutoGauge(metricService);
    unbindTimer(metricService);
  }

  private void bindCounter(AbstractMetricService metricService) {
    retryCounter =
        metricService.getOrCreateCounter(
            Metric.PIPE_RETRY_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "pipeConsensusRetryCount");
  }

  private void bindAutoGauge(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PIPE_SEND_EVENT.toString(),
        MetricLevel.IMPORTANT,
        pipeConsensusAsyncConnector,
        PipeConsensusAsyncConnector::getTransferBufferSize,
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "transferBufferSize");
    metricService.createAutoGauge(
        Metric.PIPE_SEND_EVENT.toString(),
        MetricLevel.IMPORTANT,
        pipeConsensusAsyncConnector,
        PipeConsensusAsyncConnector::getRetryBufferSize,
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "retryBufferSize");
  }

  private void bindTimer(AbstractMetricService metricService) {
    connectorEnqueueTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.TYPE.toString(),
            "connectorEnqueue",
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    connectorTsFilePieceTransferTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.TYPE.toString(),
            "connectorTsFilePieceTransfer",
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    connectorTsFileTransferTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.TYPE.toString(),
            "connectorTsFileTransfer",
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    connectorWALTransferTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.TYPE.toString(),
            "connectorWALTransfer",
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    retryWALTransferTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_RETRY_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.TYPE.toString(),
            "retryWALTransfer",
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    retryTsFileTransferTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_RETRY_SEND_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            CONNECTOR,
            Tag.TYPE.toString(),
            "retryTsFileTransfer",
            Tag.REGION.toString(),
            pipeConsensusAsyncConnector.getConsensusGroupIdStr());
  }

  private void unbindCounter(AbstractMetricService metricService) {
    retryCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.COUNTER,
        Metric.PIPE_RETRY_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "pipeConsensusRetryCount");
  }

  private void unbindAutoGauge(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "transferBufferSize");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "retryBufferSize");
  }

  private void unbindTimer(AbstractMetricService metricService) {
    connectorEnqueueTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    connectorWALTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    connectorTsFileTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    connectorTsFilePieceTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    retryWALTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    retryTsFileTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.TYPE.toString(),
        "connectorTsFileTransfer",
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.TYPE.toString(),
        "connectorTsFilePieceTransfer",
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.TYPE.toString(),
        "connectorTsFileTransfer",
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.TYPE.toString(),
        "connectorWALTransfer",
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_RETRY_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.TYPE.toString(),
        "retryWALTransfer",
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_RETRY_SEND_EVENT.toString(),
        Tag.NAME.toString(),
        CONNECTOR,
        Tag.TYPE.toString(),
        "retryTsFileTransfer",
        Tag.REGION.toString(),
        pipeConsensusAsyncConnector.getConsensusGroupIdStr());
  }
}
