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
import org.apache.iotdb.db.pipe.receiver.protocol.pipeconsensus.PipeConsensusReceiver;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeConsensusReceiverMetrics implements IMetricSet {
  private final PipeConsensusReceiver pipeConsensusReceiver;

  private Timer tsFilePieceWriteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tsFilePiecePreCheckTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tsFileSealLoadTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tsFileSealPreCheckTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer borrowTsFileWriterTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer acquireExecutorLockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer dispatchWaitingTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer receiveWALTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer receiveTsFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer receiveEventTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private static final String RECEIVER = "pipeConsensusReceiver";

  public PipeConsensusReceiverMetrics(PipeConsensusReceiver pipeConsensusReceiver) {
    this.pipeConsensusReceiver = pipeConsensusReceiver;
  }

  public void recordTsFilePieceWriteTime(long costTimeInNanos) {
    tsFilePieceWriteTimer.updateNanos(costTimeInNanos);
  }

  public void recordTsFilePiecePreCheckTime(long costTimeInNanos) {
    tsFilePiecePreCheckTimer.updateNanos(costTimeInNanos);
  }

  public void recordTsFileSealLoadTimer(long costTimeInNanos) {
    tsFileSealLoadTimer.updateNanos(costTimeInNanos);
  }

  public void recordTsFileSealPreCheckTimer(long costTimeInNanos) {
    tsFileSealPreCheckTimer.updateNanos(costTimeInNanos);
  }

  public void recordBorrowTsFileWriterTimer(long costTimeInNanos) {
    borrowTsFileWriterTimer.updateNanos(costTimeInNanos);
  }

  public void recordAcquireExecutorLockTimer(long costTimeInNanos) {
    acquireExecutorLockTimer.updateNanos(costTimeInNanos);
  }

  public void recordDispatchWaitingTimer(long costTimeInNanos) {
    dispatchWaitingTimer.updateNanos(costTimeInNanos);
  }

  public void recordReceiveWALTimer(long costTimeInNanos) {
    receiveWALTimer.updateNanos(costTimeInNanos);
  }

  public void recordReceiveTsFileTimer(long costTimeInNanos) {
    receiveTsFileTimer.updateNanos(costTimeInNanos);
  }

  public void recordReceiveEventTimer(long costTimeInNanos) {
    receiveEventTimer.updateNanos(costTimeInNanos);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindAutoGauge(metricService);
    bindStageTimer(metricService);
    bindReceiveTimer(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindAutoGauge(metricService);
    unbindStageTimer(metricService);
    unbindReceiveTimer(metricService);
  }

  public void bindAutoGauge(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PIPE_RECEIVE_EVENT.toString(),
        MetricLevel.IMPORTANT,
        pipeConsensusReceiver,
        PipeConsensusReceiver::getReceiveBufferSize,
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "receiveBufferSize");
    metricService.createAutoGauge(
        Metric.PIPE_RECEIVE_EVENT.toString(),
        MetricLevel.IMPORTANT,
        pipeConsensusReceiver,
        PipeConsensusReceiver::getWALEventCount,
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "WALEventCount");
    metricService.createAutoGauge(
        Metric.PIPE_RECEIVE_EVENT.toString(),
        MetricLevel.IMPORTANT,
        pipeConsensusReceiver,
        PipeConsensusReceiver::getTsFileEventCount,
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "tsFileEventCount");
  }

  public void bindStageTimer(AbstractMetricService metricService) {
    tsFilePieceWriteTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "tsFilePieceWrite");
    tsFilePiecePreCheckTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "tsFilePiecePreCheck");
    tsFileSealLoadTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "tsFileSealLoad");
    tsFileSealPreCheckTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "tsFileSealPreCheck");
    borrowTsFileWriterTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "borrowTsFileWriter");
    acquireExecutorLockTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "acquireExecutorLock");
    dispatchWaitingTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "dispatchWaiting");
  }

  public void bindReceiveTimer(AbstractMetricService metricService) {
    receiveEventTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_RECEIVE_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "receiveEvent");
    receiveWALTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_RECEIVE_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "receiveWALEvent");
    receiveTsFileTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_RECEIVE_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.REGION.toString(),
            pipeConsensusReceiver.getConsensusGroupIdStr(),
            Tag.TYPE.toString(),
            "receiveTsFileEvent");
  }

  public void unbindAutoGauge(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "receiveBufferSize");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "WALEventCount");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "tsFileEventCount");
  }

  public void unbindStageTimer(AbstractMetricService metricService) {
    tsFilePieceWriteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    tsFilePiecePreCheckTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    tsFileSealLoadTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    tsFileSealPreCheckTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    borrowTsFileWriterTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    acquireExecutorLockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    dispatchWaitingTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "tsFilePieceWrite");
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "tsFilePiecePreCheck");
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "tsFileSealLoad");
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "tsFileSealPreCheck");
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "borrowTsFileWriter");
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "acquireExecutorLock");
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "dispatchWaiting");
  }

  public void unbindReceiveTimer(AbstractMetricService metricService) {
    receiveWALTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    receiveTsFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    receiveEventTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "receiveWALEvent");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "receiveTsFileEvent");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.REGION.toString(),
        pipeConsensusReceiver.getConsensusGroupIdStr(),
        Tag.TYPE.toString(),
        "receiveEvent");
  }
}
