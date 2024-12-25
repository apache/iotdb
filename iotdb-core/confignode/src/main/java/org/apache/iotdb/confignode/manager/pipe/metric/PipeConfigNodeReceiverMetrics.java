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

package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeConfigNodeReceiverMetrics implements IMetricSet {

  private static final PipeConfigNodeReceiverMetrics INSTANCE = new PipeConfigNodeReceiverMetrics();

  private Timer handshakeConfigNodeV1Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer handshakeConfigNodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferConfigPlanTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferConfigSnapshotPieceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferConfigSnapshotSealTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private static final String RECEIVER = "pipeConfigNodeReceiver";

  private PipeConfigNodeReceiverMetrics() {}

  public void recordHandshakeConfigNodeV1Timer(long costTimeInNanos) {
    handshakeConfigNodeV1Timer.updateNanos(costTimeInNanos);
  }

  public void recordHandshakeConfigNodeV2Timer(long costTimeInNanos) {
    handshakeConfigNodeV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferConfigPlanTimer(long costTimeInNanos) {
    transferConfigPlanTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferConfigSnapshotPieceTimer(long costTimeInNanos) {
    transferConfigSnapshotPieceTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferConfigSnapshotSealTimer(long costTimeInNanos) {
    transferConfigSnapshotSealTimer.updateNanos(costTimeInNanos);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindToTimer(metricService);
  }

  private void bindToTimer(AbstractMetricService metricService) {
    handshakeConfigNodeV1Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handshakeConfigNodeV1");

    handshakeConfigNodeV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handshakeConfigNodeV2");

    transferConfigPlanTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferConfigPlan");

    transferConfigSnapshotPieceTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferConfigSnapshotPiece");

    transferConfigSnapshotSealTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferConfigSnapshotSeal");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbind(metricService);
  }

  private void unbind(AbstractMetricService metricService) {
    handshakeConfigNodeV1Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    handshakeConfigNodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferConfigPlanTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferConfigSnapshotPieceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferConfigSnapshotSealTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handshakeConfigNodeV1");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handshakeConfigNodeV2");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferConfigPlan");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferConfigSnapshotPiece");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferConfigSnapshotSeal");
  }

  public static PipeConfigNodeReceiverMetrics getInstance() {
    return INSTANCE;
  }
}
