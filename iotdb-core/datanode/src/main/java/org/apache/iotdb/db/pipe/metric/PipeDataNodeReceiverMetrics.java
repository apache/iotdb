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
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeDataNodeReceiverMetrics implements IMetricSet {

  private static final PipeDataNodeReceiverMetrics INSTANCE = new PipeDataNodeReceiverMetrics();

  private Timer handshakeDatanodeV1Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer handshakeDatanodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletInsertNodeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletRawTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletBinaryTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletBatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTsFilePieceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTsFileSealTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTsFilePieceWithModTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTsFileSealWithModTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferSchemaPlanTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferSchemaSnapshotPieceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferSchemaSnapshotSealTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferConfigPlanTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferCompressedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferSliceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private static final String RECEIVER = "pipeDataNodeReceiver";

  private PipeDataNodeReceiverMetrics() {}

  public void recordHandshakeDatanodeV1Timer(long costTimeInNanos) {
    handshakeDatanodeV1Timer.updateNanos(costTimeInNanos);
  }

  public void recordHandshakeDatanodeV2Timer(long costTimeInNanos) {
    handshakeDatanodeV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletInsertNodeTimer(long costTimeInNanos) {
    transferTabletInsertNodeTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletRawTimer(long costTimeInNanos) {
    transferTabletRawTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletBinaryTimer(long costTimeInNanos) {
    transferTabletBinaryTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletBatchTimer(long costTimeInNanos) {
    transferTabletBatchTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFilePieceTimer(long costTimeInNanos) {
    transferTsFilePieceTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFileSealTimer(long costTimeInNanos) {
    transferTsFileSealTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFilePieceWithModTimer(long costTimeInNanos) {
    transferTsFilePieceWithModTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFileSealWithModTimer(long costTimeInNanos) {
    transferTsFileSealWithModTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSchemaPlanTimer(long costTimeInNanos) {
    transferSchemaPlanTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSchemaSnapshotPieceTimer(long costTimeInNanos) {
    transferSchemaSnapshotPieceTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSchemaSnapshotSealTimer(long costTimeInNanos) {
    transferSchemaSnapshotSealTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferConfigPlanTimer(long costTimeInNanos) {
    transferConfigPlanTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferCompressedTimer(long costTimeInNanos) {
    transferCompressedTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSliceTimer(long costTimeInNanos) {
    transferSliceTimer.updateNanos(costTimeInNanos);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindToTimer(metricService);
  }

  private void bindToTimer(AbstractMetricService metricService) {
    handshakeDatanodeV1Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handshakeDataNodeV1");
    handshakeDatanodeV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handshakeDataNodeV2");
    transferTabletInsertNodeTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletInsertNode");
    transferTabletRawTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletRaw");
    transferTabletBinaryTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBinary");
    transferTabletBatchTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBatch");
    transferTsFilePieceTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFilePiece");
    transferTsFileSealTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFileSeal");
    transferTsFilePieceWithModTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFilePieceWithMod");
    transferTsFileSealWithModTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFileSealWithMod");
    transferSchemaPlanTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSchemaPlan");
    transferSchemaSnapshotPieceTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSchemaSnapshotPiece");
    transferSchemaSnapshotSealTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSchemaSnapshotSeal");
    transferConfigPlanTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferConfigPlan");
    transferCompressedTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferCompressed");
    transferSliceTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSlice");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbind(metricService);
  }

  private void unbind(AbstractMetricService metricService) {
    handshakeDatanodeV1Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    handshakeDatanodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletInsertNodeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletRawTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletBinaryTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletBatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTsFilePieceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTsFileSealTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTsFilePieceWithModTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTsFileSealWithModTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferSchemaPlanTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferSchemaSnapshotPieceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferSchemaSnapshotSealTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferConfigPlanTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferCompressedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferSliceTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handshakeDatanodeV1");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handshakeDatanodeV2");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletInsertNode");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletRaw");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletBinary");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletBatch");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFilePiece");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFileSeal");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFilePieceWithMod");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFileSealWithMod");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSchemaPlan");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSchemaSnapshotPiece");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSchemaSnapshotSeal");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferConfigPlan");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferCompressed");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_DATANODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSlice");
  }

  public static PipeDataNodeReceiverMetrics getInstance() {
    return INSTANCE;
  }
}
