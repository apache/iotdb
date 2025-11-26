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

package org.apache.iotdb.db.pipe.metric.receiver;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeDataNodeReceiverMetrics implements IMetricSet {

  private static final PipeDataNodeReceiverMetrics INSTANCE = new PipeDataNodeReceiverMetrics();

  private Timer handshakeDatanodeV1Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer handshakeDatanodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletInsertNodeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletInsertNodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletRawTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletRawV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletBinaryTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletBinaryV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletBatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer transferTabletBatchV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
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

  private Rate handshakeDatanodeV1Rate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate handshakeDatanodeV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletInsertNodeRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletInsertNodeV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletRawRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletRawV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletBinaryRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletBinaryV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletBatchRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTabletBatchV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTsFilePieceRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTsFileSealRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTsFilePieceWithModRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferTsFileSealWithModRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferSchemaPlanRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferSchemaSnapshotPieceRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferSchemaSnapshotSealRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferConfigPlanRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferCompressedRate = DoNothingMetricManager.DO_NOTHING_RATE;
  private Rate transferSliceRate = DoNothingMetricManager.DO_NOTHING_RATE;

  private static final String RECEIVER = "pipeDataNodeReceiver";

  private PipeDataNodeReceiverMetrics() {}

  public void recordHandshakeDatanodeV1Timer(final long costTimeInNanos) {
    handshakeDatanodeV1Timer.updateNanos(costTimeInNanos);
  }

  public void recordHandshakeDatanodeV2Timer(final long costTimeInNanos) {
    handshakeDatanodeV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletInsertNodeTimer(final long costTimeInNanos) {
    transferTabletInsertNodeTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletInsertNodeV2Timer(final long costTimeInNanos) {
    transferTabletInsertNodeV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletRawTimer(final long costTimeInNanos) {
    transferTabletRawTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletRawV2Timer(final long costTimeInNanos) {
    transferTabletRawV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletBinaryTimer(final long costTimeInNanos) {
    transferTabletBinaryTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletBinaryV2Timer(final long costTimeInNanos) {
    transferTabletBinaryV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletBatchTimer(final long costTimeInNanos) {
    transferTabletBatchTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTabletBatchV2Timer(final long costTimeInNanos) {
    transferTabletBatchV2Timer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFilePieceTimer(final long costTimeInNanos) {
    transferTsFilePieceTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFileSealTimer(final long costTimeInNanos) {
    transferTsFileSealTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFilePieceWithModTimer(final long costTimeInNanos) {
    transferTsFilePieceWithModTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferTsFileSealWithModTimer(final long costTimeInNanos) {
    transferTsFileSealWithModTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSchemaPlanTimer(final long costTimeInNanos) {
    transferSchemaPlanTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSchemaSnapshotPieceTimer(final long costTimeInNanos) {
    transferSchemaSnapshotPieceTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSchemaSnapshotSealTimer(final long costTimeInNanos) {
    transferSchemaSnapshotSealTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferConfigPlanTimer(final long costTimeInNanos) {
    transferConfigPlanTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferCompressedTimer(final long costTimeInNanos) {
    transferCompressedTimer.updateNanos(costTimeInNanos);
  }

  public void recordTransferSliceTimer(final long costTimeInNanos) {
    transferSliceTimer.updateNanos(costTimeInNanos);
  }

  public void markHandshakeDatanodeV1Size(final long reqBytes) {
    handshakeDatanodeV1Rate.mark(reqBytes);
  }

  public void markHandshakeDatanodeV2Size(final long reqBytes) {
    handshakeDatanodeV2Rate.mark(reqBytes);
  }

  public void markTransferTabletInsertNodeSize(final long reqBytes) {
    transferTabletInsertNodeRate.mark(reqBytes);
  }

  public void markTransferTabletInsertNodeV2Size(final long reqBytes) {
    transferTabletInsertNodeV2Rate.mark(reqBytes);
  }

  public void markTransferTabletRawSize(final long reqBytes) {
    transferTabletRawRate.mark(reqBytes);
  }

  public void markTransferTabletRawV2Size(final long reqBytes) {
    transferTabletRawV2Rate.mark(reqBytes);
  }

  public void markTransferTabletBinarySize(final long reqBytes) {
    transferTabletBinaryRate.mark(reqBytes);
  }

  public void markTransferTabletBinaryV2Size(final long reqBytes) {
    transferTabletBinaryV2Rate.mark(reqBytes);
  }

  public void markTransferTabletBatchSize(final long reqBytes) {
    transferTabletBatchRate.mark(reqBytes);
  }

  public void markTransferTabletBatchV2Size(final long reqBytes) {
    transferTabletBatchV2Rate.mark(reqBytes);
  }

  public void markTransferTsFilePieceSize(final long reqBytes) {
    transferTsFilePieceRate.mark(reqBytes);
  }

  public void markTransferTsFileSealSize(final long reqBytes) {
    transferTsFileSealRate.mark(reqBytes);
  }

  public void markTransferTsFilePieceWithModSize(final long reqBytes) {
    transferTsFilePieceWithModRate.mark(reqBytes);
  }

  public void markTransferTsFileSealWithModSize(final long reqBytes) {
    transferTsFileSealWithModRate.mark(reqBytes);
  }

  public void markTransferSchemaPlanSize(final long reqBytes) {
    transferSchemaPlanRate.mark(reqBytes);
  }

  public void markTransferSchemaSnapshotPieceSize(final long reqBytes) {
    transferSchemaSnapshotPieceRate.mark(reqBytes);
  }

  public void markTransferSchemaSnapshotSealSize(final long reqBytes) {
    transferSchemaSnapshotSealRate.mark(reqBytes);
  }

  public void markTransferConfigPlanSize(final long reqBytes) {
    transferConfigPlanRate.mark(reqBytes);
  }

  public void markTransferCompressedSize(final long reqBytes) {
    transferCompressedRate.mark(reqBytes);
  }

  public void markTransferSliceSize(final long reqBytes) {
    transferSliceRate.mark(reqBytes);
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    bindToTimer(metricService);
  }

  private void bindToTimer(final AbstractMetricService metricService) {
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
    transferTabletInsertNodeV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletInsertNodeV2");
    transferTabletRawTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletRaw");
    transferTabletRawV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletRawV2");
    transferTabletBinaryTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBinary");
    transferTabletBinaryV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBinaryV2");
    transferTabletBatchTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBatch");
    transferTabletBatchV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_DATANODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBatchV2");
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

    // Rate
    handshakeDatanodeV1Rate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handshakeDataNodeV1");
    handshakeDatanodeV2Rate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handshakeDataNodeV2");
    transferTabletInsertNodeRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletInsertNode");
    transferTabletInsertNodeV2Rate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletInsertNodeV2");
    transferTabletRawRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletRaw");
    transferTabletRawV2Rate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletRawV2");
    transferTabletBinaryRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBinary");
    transferTabletBinaryV2Rate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBinaryV2");
    transferTabletBatchRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBatch");
    transferTabletBatchV2Rate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTabletBatchV2");
    transferTsFilePieceRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFilePiece");
    transferTsFileSealRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFileSeal");
    transferTsFilePieceWithModRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFilePieceWithMod");
    transferTsFileSealWithModRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferTsFileSealWithMod");
    transferSchemaPlanRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSchemaPlan");
    transferSchemaSnapshotPieceRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSchemaSnapshotPiece");
    transferSchemaSnapshotSealRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSchemaSnapshotSeal");
    transferConfigPlanRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferConfigPlan");
    transferCompressedRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferCompressed");
    transferSliceRate =
        metricService.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "transferSlice");
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    unbind(metricService);
  }

  private void unbind(final AbstractMetricService metricService) {
    handshakeDatanodeV1Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    handshakeDatanodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletInsertNodeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletInsertNodeV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletRawTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletRawV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletBinaryTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletBinaryV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletBatchTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    transferTabletBatchV2Timer = DoNothingMetricManager.DO_NOTHING_TIMER;
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

    handshakeDatanodeV1Rate = DoNothingMetricManager.DO_NOTHING_RATE;
    handshakeDatanodeV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletInsertNodeRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletInsertNodeV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletRawRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletRawV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletBinaryRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletBinaryV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletBatchRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTabletBatchV2Rate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTsFilePieceRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTsFileSealRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTsFilePieceWithModRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferTsFileSealWithModRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferSchemaPlanRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferSchemaSnapshotPieceRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferSchemaSnapshotSealRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferConfigPlanRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferCompressedRate = DoNothingMetricManager.DO_NOTHING_RATE;
    transferSliceRate = DoNothingMetricManager.DO_NOTHING_RATE;

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
        "transferTabletInsertNodeV2");
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
        "transferTabletRawV2");
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
        "transferTabletBinaryV2");
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
        "transferTabletBatchV2");
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

    // Rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handshakeDatanodeV1");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handshakeDatanodeV2");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletInsertNode");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletInsertNodeV2");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletRaw");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletRawV2");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletBinary");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletBinaryV2");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletBatch");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTabletBatchV2");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFilePiece");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFileSeal");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFilePieceWithMod");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferTsFileSealWithMod");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSchemaPlan");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSchemaSnapshotPiece");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSchemaSnapshotSeal");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferConfigPlan");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferCompressed");
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "transferSlice");
  }

  public static PipeDataNodeReceiverMetrics getInstance() {
    return INSTANCE;
  }
}
