package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.core.IoTDBMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeDataNodeReceiverMetrics implements IMetricSet {

  private static final PipeDataNodeReceiverMetrics INSTANCE = new PipeDataNodeReceiverMetrics();

  private Timer handshakeDatanodeV1Timer = IoTDBMetricManager.getInstance().createTimer();
  private Timer handshakeDatanodeV2Timer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTabletInsertNodeTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTabletRawTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTabletBinaryTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTabletBatchTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTsFilePieceTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTsFileSealTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTsFilePieceWithModTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferTsFileSealWithModTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferSchemaPlanTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferSchemaSnapshotPieceTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferSchemaSnapshotSealTimer = IoTDBMetricManager.getInstance().createTimer();

  private static final String RECEIVER = "pipeDataNodeReceiver";

  private PipeDataNodeReceiverMetrics(){}

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
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unBindTimer(metricService);
  }

  private void unBindTimer(AbstractMetricService metricService) {
    handshakeDatanodeV1Timer = IoTDBMetricManager.getInstance().createTimer();
    handshakeDatanodeV2Timer = IoTDBMetricManager.getInstance().createTimer();
    transferTabletInsertNodeTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTabletRawTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTabletBinaryTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTabletBatchTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTsFilePieceTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTsFileSealTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTsFilePieceWithModTimer = IoTDBMetricManager.getInstance().createTimer();
    transferTsFileSealWithModTimer = IoTDBMetricManager.getInstance().createTimer();
    transferSchemaPlanTimer = IoTDBMetricManager.getInstance().createTimer();
    transferSchemaSnapshotPieceTimer = IoTDBMetricManager.getInstance().createTimer();
    transferSchemaSnapshotSealTimer = IoTDBMetricManager.getInstance().createTimer();

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
  }

  public static PipeDataNodeReceiverMetrics getInstance() {
    return INSTANCE;
  }
}
