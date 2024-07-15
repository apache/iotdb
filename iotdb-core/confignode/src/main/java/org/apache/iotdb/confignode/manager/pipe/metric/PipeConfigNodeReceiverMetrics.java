package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.core.IoTDBMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeConfigNodeReceiverMetrics implements IMetricSet {

  private static final PipeConfigNodeReceiverMetrics INSTANCE = new PipeConfigNodeReceiverMetrics();

  private Timer handShakeConfigNodeV1Timer = IoTDBMetricManager.getInstance().createTimer();
  private Timer handShakeConfigNodeV2Timer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferConfigPlanTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferConfigSnapshotPieceTimer = IoTDBMetricManager.getInstance().createTimer();
  private Timer transferConfigSnapshotSealTimer = IoTDBMetricManager.getInstance().createTimer();

  private static final String RECEIVER = "pipeConfigNodeReceiver";

  private PipeConfigNodeReceiverMetrics() {}

  public void recordHandShakeConfigNodeV1Timer(long costTimeInNanos) {
    handShakeConfigNodeV1Timer.updateNanos(costTimeInNanos);
  }

  public void recordHandShakeConfigNodeV2Timer(long costTimeInNanos) {
    handShakeConfigNodeV2Timer.updateNanos(costTimeInNanos);
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
    handShakeConfigNodeV1Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handShakeConfigNodeV1");

    handShakeConfigNodeV2Timer =
        metricService.getOrCreateTimer(
            Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RECEIVER,
            Tag.TYPE.toString(),
            "handShakeConfigNodeV2");

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
    unBindToTimer(metricService);
  }

  private void unBindToTimer(AbstractMetricService metricService) {
    handShakeConfigNodeV1Timer = IoTDBMetricManager.getInstance().createTimer();
    handShakeConfigNodeV2Timer = IoTDBMetricManager.getInstance().createTimer();
    transferConfigPlanTimer = IoTDBMetricManager.getInstance().createTimer();
    transferConfigSnapshotPieceTimer = IoTDBMetricManager.getInstance().createTimer();
    transferConfigSnapshotSealTimer = IoTDBMetricManager.getInstance().createTimer();

    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handShakeConfigNodeV1");
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_CONFIGNODE_RECEIVER.toString(),
        Tag.NAME.toString(),
        RECEIVER,
        Tag.TYPE.toString(),
        "handShakeConfigNodeV2");
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
