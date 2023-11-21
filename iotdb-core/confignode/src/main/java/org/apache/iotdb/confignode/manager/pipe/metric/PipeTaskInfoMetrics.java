package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.pipe.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.task.PipeTaskCoordinator;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeTaskInfoMetrics implements IMetricSet {

  private final PipeManager pipeManager;

  private static final String RUNNING = "running";

  private static final String DROPPED = "dropped";

  private static final String USER_STOPPED = "userStopped";

  private static final String EXCEPTION_STOPPED = "exceptionStopped";

  public PipeTaskInfoMetrics(PipeManager pipeManager) {
    this.pipeManager = pipeManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    PipeTaskCoordinator coordinator = pipeManager.getPipeTaskCoordinator();
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::runningPipeCount,
        Tag.STATUS.toString(),
        RUNNING);
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::droppedPipeCount,
        Tag.STATUS.toString(),
        DROPPED);
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::userStoppedPipeCount,
        Tag.STATUS.toString(),
        USER_STOPPED);
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::exceptionStoppedPipeCount,
        Tag.STATUS.toString(),
        EXCEPTION_STOPPED);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_TASK_STATUS.toString(), Tag.STATUS.toString(), RUNNING);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_TASK_STATUS.toString(), Tag.STATUS.toString(), DROPPED);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_TASK_STATUS.toString(),
        Tag.STATUS.toString(),
        USER_STOPPED);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_TASK_STATUS.toString(),
        Tag.STATUS.toString(),
        EXCEPTION_STOPPED);
  }
}
