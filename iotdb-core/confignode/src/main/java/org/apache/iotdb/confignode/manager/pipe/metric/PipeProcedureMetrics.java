package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.db.pipe.metric.PipeProcessorMetrics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipeProcedureMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorMetrics.class);

  private final Map<String, Timer> timerMap = new HashMap<>();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    Arrays.stream(PipeTaskOperation.values())
        .forEach(
            op ->
                timerMap.put(
                    op.getName(),
                    metricService.getOrCreateTimer(
                        Metric.PIPE_PROCEDURE.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        op.getName())));
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    timerMap.forEach(
        (name, timer) ->
            metricService.remove(
                MetricType.TIMER, Metric.PIPE_PROCEDURE.toString(), Tag.NAME.toString(), name));
  }

  public void updateTimer(String name, long durationMillis) {
    Timer timer = timerMap.get(name);
    if (timer == null) {
      LOGGER.warn("Failed to update pipe procedure timer, PipeProcedure({}) does not exist", name);
      return;
    }
    timer.updateMillis(durationMillis);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeProcedureMetricsHolder {

    private static final PipeProcedureMetrics INSTANCE = new PipeProcedureMetrics();

    private PipeProcedureMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeProcedureMetrics getInstance() {
    return PipeProcedureMetrics.PipeProcedureMetricsHolder.INSTANCE;
  }

  private PipeProcedureMetrics() {
    // empty constructor
  }
}
