package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.resource.wal.PipeWALResourceManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeResourceMetrics implements IMetricSet {

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PIPE_MEM_COST.toString(),
        MetricLevel.IMPORTANT,
        PipeResourceManager.memory(),
        PipeMemoryManager::getUsedMemorySizeInBytes);
    metricService.createAutoGauge(
        Metric.PIPE_MEM_USAGE.toString(),
        MetricLevel.IMPORTANT,
        PipeResourceManager.memory(),
        PipeMemoryManager::getMemoryUsage);
    metricService.createAutoGauge(
        Metric.PINNED_WAL_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeResourceManager.wal(),
        PipeWALResourceManager::getPinnedWalCount);
    metricService.createAutoGauge(
        Metric.LINKED_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeResourceManager.tsfile(),
        PipeTsFileResourceManager::getLinkedTsfileCount);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_MEM_COST.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_MEM_USAGE.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PINNED_WAL_COUNT.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.LINKED_TSFILE_COUNT.toString());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeResourceMetricsHolder {

    private static final PipeResourceMetrics INSTANCE = new PipeResourceMetrics();

    private PipeResourceMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeResourceMetrics getInstance() {
    return PipeResourceMetrics.PipeResourceMetricsHolder.INSTANCE;
  }

  private PipeResourceMetrics() {
    // empty constructor
  }
}
