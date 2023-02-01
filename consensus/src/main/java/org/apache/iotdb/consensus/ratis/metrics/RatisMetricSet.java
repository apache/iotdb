package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;

import org.apache.ratis.metrics.MetricRegistries;

public class RatisMetricSet implements IMetricSet {
  private MetricRegistries manager;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    manager = MetricRegistries.global();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    manager.clear();
  }
}
