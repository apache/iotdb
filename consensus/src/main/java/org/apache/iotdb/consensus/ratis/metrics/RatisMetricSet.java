package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;

import org.apache.ratis.metrics.MetricRegistries;

public class RatisMetricSet implements IMetricSet {
  @Override
  public void bindTo(AbstractMetricService metricService) {
    MetricRegistries.global();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {}
}
