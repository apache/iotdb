package org.apache.iotdb.metrics;

public interface MetricReporter {
  boolean start();
  void setMetricFactory(MetricFactory metricFactory);
  boolean stop();
}
