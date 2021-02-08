package org.apache.iotdb.metrics;

public interface MetricReporter {
  boolean start();
  boolean stop();
}
