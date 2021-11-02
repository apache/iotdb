package org.apache.iotdb.metrics;

import org.apache.iotdb.metrics.utils.ReporterType;

public interface Reporter {
  /** Start reporter */
  boolean start();

  /** Stop reporter */
  boolean stop();

  /** Get type of reporter */
  ReporterType getReporterType();

  /** Set metric manager */
  void setMetricManager(MetricManager metricManager);
}
