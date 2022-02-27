package org.apache.iotdb.metrics.enums;

public enum MetricLevel {
  core(0),
  important(1),
  normal(2),
  all(3);

  /** Level of metric */
  int level;

  MetricLevel(int level) {
    this.level = level;
  }
}
