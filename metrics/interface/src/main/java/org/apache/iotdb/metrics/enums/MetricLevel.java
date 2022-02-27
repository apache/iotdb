package org.apache.iotdb.metrics.enums;

public enum MetricLevel {
  all(0),
  normal(1),
  important(2),
  core(3);

  /** Level of metric */
  int level;

  MetricLevel(int level) {
    this.level = level;
  }

  public static boolean isHigher(MetricLevel level1, MetricLevel level2) {
    return level1.level > level2.level;
  }
}
