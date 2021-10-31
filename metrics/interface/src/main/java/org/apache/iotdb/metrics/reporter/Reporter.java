package org.apache.iotdb.metrics.reporter;

import org.apache.iotdb.metrics.utils.ReporterType;

public interface Reporter {
  /** Start reporter */
  boolean start();

  /** Stop reporter */
  boolean stop();

  /**
   * Get type of reporter
   *
   * @return
   */
  ReporterType getReporterType();
}
