package org.apache.iotdb.metrics.opentelemetry.type;

import org.apache.iotdb.metrics.type.HistogramSnapshot;

import java.io.OutputStream;

/** @Author stormbroken Create by 2021/07/15 @Version 1.0 */
public class OpenTelemetryHistogramSnapshot implements HistogramSnapshot {
  @Override
  public double getValue(double quantile) {
    return 0;
  }

  @Override
  public long[] getValues() {
    return new long[0];
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public double getMedian() {
    return 0;
  }

  @Override
  public long getMax() {
    return 0;
  }

  @Override
  public double getMean() {
    return 0;
  }

  @Override
  public long getMin() {
    return 0;
  }

  /**
   * Writes the values of the snapshot to the given stream.
   *
   * @param output an output stream
   */
  @Override
  public void dump(OutputStream output) {}
}
