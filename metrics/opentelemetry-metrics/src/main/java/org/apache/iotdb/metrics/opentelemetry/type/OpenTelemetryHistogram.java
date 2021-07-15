package org.apache.iotdb.metrics.opentelemetry.type;

import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.HistogramSnapshot;

/** @Author stormbroken Create by 2021/07/15 @Version 1.0 */
public class OpenTelemetryHistogram implements Histogram {
  @Override
  public void update(int value) {}

  @Override
  public void update(long value) {}

  @Override
  public long count() {
    return 0;
  }

  @Override
  public HistogramSnapshot takeSnapshot() {
    return null;
  }
}
