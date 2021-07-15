package org.apache.iotdb.metrics.opentelemetry.type;

import org.apache.iotdb.metrics.type.Gauge;

/** @Author stormbroken Create by 2021/07/15 @Version 1.0 */
public class OpenTelemetryGauge implements Gauge {
  @Override
  public long value() {
    return 0;
  }

  @Override
  public void set(long value) {}
}
