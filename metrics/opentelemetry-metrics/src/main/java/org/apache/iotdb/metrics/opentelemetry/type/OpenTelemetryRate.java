package org.apache.iotdb.metrics.opentelemetry.type;

import org.apache.iotdb.metrics.type.Rate;

/** @Author stormbroken Create by 2021/07/15 @Version 1.0 */
public class OpenTelemetryRate implements Rate {
  @Override
  public long getCount() {
    return 0;
  }

  @Override
  public double getOneMinuteRate() {
    return 0;
  }

  @Override
  public double getMeanRate() {
    return 0;
  }

  @Override
  public double getFiveMinuteRate() {
    return 0;
  }

  @Override
  public double getFifteenMinuteRate() {
    return 0;
  }

  @Override
  public void mark() {}

  @Override
  public void mark(long n) {}
}
