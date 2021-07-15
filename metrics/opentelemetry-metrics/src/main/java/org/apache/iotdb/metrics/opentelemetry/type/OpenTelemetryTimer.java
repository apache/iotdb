package org.apache.iotdb.metrics.opentelemetry.type;

import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import java.util.concurrent.TimeUnit;

/** @Author stormbroken Create by 2021/07/15 @Version 1.0 */
public class OpenTelemetryTimer implements Timer {
  @Override
  public void update(long duration, TimeUnit unit) {}

  @Override
  public void updateMillis(long durationMillis) {}

  @Override
  public void updateMicros(long durationMicros) {}

  @Override
  public void updateNanos(long durationNanos) {}

  @Override
  public HistogramSnapshot takeSnapshot() {
    return null;
  }

  /**
   * It's not safe to use the update interface.
   *
   * @return the getOrCreatRate related with the getOrCreateTimer
   */
  @Override
  public Rate getImmutableRate() {
    return null;
  }
}
