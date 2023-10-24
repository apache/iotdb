package org.apache.iotdb.metrics.micrometer.type;

import org.apache.iotdb.metrics.micrometer.uitls.IoTDBMovingAverage;
import org.apache.iotdb.metrics.type.Rate;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * could not publish to other metrics system exclude jmx and csv, because micrometer assumes that
 * other metrics system have the ability to calculate rate. Details is at
 * https://github.com/micrometer-metrics/micrometer/issues/1935.
 *
 * <p>Now, we only record a gauge for the rate record in micrometer, and we use dropwizard meter to
 * calculate the meter.
 */
public class IoTDBRate implements Rate {
  AtomicLong atomicLong;
  Meter meter;

  public IoTDBRate(AtomicLong atomicLong) {
    this.atomicLong = atomicLong;
    this.meter = new Meter(IoTDBMovingAverage.getInstance(), Clock.defaultClock());
  }

  @Override
  public long getCount() {
    return meter.getCount();
  }

  @Override
  public double getOneMinuteRate() {
    return meter.getOneMinuteRate();
  }

  @Override
  public double getMeanRate() {
    return meter.getMeanRate();
  }

  @Override
  public void mark() {
    atomicLong.set(1);
    meter.mark();
  }

  @Override
  public void mark(long n) {
    atomicLong.set(n);
    meter.mark(n);
  }
}
