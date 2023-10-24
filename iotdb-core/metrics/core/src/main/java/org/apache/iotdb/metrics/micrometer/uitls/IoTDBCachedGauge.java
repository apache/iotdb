package org.apache.iotdb.metrics.micrometer.uitls;

import org.apache.iotdb.metrics.type.AutoGauge;

import com.codahale.metrics.Clock;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

/**
 * TODO 注释
 *
 * @param <T>
 */
public class IoTDBCachedGauge<T> implements AutoGauge {
  private final Clock clock;
  private final AtomicLong reloadAt;
  private final long timeoutNS;
  // 对外接口是 double
  private final AtomicReference<Double> value;
  private final WeakReference<T> refObj;
  private final ToDoubleFunction<T> mapper;

  protected IoTDBCachedGauge(
      WeakReference<T> refObj, ToDoubleFunction<T> mapper, long timeout, TimeUnit timeoutUnit) {
    this(Clock.defaultClock(), refObj, mapper, timeout, timeoutUnit);
  }

  protected IoTDBCachedGauge(
      Clock clock,
      WeakReference<T> refObj,
      ToDoubleFunction<T> mapper,
      long timeout,
      TimeUnit timeoutUnit) {
    this.clock = clock;
    this.refObj = refObj;
    this.mapper = mapper;
    this.reloadAt = new AtomicLong(clock.getTick());
    this.timeoutNS = timeoutUnit.toNanos(timeout);
    this.value = new AtomicReference<>();
  }

  @Override
  public double value() {
    return getValue();
  }

  private double loadValue() {
    if (refObj.get() == null) {
      return 0d;
    }
    return mapper.applyAsDouble(refObj.get());
  }

  public double getValue() {
    Double currentValue = this.value.get();
    if (shouldLoad() || currentValue == null) {
      double newValue = loadValue();
      if (!this.value.compareAndSet(currentValue, newValue)) {
        return this.value.get();
      }
      return newValue;
    }
    return currentValue;
  }

  private boolean shouldLoad() {
    while (true) {
      final long time = clock.getTick();
      final long current = reloadAt.get();
      if (current > time) {
        return false;
      }
      if (reloadAt.compareAndSet(current, time + timeoutNS)) {
        return true;
      }
    }
  }
}
