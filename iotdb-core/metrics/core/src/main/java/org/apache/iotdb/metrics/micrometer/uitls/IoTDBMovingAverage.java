package org.apache.iotdb.metrics.micrometer.uitls;

import com.codahale.metrics.Clock;
import com.codahale.metrics.EWMA;
import com.codahale.metrics.MovingAverages;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** TODO 注释 */
public class IoTDBMovingAverage implements MovingAverages {

  private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);
  private final EWMA m1Rate = EWMA.oneMinuteEWMA();
  private final AtomicLong lastTick;
  private final Clock clock;

  /** Creates a new {@link IoTDBMovingAverage}. */
  public IoTDBMovingAverage() {
    this(Clock.defaultClock());
  }

  /** Creates a new {@link IoTDBMovingAverage}. */
  public IoTDBMovingAverage(Clock clock) {
    this.clock = clock;
    this.lastTick = new AtomicLong(this.clock.getTick());
  }

  private static class IoTDBMovingAverageHolder {
    private static final IoTDBMovingAverage INSTANCE = new IoTDBMovingAverage();

    private IoTDBMovingAverageHolder() {
      // empty constructor
    }
  }

  public static IoTDBMovingAverage getInstance() {
    return IoTDBMovingAverageHolder.INSTANCE;
  }

  @Override
  public void update(long n) {
    m1Rate.update(n);
  }

  @Override
  public void tickIfNecessary() {
    final long oldTick = lastTick.get();
    final long newTick = clock.getTick();
    final long age = newTick - oldTick;
    if (age > TICK_INTERVAL) {
      final long newIntervalStartTick = newTick - age % TICK_INTERVAL;
      if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
        final long requiredTicks = age / TICK_INTERVAL;
        for (long i = 0; i < requiredTicks; i++) {
          m1Rate.tick();
        }
      }
    }
  }

  @Override
  public double getM1Rate() {
    return m1Rate.getRate(TimeUnit.SECONDS);
  }

  @Override
  public double getM5Rate() {
    return 0d;
  }

  @Override
  public double getM15Rate() {
    return 0d;
  }
}
