/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.core.utils;

import com.codahale.metrics.Clock;
import com.codahale.metrics.EWMA;
import com.codahale.metrics.MovingAverages;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This file is modified from com.codahale.metrics.ExponentialMovingAverages, some unnecessary
 * variables are removed.
 */
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
