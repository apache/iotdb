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

import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.utils.AbstractMetricMBean;

import com.codahale.metrics.Clock;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

/**
 * Gauges with cache, which have better performance in some read-intensive and calculation-intensive
 * scenarios. Can be served as an additional option.
 *
 * <p>This file is modified from com.codahale.metrics.CachedGauge<T>, it should be bug free.
 */
public class IoTDBCachedGauge<T> extends AbstractMetricMBean
    implements AutoGauge, IoTDBCachedGaugeMBean {
  /** The timer of metric system */
  private final Clock clock;

  /** The time to reload cache */
  private final AtomicLong reloadAt;

  /** The timeout duration */
  private final long timeoutNS;

  /** The cache's value */
  private final AtomicReference<Double> value;

  /** The reference object of gauge */
  private final WeakReference<T> refObj;

  /** The calculate function of gauge */
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
  public double getValue() {
    return getGaugeValue();
  }

  private double loadValue() {
    if (refObj.get() == null) {
      return 0d;
    }
    return mapper.applyAsDouble(refObj.get());
  }

  public double getGaugeValue() {
    Double currentValue = this.value.get();
    // if cache expires or is not loaded, we update the cache with calculate function
    if (shouldLoad() || currentValue == null) {
      double newValue = loadValue();
      if (!this.value.compareAndSet(currentValue, newValue)) {
        return this.value.get();
      }
      return newValue;
    }
    // else we directly return the cache's value
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
