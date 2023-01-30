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

package org.apache.iotdb.metrics.micrometer.type;

import org.apache.iotdb.metrics.type.Gauge;

import io.micrometer.core.instrument.Tags;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.function.ToLongFunction;

public class MicrometerAutoGauge<T> implements Gauge {
  private final WeakReference<T> refObject;
  private final ToLongFunction<T> mapper;

  public MicrometerAutoGauge(
      io.micrometer.core.instrument.MeterRegistry meterRegistry,
      String metricName,
      T object,
      ToLongFunction<T> mapper,
      String... tags) {
    LoggerFactory.getLogger(MicrometerAutoGauge.class).info("{},{}", metricName, tags);
    this.refObject =
        new WeakReference<>(
            meterRegistry.gauge(
                metricName, Tags.of(tags), object, value -> (double) mapper.applyAsLong(value)));
    this.mapper = mapper;
  }

  @Override
  public void set(long value) {
    throw new UnsupportedOperationException("unsupported manually updating an exist obj's state");
  }

  @Override
  public long value() {
    if (refObject.get() == null) {
      return 0L;
    }
    return mapper.applyAsLong(refObject.get());
  }

  @Override
  public void incr(long value) {
    throw new UnsupportedOperationException("unsupported manually updating an exist obj's state");
  }

  @Override
  public void decr(long value) {
    throw new UnsupportedOperationException("unsupported manually updating an exist obj's state");
  }
}
