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

package org.apache.iotdb.metrics.core.type;

import org.apache.iotdb.metrics.core.reporter.IoTDBJmxReporter.AbstractJmxRateBean;
import org.apache.iotdb.metrics.core.uitls.IoTDBMovingAverage;
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
public class IoTDBRate extends AbstractJmxRateBean implements Rate {
  Meter meter;

  public IoTDBRate() {
    this.meter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
  }

  @Override
  public long getCount() {
    return this.count();
  }

  @Override
  public double getMeanRate() {
    return this.meanRate();
  }

  @Override
  public double getOneMinuteRate() {
    return this.oneMinuteRate();
  }

  @Override
  public long count() {
    return meter.getCount();
  }

  @Override
  public double oneMinuteRate() {
    return meter.getOneMinuteRate();
  }

  @Override
  public double meanRate() {
    return meter.getMeanRate();
  }

  @Override
  public void mark() {
    meter.mark();
  }

  @Override
  public void mark(long n) {
    meter.mark(n);
  }
}
