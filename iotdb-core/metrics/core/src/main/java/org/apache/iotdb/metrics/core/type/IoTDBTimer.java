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

import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.AbstractMetricMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class IoTDBTimer extends AbstractMetricMBean implements Timer, IoTDBTimerMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBTimer.class);
  io.micrometer.core.instrument.Timer timer;

  public IoTDBTimer(io.micrometer.core.instrument.Timer timer) {
    this.timer = timer;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    timer.record(duration, unit);
  }

  @Override
  public HistogramSnapshot takeSnapshot() {
    try {
      return new IoTDBTimerHistogramSnapshot(timer);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOGGER.warn(
          "Detected an error while taking snapshot, may cause a miss during this recording.", e);
      return null;
    }
  }

  @Override
  public double getSum() {
    return Optional.ofNullable(takeSnapshot()).map(HistogramSnapshot::getSum).orElse(0.0);
  }

  @Override
  public double getMax() {
    return Optional.ofNullable(takeSnapshot()).map(HistogramSnapshot::getMax).orElse(0.0);
  }

  @Override
  public double getMean() {
    return Optional.ofNullable(takeSnapshot()).map(HistogramSnapshot::getMean).orElse(0.0);
  }

  @Override
  public int getSize() {
    return Optional.ofNullable(takeSnapshot()).map(HistogramSnapshot::size).orElse(0);
  }

  @Override
  public double get50thPercentile() {
    return Optional.ofNullable(takeSnapshot())
        .map(histogramSnapshot -> histogramSnapshot.getValue(0.5))
        .orElse(0.0);
  }

  @Override
  public double get99thPercentile() {
    return Optional.ofNullable(takeSnapshot())
        .map(histogramSnapshot -> histogramSnapshot.getValue(0.99))
        .orElse(0.0);
  }

  @Override
  public long getCount() {
    return timer.count();
  }
}
