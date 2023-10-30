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

import org.apache.iotdb.metrics.core.type.IoTDBTimerMBean.AbstractJmxTimerBean;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Timer;

import java.util.concurrent.TimeUnit;

public class IoTDBTimer extends AbstractJmxTimerBean implements Timer, IoTDBTimerMBean {

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
    return new IoTDBTimerHistogramSnapshot(timer);
  }

  @Override
  public double getSum() {
    return this.takeSnapshot().getSum();
  }

  @Override
  public double getMax() {
    return this.takeSnapshot().getMax();
  }

  @Override
  public double getMean() {
    return this.takeSnapshot().getMean();
  }

  @Override
  public int getSize() {
    return this.takeSnapshot().size();
  }

  @Override
  public double get50thPercentile() {
    return this.takeSnapshot().getValue(0.5);
  }

  @Override
  public double get99thPercentile() {
    return this.takeSnapshot().getValue(0.99);
  }

  @Override
  public long getCount() {
    return timer.count();
  }
}
