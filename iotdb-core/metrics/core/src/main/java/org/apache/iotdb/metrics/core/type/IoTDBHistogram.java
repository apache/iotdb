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

import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.AbstractMetricMBean;

public class IoTDBHistogram extends AbstractMetricMBean implements Histogram, IoTDBHistogramMBean {

  io.micrometer.core.instrument.DistributionSummary distributionSummary;

  public IoTDBHistogram(io.micrometer.core.instrument.DistributionSummary distributionSummary) {
    this.distributionSummary = distributionSummary;
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
  public void update(long value) {
    distributionSummary.record(value);
  }

  @Override
  public long getCount() {
    return distributionSummary.count();
  }

  @Override
  public org.apache.iotdb.metrics.type.HistogramSnapshot takeSnapshot() {
    return new IoTDBHistogramSnapshot(distributionSummary);
  }
}
