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

package org.apache.iotdb.metrics.core;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.core.type.IoTDBAutoGauge;
import org.apache.iotdb.metrics.core.type.IoTDBCounter;
import org.apache.iotdb.metrics.core.type.IoTDBGauge;
import org.apache.iotdb.metrics.core.type.IoTDBHistogram;
import org.apache.iotdb.metrics.core.type.IoTDBRate;
import org.apache.iotdb.metrics.core.type.IoTDBTimer;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.NoPauseDetector;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleConfig;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

public class IoTDBMetricManager extends AbstractMetricManager {

  /** The clock which is used in Metric system */
  private final Clock clock = Clock.SYSTEM;

  /** The histogram config which is used to create IoTDBTimer and IoTDBHistogram */
  private final DistributionStatisticConfig IoTDBDistributionStatisticConfig;

  private IoTDBMetricManager() {
    SimpleConfig config = SimpleConfig.DEFAULT;
    // set default distribution config
    DistributionStatisticConfig defaultHistogramConfig =
        DistributionStatisticConfig.builder()
            .expiry(config.step())
            .build()
            .merge(DistributionStatisticConfig.DEFAULT);

    // merge default config with IoTDB's config
    DistributionStatisticConfig distributionStatisticConfig =
        DistributionStatisticConfig.builder()
            .percentiles(0.5, 0.99)
            .bufferLength(2)
            .build()
            .merge(defaultHistogramConfig);

    // set expiry and get final IoTDB's histogram config
    this.IoTDBDistributionStatisticConfig =
        distributionStatisticConfig.merge(
            DistributionStatisticConfig.builder().expiry(config.step()).build());
  }

  @Override
  public Counter createCounter() {
    return new IoTDBCounter();
  }

  public <T> AutoGauge createAutoGauge(T obj, ToDoubleFunction<T> mapper) {
    return new IoTDBAutoGauge<>(obj, mapper);
  }

  @Override
  public Gauge createGauge() {
    return new IoTDBGauge();
  }

  @Override
  public Histogram createHistogram() {
    // create distributionSummary
    io.micrometer.core.instrument.DistributionSummary distributionSummary =
        new CumulativeDistributionSummary(
            null, clock, IoTDBDistributionStatisticConfig, 1.0, false);

    return new IoTDBHistogram(distributionSummary);
  }

  @Override
  public Rate createRate() {
    return new IoTDBRate();
  }

  @Override
  public Timer createTimer() {
    // set pauseDetector
    PauseDetector pauseDetector = new NoPauseDetector();

    io.micrometer.core.instrument.Timer timer =
        new CumulativeTimer(
            null, clock, IoTDBDistributionStatisticConfig, pauseDetector, TimeUnit.SECONDS, false);
    return new IoTDBTimer(timer);
  }

  @Override
  protected void removeMetric(MetricType type, MetricInfo metricInfo) {
    // empty body
  }

  @Override
  public boolean stopFramework() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }

  private static class IoTDBMetricManagerHolder {
    private static final IoTDBMetricManager INSTANCE = new IoTDBMetricManager();

    private IoTDBMetricManagerHolder() {
      // empty constructor
    }
  }

  public static IoTDBMetricManager getInstance() {
    return IoTDBMetricManagerHolder.INSTANCE;
  }
}
