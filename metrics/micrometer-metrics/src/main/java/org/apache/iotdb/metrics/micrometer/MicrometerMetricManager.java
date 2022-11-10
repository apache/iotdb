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

package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.micrometer.type.MicrometerAutoGauge;
import org.apache.iotdb.metrics.micrometer.type.MicrometerCounter;
import org.apache.iotdb.metrics.micrometer.type.MicrometerGauge;
import org.apache.iotdb.metrics.micrometer.type.MicrometerHistogram;
import org.apache.iotdb.metrics.micrometer.type.MicrometerRate;
import org.apache.iotdb.metrics.micrometer.type.MicrometerTimer;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/** Metric manager based on micrometer. More details in https://micrometer.io/. */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class MicrometerMetricManager extends AbstractMetricManager {

  io.micrometer.core.instrument.MeterRegistry meterRegistry;

  public MicrometerMetricManager() {
    meterRegistry = Metrics.globalRegistry;
  }

  @Override
  public Counter createCounter(MetricInfo metricInfo) {
    return new MicrometerCounter(
        meterRegistry.counter(metricInfo.getName(), metricInfo.getTagsInArray()));
  }

  @Override
  public <T> Gauge createAutoGauge(MetricInfo metricInfo, T obj, ToLongFunction<T> mapper) {
    return new MicrometerAutoGauge<T>(
        meterRegistry, metricInfo.getName(), obj, mapper, metricInfo.getTagsInArray());
  }

  @Override
  public Gauge createGauge(MetricInfo metricInfo) {
    return new MicrometerGauge(meterRegistry, metricInfo.getName(), metricInfo.getTagsInArray());
  }

  @Override
  public Histogram createHistogram(MetricInfo metricInfo) {
    io.micrometer.core.instrument.DistributionSummary distributionSummary =
        io.micrometer.core.instrument.DistributionSummary.builder(metricInfo.getName())
            .tags(metricInfo.getTagsInArray())
            .publishPercentiles(0, 0.25, 0.5, 0.75, 1)
            .register(meterRegistry);
    return new MicrometerHistogram(distributionSummary);
  }

  @Override
  public Rate createRate(MetricInfo metricInfo) {
    return new MicrometerRate(
        meterRegistry.gauge(
            metricInfo.getName(), Tags.of(metricInfo.getTagsInArray()), new AtomicLong(0)));
  }

  @Override
  public Timer createTimer(MetricInfo metricInfo) {
    io.micrometer.core.instrument.Timer timer =
        io.micrometer.core.instrument.Timer.builder(metricInfo.getName())
            .tags(metricInfo.getTagsInArray())
            .publishPercentiles(0, 0.25, 0.5, 0.75, 1)
            .register(meterRegistry);
    return new MicrometerTimer(timer);
  }

  @Override
  protected void removeMetric(MetricType type, MetricInfo metricInfo) {
    Meter.Type meterType = transformType(type);
    Meter.Id id =
        new Meter.Id(
            metricInfo.getName(), Tags.of(metricInfo.getTagsInArray()), null, null, meterType);
    meterRegistry.remove(id);
  }

  @Override
  public boolean stopFramework() {
    meterRegistry.clear();
    return true;
  }

  private Meter.Type transformType(MetricType type) {
    switch (type) {
      case COUNTER:
        return Meter.Type.COUNTER;
      case GAUGE:
      case RATE:
        return Meter.Type.GAUGE;
      case HISTOGRAM:
        return Meter.Type.DISTRIBUTION_SUMMARY;
      case TIMER:
        return Meter.Type.TIMER;
      default:
        return Meter.Type.OTHER;
    }
  }
}
