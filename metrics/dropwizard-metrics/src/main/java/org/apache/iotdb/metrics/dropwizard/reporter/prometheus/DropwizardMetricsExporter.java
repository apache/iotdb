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

package org.apache.iotdb.metrics.dropwizard.reporter.prometheus;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class DropwizardMetricsExporter {
  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricsExporter.class);

  private final PrometheusTextWriter writer;

  public DropwizardMetricsExporter(PrometheusTextWriter writer) {
    this.writer = writer;
  }

  /** Export Gauge as Prometheus Guage */
  public void writeGauge(String name, Gauge<?> gauge) throws IOException {
    final String sanitizedName = sanitizeMetricName(name);
    writer.writeHelp(sanitizedName, getHelpMessage(name, gauge));
    writer.writeType(sanitizedName, MetricType.GAUGE);

    Object obj = gauge.getValue();
    double value;
    if (obj instanceof Number) {
      value = ((Number) obj).doubleValue();
    } else if (obj instanceof Boolean) {
      value = ((Boolean) obj) ? 1 : 0;
    } else {
      LOG.warn("Invalid type for Gauge {}: {}", name, obj.getClass().getName());
      return;
    }

    writer.writeSample(sanitizedName, emptyMap(), value);
  }

  /**
   * Export counter as Prometheus GAUGE
   *
   * @param dropwizardName need to transform
   * @param counter
   * @throws IOException
   */
  public void writeCounter(String dropwizardName, Counter counter) throws IOException {
    String name = sanitizeMetricName(dropwizardName);
    writer.writeHelp(name, getHelpMessage(dropwizardName, counter));
    writer.writeType(name, MetricType.GAUGE);
    writer.writeSample(name, emptyMap(), counter.getCount());
  }

  /**
   * Export histogram snapshot as Prometheus SUMMARY
   *
   * @param dropwizardName
   * @param histogram
   * @throws IOException
   */
  public void writeHistogram(String dropwizardName, Histogram histogram) throws IOException {
    writeSnapshotAndCount(
        dropwizardName,
        histogram.getSnapshot(),
        histogram.getCount(),
        1.0,
        MetricType.SUMMARY,
        getHelpMessage(dropwizardName, histogram));
  }

  /**
   * Export histogram snapshot
   *
   * @param dropwizardName
   * @param snapshot
   * @param count
   * @param factor
   * @param type
   * @param helpMessage
   * @throws IOException
   */
  private void writeSnapshotAndCount(
      String dropwizardName,
      Snapshot snapshot,
      long count,
      double factor,
      MetricType type,
      String helpMessage)
      throws IOException {
    String name = sanitizeMetricName(dropwizardName);
    writer.writeHelp(name, helpMessage);
    writer.writeType(name, type);
    writer.writeSample(name, mapOf("quantile", "0.5"), snapshot.getMedian() * factor);
    writer.writeSample(name, mapOf("quantile", "0.75"), snapshot.get75thPercentile() * factor);
    writer.writeSample(name, mapOf("quantile", "0.95"), snapshot.get95thPercentile() * factor);
    writer.writeSample(name, mapOf("quantile", "0.98"), snapshot.get98thPercentile() * factor);
    writer.writeSample(name, mapOf("quantile", "0.99"), snapshot.get99thPercentile() * factor);
    writer.writeSample(name, mapOf("quantile", "0.999"), snapshot.get999thPercentile() * factor);
    writer.writeSample(name + "_min", emptyMap(), snapshot.getMin());
    writer.writeSample(name + "_max", emptyMap(), snapshot.getMax());
    writer.writeSample(name + "_median", emptyMap(), snapshot.getMedian());
    writer.writeSample(name + "_mean", emptyMap(), snapshot.getMean());
    writer.writeSample(name + "_stddev", emptyMap(), snapshot.getStdDev());
    writer.writeSample(name + "_count", emptyMap(), count);
  }

  /**
   * Export Timer as Prometheus SUMMARY
   *
   * @param dropwizardName
   * @param timer
   * @throws IOException
   */
  public void writeTimer(String dropwizardName, Timer timer) throws IOException {
    writeSnapshotAndCount(
        dropwizardName,
        timer.getSnapshot(),
        timer.getCount(),
        1.0D / TimeUnit.SECONDS.toNanos(1L),
        MetricType.SUMMARY,
        getHelpMessage(dropwizardName, timer));
    writeMetered(dropwizardName, timer);
  }

  /**
   * Export Meter as Prometheus Counter
   *
   * @param dropwizardName
   * @param meter
   * @throws IOException
   */
  public void writeMeter(String dropwizardName, Meter meter) throws IOException {
    String name = sanitizeMetricName(dropwizardName) + "_total";

    writer.writeHelp(name, getHelpMessage(dropwizardName, meter));
    writer.writeType(name, MetricType.COUNTER);
    writer.writeSample(name, emptyMap(), meter.getCount());

    writeMetered(dropwizardName, meter);
  }

  /**
   * Export meter for multi type
   *
   * @param dropwizardName
   * @param metered
   * @throws IOException
   */
  private void writeMetered(String dropwizardName, Metered metered) throws IOException {
    String name = sanitizeMetricName(dropwizardName);
    writer.writeSample(name, mapOf("rate", "m1"), metered.getOneMinuteRate());
    writer.writeSample(name, mapOf("rate", "m5"), metered.getFiveMinuteRate());
    writer.writeSample(name, mapOf("rate", "m15"), metered.getFifteenMinuteRate());
    writer.writeSample(name, mapOf("rate", "mean"), metered.getMeanRate());
  }

  private Map<String, String> mapOf(String key, String value) {
    HashMap<String, String> result = new HashMap<>();
    result.put(key, value);
    return result;
  }

  private Map<String, String> emptyMap() {
    return Collections.emptyMap();
  }

  private static String getHelpMessage(String metricName, Metric metric) {
    return String.format(
        "Generated from Dropwizard metric import (metric=%s, type=%s)",
        metricName, metric.getClass().getName());
  }

  static String sanitizeMetricName(String dropwizardName) {
    return dropwizardName.replaceAll("[^a-zA-Z0-9:_]", "_");
  }
}
