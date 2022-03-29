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

package org.apache.iotdb.metrics.dropwizard.reporter;

import org.apache.iotdb.metrics.dropwizard.MetricName;

import com.codahale.metrics.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class DropwizardMetricsExporter {
  private final MetricRegistry metricRegistry;
  private final PrometheusTextWriter writer;

  public DropwizardMetricsExporter(MetricRegistry metricRegistry, PrometheusTextWriter writer) {
    this.metricRegistry = metricRegistry;
    this.writer = writer;
  }

  public void scrape() throws IOException {
    for (Map.Entry<String, Gauge> entry : metricRegistry.getGauges().entrySet()) {
      writeGauge(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Counter> entry : metricRegistry.getCounters().entrySet()) {
      writeCounter(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Histogram> entry : metricRegistry.getHistograms().entrySet()) {
      writeHistogram(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Meter> entry : metricRegistry.getMeters().entrySet()) {
      writeMeter(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Timer> entry : metricRegistry.getTimers().entrySet()) {
      writeTimer(entry.getKey(), entry.getValue());
    }
  }

  /** Export Gauge as Prometheus Gauge */
  public void writeGauge(String dropwizardName, Gauge<?> gauge) throws IOException {
    MetricName metricName = new MetricName(dropwizardName);
    String sanitizeName = metricName.getName();
    writer.writeHelp(sanitizeName, getHelpMessage(dropwizardName, gauge));
    writer.writeType(sanitizeName, MetricType.GAUGE);

    Object obj = gauge.getValue();
    double value;
    if (obj instanceof Number) {
      value = ((Number) obj).doubleValue();
    } else if (obj instanceof Boolean) {
      value = ((Boolean) obj) ? 1 : 0;
    } else {
      return;
    }

    writer.writeSample(sanitizeName, metricName.getTags(), value);
  }

  /** Export counter as Prometheus Gauge */
  public void writeCounter(String dropwizardName, Counter counter) throws IOException {
    MetricName metricName = new MetricName(dropwizardName);
    String sanitizeName = metricName.getName();
    writer.writeHelp(sanitizeName, getHelpMessage(dropwizardName, counter));
    writer.writeType(sanitizeName, MetricType.GAUGE);
    writer.writeSample(sanitizeName, metricName.getTags(), counter.getCount());
  }

  /** Export histogram snapshot as Prometheus SUMMARY */
  public void writeHistogram(String dropwizardName, Histogram histogram) throws IOException {
    writeSnapshotAndCount(
        new MetricName(dropwizardName),
        histogram.getSnapshot(),
        histogram.getCount(),
        1.0,
        getHelpMessage(dropwizardName, histogram));
  }

  /** Export histogram snapshot */
  private void writeSnapshotAndCount(
      MetricName metricName, Snapshot snapshot, long count, double factor, String helpMessage)
      throws IOException {
    String sanitizeName = metricName.getName();
    writer.writeHelp(sanitizeName, helpMessage);
    writer.writeType(sanitizeName, MetricType.SUMMARY);
    Map<String, String> tags = metricName.getTags();
    writer.writeSample(
        sanitizeName, addTags(tags, "quantile", "0.5"), snapshot.getMedian() * factor);
    writer.writeSample(
        sanitizeName, addTags(tags, "quantile", "0.75"), snapshot.get75thPercentile() * factor);
    writer.writeSample(
        sanitizeName, addTags(tags, "quantile", "0.95"), snapshot.get95thPercentile() * factor);
    writer.writeSample(
        sanitizeName, addTags(tags, "quantile", "0.98"), snapshot.get98thPercentile() * factor);
    writer.writeSample(
        sanitizeName, addTags(tags, "quantile", "0.99"), snapshot.get99thPercentile() * factor);
    writer.writeSample(
        sanitizeName, addTags(tags, "quantile", "0.999"), snapshot.get999thPercentile() * factor);
    writer.writeSample(sanitizeName + "_min", tags, snapshot.getMin());
    writer.writeSample(sanitizeName + "_max", tags, snapshot.getMax());
    writer.writeSample(sanitizeName + "_median", tags, snapshot.getMedian());
    writer.writeSample(sanitizeName + "_mean", tags, snapshot.getMean());
    writer.writeSample(sanitizeName + "_stddev", tags, snapshot.getStdDev());
    writer.writeSample(sanitizeName + "_count", tags, count);
  }

  /** Export Timer as Prometheus Summary */
  public void writeTimer(String dropwizardName, Timer timer) throws IOException {
    writeSnapshotAndCount(
        new MetricName(dropwizardName),
        timer.getSnapshot(),
        timer.getCount(),
        1.0D / TimeUnit.SECONDS.toNanos(1L),
        getHelpMessage(dropwizardName, timer));
    writeMetered(new MetricName(dropwizardName), timer);
  }

  /** Export Meter as Prometheus Counter */
  public void writeMeter(String dropwizardName, Meter meter) throws IOException {
    MetricName metricName = new MetricName(dropwizardName);
    String sanitizeName = metricName.getName() + "_total";

    writer.writeHelp(sanitizeName, getHelpMessage(dropwizardName, meter));
    writer.writeType(sanitizeName, MetricType.COUNTER);
    writer.writeSample(sanitizeName, metricName.getTags(), meter.getCount());

    writeMetered(new MetricName(dropwizardName), meter);
  }

  /** Export meter for multi type */
  private void writeMetered(MetricName metricName, Metered metered) throws IOException {
    String sanitizeName = metricName.getName();
    Map<String, String> tags = metricName.getTags();
    writer.writeSample(sanitizeName, addTags(tags, "rate", "m1"), metered.getOneMinuteRate());
    writer.writeSample(sanitizeName, addTags(tags, "rate", "m5"), metered.getFiveMinuteRate());
    writer.writeSample(sanitizeName, addTags(tags, "rate", "m15"), metered.getFifteenMinuteRate());
    writer.writeSample(sanitizeName, addTags(tags, "rate", "mean"), metered.getMeanRate());
  }

  private Map<String, String> addTags(Map<String, String> tags, String key, String value) {
    HashMap<String, String> result = new HashMap<>(tags);
    result.put(key, value);
    return result;
  }

  private static String getHelpMessage(String metricName, Metric metric) {
    return String.format(
        "Generated from Dropwizard metric import (metric=%s, type=%s)",
        metricName, metric.getClass().getName());
  }
}
