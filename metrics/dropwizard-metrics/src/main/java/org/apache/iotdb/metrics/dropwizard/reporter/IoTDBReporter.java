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

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.MetricName;
import org.apache.iotdb.metrics.utils.MetricsUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IoTDBReporter extends ScheduledReporter {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBReporter.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final TimeUnit DURATION_UNIT = TimeUnit.MILLISECONDS;
  private static final TimeUnit RATE_UNIT = TimeUnit.SECONDS;
  private final String prefix;
  private final Session session;

  protected IoTDBReporter(
      MetricRegistry registry,
      String prefix,
      MetricFilter filter,
      ScheduledExecutorService executor,
      boolean shutdownExecutorOnStop) {
    super(
        registry,
        "iotdb-reporter",
        filter,
        RATE_UNIT,
        DURATION_UNIT,
        executor,
        shutdownExecutorOnStop);
    this.prefix = prefix;
    this.session =
        new Session(
            metricConfig.getIoTDBHost(),
            metricConfig.getIoTDBPort(),
            metricConfig.getIoTDBUsername(),
            metricConfig.getIoTDBPassword(),
            true);
  }

  @Override
  public void start(long period, TimeUnit unit) {
    super.start(period, unit);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      logger.error("Failed to add session", e);
    }
  }

  @Override
  public void stop() {
    super.stop();
    try {
      if (session != null) {
        session.close();
      }
    } catch (IoTDBConnectionException e) {
      logger.error("Failed to close session.");
    }
  }

  public static class Builder {
    private final MetricRegistry metricRegistry;
    private String prefix;
    private MetricFilter metricFilter;
    private ScheduledExecutorService executorService;
    private boolean shutdownExecutorOnStop;

    private Builder(MetricRegistry metricRegistry) {
      this.metricRegistry = metricRegistry;
      this.prefix = null;
      this.metricFilter = MetricFilter.ALL;
      this.executorService = null;
      this.shutdownExecutorOnStop = true;
    }

    public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
      this.shutdownExecutorOnStop = shutdownExecutorOnStop;
      return this;
    }

    public Builder scheduleOn(ScheduledExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public Builder prefixedWith(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder filter(MetricFilter metricFilter) {
      this.metricFilter = metricFilter;
      return this;
    }

    public IoTDBReporter build() {
      return new IoTDBReporter(
          metricRegistry, prefix, metricFilter, executorService, shutdownExecutorOnStop);
    }
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      sendGauge(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      sendCounter(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      sendHistogram(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      sendMeter(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      sendTimer(entry.getKey(), entry.getValue());
    }
  }

  private void sendGauge(String name, Gauge gauge) {
    MetricName metricName = new MetricName(name);
    Object obj = gauge.getValue();
    double value;
    if (obj instanceof Number) {
      value = ((Number) obj).doubleValue();
      updateValue(prefixed(metricName.getName()), metricName.getTags(), value);
    } else if (obj instanceof Boolean) {
      updateValue(prefixed(metricName.getName()), metricName.getTags(), obj);
    }
  }

  private void sendCounter(String name, Counter counter) {
    MetricName metricName = new MetricName(name);
    double value = counter.getCount();
    updateValue(prefixed(metricName.getName()), metricName.getTags(), value);
  }

  private void sendHistogram(String name, Histogram histogram) {
    MetricName metricName = new MetricName(name);
    writeSnapshotAndCount(
        prefixed(metricName.getName()),
        metricName.getTags(),
        histogram.getSnapshot(),
        histogram.getCount(),
        1.0);
  }

  private void sendMeter(String name, Meter meter) {
    MetricName metricName = new MetricName(name);
    double value = meter.getCount();
    updateValue(prefixed(metricName.getName()), metricName.getTags(), value);
  }

  private void sendTimer(String name, Timer timer) {
    MetricName metricName = new MetricName(name);
    writeSnapshotAndCount(
        prefixed(metricName.getName()),
        metricName.getTags(),
        timer.getSnapshot(),
        timer.getCount(),
        1.0D / TimeUnit.SECONDS.toNanos(1L));
  }

  private void writeSnapshotAndCount(
      String name, Map<String, String> tags, Snapshot snapshot, long count, double factor) {
    updateValue(name, addTags(tags, "quantile", "0.5"), snapshot.getMedian() * factor);
    updateValue(name, addTags(tags, "quantile", "0.75"), snapshot.get75thPercentile() * factor);
    updateValue(name, addTags(tags, "quantile", "0.95"), snapshot.get95thPercentile() * factor);
    updateValue(name, addTags(tags, "quantile", "0.98"), snapshot.get98thPercentile() * factor);
    updateValue(name, addTags(tags, "quantile", "0.99"), snapshot.get99thPercentile() * factor);
    updateValue(name, addTags(tags, "quantile", "0.999"), snapshot.get999thPercentile() * factor);
    updateValue(name + "_min", tags, snapshot.getMin());
    updateValue(name + "_max", tags, snapshot.getMax());
    updateValue(name + "_median", tags, snapshot.getMedian());
    updateValue(name + "_mean", tags, snapshot.getMean());
    updateValue(name + "_stddev", tags, snapshot.getStdDev());
    updateValue(name + "_count", tags, count);
  }

  private void updateValue(String name, Map<String, String> labels, Object value) {
    if (value != null) {
      String deviceId = MetricsUtils.generatePath(name, labels);
      List<String> sensors = Collections.singletonList("value");

      List<TSDataType> dataTypes = new ArrayList<>();
      if (value instanceof Boolean) {
        dataTypes.add(TSDataType.BOOLEAN);
      } else if (value instanceof Integer) {
        dataTypes.add(TSDataType.INT32);
      } else if (value instanceof Long) {
        dataTypes.add(TSDataType.INT64);
      } else if (value instanceof Double) {
        dataTypes.add(TSDataType.DOUBLE);
      } else {
        dataTypes.add(TSDataType.TEXT);
        value = value.toString();
      }

      try {
        session.insertRecord(deviceId, System.currentTimeMillis(), sensors, dataTypes, value);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        logger.warn("Failed to insert record");
      }
    }
  }

  private String prefixed(String name) {
    return prefix == null ? name : (prefix + name);
  }

  private Map<String, String> addTags(Map<String, String> tags, String key, String value) {
    HashMap<String, String> result = new HashMap<>(tags);
    result.put(key, value);
    return result;
  }

  public static Builder forRegistry(MetricRegistry metricRegistry) {
    return new Builder(metricRegistry);
  }
}
