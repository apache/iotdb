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

package org.apache.iotdb.metrics.utils;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingMetric;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class IoTDBMetricsUtils {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBMetricsUtils.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final String DATABASE = "root.__system";

  /** Generate the path of metric */
  private static StringBuilder generateMetric(String name) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append(DATABASE)
        .append(".")
        .append(metricConfig.getIoTDBReporterConfig().getLocation())
        .append(".`")
        .append(metricConfig.getRpcAddress())
        .append(":")
        .append(metricConfig.getRpcPort())
        .append("`")
        .append(".")
        .append("`")
        .append(name)
        .append("`");
    return stringBuilder;
  }

  /** Generate the path of metric with tags array */
  public static String generatePath(String name, String... tags) {
    StringBuilder stringBuilder = generateMetric(name);
    for (int i = 0; i < tags.length; i += 2) {
      stringBuilder
          .append(".")
          .append("`")
          .append(tags[i])
          .append("=")
          .append(tags[i + 1])
          .append("`");
    }
    return stringBuilder.toString();
  }

  /** Generate the path of metric with tags map */
  public static String generatePath(String name, Map<String, String> labels) {
    StringBuilder stringBuilder = generateMetric(name);
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      stringBuilder
          .append(".")
          .append("`")
          .append(entry.getKey())
          .append("=")
          .append(entry.getValue())
          .append("`");
    }
    return stringBuilder.toString();
  }

  /** Check the existence of database. If not exists, then create */
  public static void checkOrCreateDatabase(SessionPool session) {
    try (SessionDataSetWrapper result =
        session.executeQueryStatement("SHOW DATABASES " + DATABASE)) {
      if (!result.hasNext()) {
        session.createDatabase(DATABASE);
      }
    } catch (IoTDBConnectionException e) {
      logger.error("CheckOrCreateStorageGroup failed because ", e);
    } catch (StatementExecutionException e) {
      // do nothing
    }
  }

  public static void exportMetricToIoTDBFormat(
      IMetric metric, Map<Pair<String, String>, Object> values, String name, String... tags) {
    if (!(metric instanceof DoNothingMetric)) {
      String writePrefix = generatePath(name, tags);
      if (metric instanceof Counter) {
        Counter counter = (Counter) metric;
        values.put(new Pair<>(writePrefix, "value"), counter.count());
      } else if (metric instanceof AutoGauge) {
        // do nothing
      } else if (metric instanceof Gauge) {
        Gauge gauge = (Gauge) metric;
        values.put(new Pair<>(writePrefix, "value"), gauge.value());
      } else if (metric instanceof Rate) {
        Rate rate = (Rate) metric;
        exportRateToIoTDBFormat(writePrefix, rate, values);
      } else if (metric instanceof Histogram) {
        Histogram histogram = (Histogram) metric;
        values.put(new Pair<>(writePrefix, "count"), histogram.count());
        exportHistogramSnapshotToIoTDBFormat(writePrefix, histogram.takeSnapshot(), values);
      } else if (metric instanceof Timer) {
        Timer timer = (Timer) metric;
        exportRateToIoTDBFormat(writePrefix, timer.getImmutableRate(), values);
        exportHistogramSnapshotToIoTDBFormat(writePrefix, timer.takeSnapshot(), values);
      }
    }
  }

  public static Map<Pair<String, String>, Object> exportMetricToIoTDBFormat(
      Map<Pair<String, String[]>, IMetric> metricMap) {
    Map<Pair<String, String>, Object> values = new LinkedHashMap<>();
    for (Map.Entry<Pair<String, String[]>, IMetric> entry : metricMap.entrySet()) {
      exportMetricToIoTDBFormat(
          entry.getValue(), values, entry.getKey().getLeft(), entry.getKey().getRight());
    }
    return values;
  }

  /** Export rate to IoTDB format */
  private static void exportRateToIoTDBFormat(
      String writePrefix, Rate rate, Map<Pair<String, String>, Object> values) {
    values.put(new Pair<>(writePrefix, "count"), rate.getCount());
    values.put(new Pair<>(writePrefix, "mean"), rate.getMeanRate());
    values.put(new Pair<>(writePrefix, "m1"), rate.getOneMinuteRate());
    values.put(new Pair<>(writePrefix, "m5"), rate.getFiveMinuteRate());
    values.put(new Pair<>(writePrefix, "m15"), rate.getFifteenMinuteRate());
  }

  /** Export histogramSnapshot to IoTDB format */
  private static void exportHistogramSnapshotToIoTDBFormat(
      String writePrefix, HistogramSnapshot snapshot, Map<Pair<String, String>, Object> values) {
    values.put(new Pair<>(writePrefix, "min"), snapshot.getMin());
    values.put(new Pair<>(writePrefix, "p50"), snapshot.getMedian());
    values.put(new Pair<>(writePrefix, "p75"), snapshot.getValue(0.75));
    values.put(new Pair<>(writePrefix, "p90"), snapshot.getValue(0.90));
    values.put(new Pair<>(writePrefix, "p95"), snapshot.getValue(0.95));
    values.put(new Pair<>(writePrefix, "p99"), snapshot.getValue(0.99));
    values.put(new Pair<>(writePrefix, "max"), snapshot.getMax());
    values.put(new Pair<>(writePrefix, "mean"), snapshot.getMean());
  }
}
