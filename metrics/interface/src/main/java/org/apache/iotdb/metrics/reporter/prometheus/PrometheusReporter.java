/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.reporter.prometheus;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.netty.channel.ChannelOption;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PrometheusReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private AbstractMetricManager metricManager;
  private DisposableServer httpServer;

  public PrometheusReporter(AbstractMetricManager metricManager) {
    this.metricManager = metricManager;
  }

  @Override
  public boolean start() {
    if (httpServer != null) {
      return false;
    }
    httpServer =
        HttpServer.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
            .channelGroup(new DefaultChannelGroup(GlobalEventExecutor.INSTANCE))
            .port(metricConfig.getPrometheusReporterPort())
            .route(
                routes ->
                    routes.get(
                        "/metrics",
                        (request, response) -> response.sendString(Mono.just(scrape()))))
            .bindNow();
    LOGGER.info(
        "http server for metrics started, listen on {}", metricConfig.getPrometheusReporterPort());
    return true;
  }

  private String scrape() {
    Writer writer = new StringWriter();
    PrometheusTextWriter prometheusTextWriter = new PrometheusTextWriter(writer);

    String result;
    try {
      for (Map.Entry<MetricInfo, IMetric> metricEntry : metricManager.getAllMetrics().entrySet()) {
        MetricInfo metricInfo = metricEntry.getKey();
        IMetric metric = metricEntry.getValue();

        String name = metricInfo.getName().replaceAll("[^a-zA-Z0-9:_\\]\\[]", "_");
        MetricType metricType = metricInfo.getMetaInfo().getType();
        if (metric instanceof Counter) {
          name += "_total";
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          Counter counter = (Counter) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), counter.count());
        } else if (metric instanceof Gauge) {
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          Gauge gauge = (Gauge) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), gauge.value());
        } else if (metric instanceof AutoGauge) {
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          AutoGauge gauge = (AutoGauge) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), gauge.value());
        } else if (metric instanceof Histogram) {
          Histogram histogram = (Histogram) metric;
          HistogramSnapshot snapshot = histogram.takeSnapshot();
          writeSnapshotAndCount(
              name,
              metricInfo.getTags(),
              metricType,
              snapshot,
              histogram.count(),
              prometheusTextWriter);
        } else if (metric instanceof Rate) {
          name += "_total";
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          Rate rate = (Rate) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), rate.getCount());
          prometheusTextWriter.writeSample(
              name, addTags(metricInfo.getTags(), "rate", "m1"), rate.getOneMinuteRate());
          prometheusTextWriter.writeSample(
              name, addTags(metricInfo.getTags(), "rate", "m5"), rate.getFiveMinuteRate());
          prometheusTextWriter.writeSample(
              name, addTags(metricInfo.getTags(), "rate", "m15"), rate.getFifteenMinuteRate());
          prometheusTextWriter.writeSample(
              name, addTags(metricInfo.getTags(), "rate", "mean"), rate.getMeanRate());
        } else if (metric instanceof Timer) {
          Timer timer = (Timer) metric;
          HistogramSnapshot snapshot = timer.takeSnapshot();
          name += "_seconds";
          writeSnapshotAndCount(
              name,
              metricInfo.getTags(),
              metricType,
              snapshot,
              timer.getImmutableRate().getCount(),
              prometheusTextWriter);
        }
      }
      result = writer.toString();
    } catch (IOException e) {
      // This actually never happens since StringWriter::write() doesn't throw any IOException
      throw new RuntimeException(e);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        // do nothing
      }
    }
    return result;
  }

  private void writeSnapshotAndCount(
      String name,
      Map<String, String> tags,
      MetricType type,
      HistogramSnapshot snapshot,
      long count,
      PrometheusTextWriter prometheusTextWriter)
      throws IOException {
    prometheusTextWriter.writeHelp(name);
    prometheusTextWriter.writeType(name, type);
    prometheusTextWriter.writeSample(name + "_max", tags, snapshot.getMax());
    prometheusTextWriter.writeSample(
        name + "_sum", tags, Arrays.stream(snapshot.getValues()).sum());
    prometheusTextWriter.writeSample(name + "_count", tags, count);

    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "0.0"), snapshot.getValue(0.0));
    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "0.25"), snapshot.getValue(0.25));
    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "0.5"), snapshot.getValue(0.5));
    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "0.75"), snapshot.getValue(0.75));
    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "1.0"), snapshot.getValue(1.0));
  }

  private Map<String, String> addTags(Map<String, String> tags, String key, String value) {
    HashMap<String, String> result = new HashMap<>(tags);
    result.put(key, value);
    return result;
  }

  private static String getHelpMessage(String metric, MetricType type) {
    return String.format(
        "Generated from metric import (metric=%s, type=%s)", metric, type.toString());
  }

  @Override
  public boolean stop() {
    if (httpServer != null) {
      try {
        httpServer.disposeNow(Duration.ofSeconds(10));
        httpServer = null;
      } catch (Exception e) {
        LOGGER.error("failed to stop server", e);
        return false;
      }
    }
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.PROMETHEUS;
  }
}
