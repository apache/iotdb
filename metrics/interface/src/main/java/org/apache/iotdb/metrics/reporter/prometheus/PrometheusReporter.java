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
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
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
import java.time.Duration;
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
    PrometheusTextWriter writer = new PrometheusTextWriter(new StringWriter());

    String result;
    try {
      for (Map.Entry<MetricInfo, IMetric> metricEntry : metricManager.getAllMetrics().entrySet()) {
        MetricInfo metricInfo = metricEntry.getKey();
        IMetric metric = metricEntry.getValue();

        String name = metricInfo.getName();
        writer.writeHelp(name, getHelpMessage(name, metric));
        writer.writeType(name, metricInfo.getMetaInfo().getType());
        Map<String, Object> values = new HashMap<>();
        metric.constructValueMap(values);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
          writer.writeSample(metricInfo.getName(), metricInfo.getTags(), entry.getValue());
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

  private static String getHelpMessage(String metricName, IMetric metric) {
    return String.format(
        "Generated from metric import (metric=%s, type=%s)",
        metricName, metric.getClass().getName());
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
