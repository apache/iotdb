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

import org.apache.iotdb.metrics.CompositeReporter;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MicrometerCompositeReporter implements CompositeReporter {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerCompositeReporter.class);
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final String NAME = "MicrometerMetricReporter";
  private Thread runThread;

  @Override
  public boolean start() {

    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      if (!start(reporter)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean start(String reporter) {
    switch (ReporterType.get(reporter)) {
      case JMX:
        startJmxReporter();
        break;
      case PROMETHEUS:
        startPrometheusReporter();
        break;
      default:
        logger.warn("Dropwizard don't support reporter type {}", reporter);
        return false;
    }
    return true;
  }

  private void startPrometheusReporter() {
    Set<MeterRegistry> meterRegistrySet =
        Metrics.globalRegistry.getRegistries().stream()
            .filter(reporter -> reporter instanceof PrometheusMeterRegistry)
            .collect(Collectors.toSet());
    if (meterRegistrySet.size() != 1) {
      logger.warn("Too many prometheusReporters");
    }
    PrometheusMeterRegistry prometheusMeterRegistry =
        (PrometheusMeterRegistry) meterRegistrySet.toArray()[0];
    DisposableServer server =
        HttpServer.create()
            .port(
                Integer.parseInt(
                    metricConfig.getPrometheusReporterConfig().getPrometheusExporterPort()))
            .route(
                routes ->
                    routes.get(
                        "/prometheus",
                        (request, response) ->
                            response.sendString(Mono.just(prometheusMeterRegistry.scrape()))))
            .bindNow();

    runThread = new Thread(server::onDispose);
    runThread.start();
  }

  private void startJmxReporter() {
    Set<MeterRegistry> meterRegistrySet =
        Metrics.globalRegistry.getRegistries().stream()
            .filter(reporter -> reporter instanceof JmxMeterRegistry)
            .collect(Collectors.toSet());
    for (MeterRegistry meterRegistry : meterRegistrySet) {
      ((JmxMeterRegistry) meterRegistry).start();
    }
  }

  @Override
  public boolean stop() {
    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      if (!stop(reporter)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean stop(String reporter) {
    switch (ReporterType.get(reporter)) {
      case JMX:
        stopJmxReporter();
        break;
      case PROMETHEUS:
        stopPrometheusReporter();
        break;
      default:
        logger.warn("Dropwizard don't support reporter type {}", reporter);
        return false;
    }
    return true;
  }

  private void stopPrometheusReporter() {
    try {
      // stop prometheus reporter
      if (runThread != null) {
        runThread.join();
      }
    } catch (InterruptedException e) {
      logger.warn("Failed to stop prometheus reporter", e);
      Thread.currentThread().interrupt();
    }
  }

  private void stopJmxReporter() {
    Set<MeterRegistry> meterRegistrySet =
        Metrics.globalRegistry.getRegistries().stream()
            .filter(reporter -> reporter instanceof JmxMeterRegistry)
            .collect(Collectors.toSet());
    for (MeterRegistry meterRegistry : meterRegistrySet) {
      ((JmxMeterRegistry) meterRegistry).stop();
    }
  }

  /**
   * set manager to reporter
   *
   * @param metricManager
   */
  @Override
  public void setMetricManager(MetricManager metricManager) {}

  @Override
  public String getName() {
    return NAME;
  }
}
