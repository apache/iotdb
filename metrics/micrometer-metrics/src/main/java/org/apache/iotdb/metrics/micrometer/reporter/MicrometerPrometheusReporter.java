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

package org.apache.iotdb.metrics.micrometer.reporter;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.Set;
import java.util.stream.Collectors;

public class MicrometerPrometheusReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerPrometheusReporter.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  private MetricManager metricManager;
  private Thread runThread;

  @Override
  public boolean start() {
    Set<MeterRegistry> meterRegistrySet =
        Metrics.globalRegistry.getRegistries().stream()
            .filter(reporter -> reporter instanceof PrometheusMeterRegistry)
            .collect(Collectors.toSet());
    if (meterRegistrySet.size() != 1) {
      LOGGER.error("Too less or too many prometheusReporters");
      return false;
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
    return true;
  }

  @Override
  public boolean stop() {
    try {
      // stop prometheus reporter
      if (runThread != null) {
        runThread.join();
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Failed to stop micrometer prometheus reporter", e);
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return null;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.metricManager = metricManager;
  }
}
