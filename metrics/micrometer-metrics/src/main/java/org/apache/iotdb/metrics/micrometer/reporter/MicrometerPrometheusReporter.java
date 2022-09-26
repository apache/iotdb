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

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.channel.ChannelOption;
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

  private AbstractMetricManager metricManager;
  private DisposableServer httpServer;

  @Override
  public boolean start() {
    if (httpServer != null) {
      return false;
    }
    Set<MeterRegistry> meterRegistrySet =
        Metrics.globalRegistry.getRegistries().stream()
            .filter(reporter -> reporter instanceof PrometheusMeterRegistry)
            .collect(Collectors.toSet());
    PrometheusMeterRegistry prometheusMeterRegistry;
    if (meterRegistrySet.size() == 0) {
      prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      prometheusMeterRegistry.throwExceptionOnRegistrationFailure();
      Metrics.addRegistry(prometheusMeterRegistry);
    } else {
      prometheusMeterRegistry = (PrometheusMeterRegistry) meterRegistrySet.toArray()[0];
    }
    httpServer =
        HttpServer.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
            .port(metricConfig.getPrometheusExporterPort())
            .route(
                routes ->
                    routes.get(
                        "/metrics",
                        (request, response) ->
                            response.sendString(Mono.just(prometheusMeterRegistry.scrape()))))
            .bindNow();
    LOGGER.info(
        "http server for metrics started, listen on {}", metricConfig.getPrometheusExporterPort());
    return true;
  }

  @Override
  public boolean stop() {
    if (httpServer != null) {
      try {
        Set<MeterRegistry> meterRegistrySet =
            Metrics.globalRegistry.getRegistries().stream()
                .filter(reporter -> reporter instanceof PrometheusMeterRegistry)
                .collect(Collectors.toSet());
        for (MeterRegistry meterRegistry : meterRegistrySet) {
          meterRegistry.close();
          Metrics.removeRegistry(meterRegistry);
        }
        httpServer.disposeNow();
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

  @Override
  public void setMetricManager(AbstractMetricManager metricManager) {
    this.metricManager = metricManager;
  }
}
