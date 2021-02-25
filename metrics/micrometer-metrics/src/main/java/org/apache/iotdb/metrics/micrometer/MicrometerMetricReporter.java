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

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricReporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;

public class MicrometerMetricReporter implements MetricReporter {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerMetricReporter.class);
  private MetricManager micrometerMetricManager;
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  private Thread runThread;

  private JmxMeterRegistry jmxMeterRegistry;

  @Override
  public boolean start() {
    List<String> reporters = metricConfig.getReporterList();
    for (String reporter : reporters) {
      switch (ReporterType.get(reporter)) {
        case JMX:
          startJmxReporter(
              ((MicrometerMetricManager) micrometerMetricManager).getJmxMeterRegistry());
          break;
        case IOTDB:
          break;
        case PROMETHEUS:
          startPrometheusReporter(
              ((MicrometerMetricManager) micrometerMetricManager).getPrometheusMeterRegistry());
          break;
        default:
          logger.warn("Dropwizard don't support reporter type {}", reporter);
      }
    }

    return true;
  }

  private void startPrometheusReporter(PrometheusMeterRegistry prometheusMeterRegistry) {
    try {
      HttpServer server =
          HttpServer.create(
              new InetSocketAddress(Integer.parseInt(metricConfig.getPrometheusExporterPort())), 0);
      server.createContext(
          "/prometheus",
          httpExchange -> {
            String response = prometheusMeterRegistry.scrape();
            httpExchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = httpExchange.getResponseBody()) {
              os.write(response.getBytes());
            }
          });

      runThread = new Thread(server::start);
      runThread.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void startJmxReporter(JmxMeterRegistry jmxMeterRegistry) {
    logger.info("start jmx reporter from micrometer");
    //jmxMeterRegistry.start();
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    micrometerMetricManager = metricManager;
  }

  @Override
  public boolean stop() {
    try {
      runThread.join();
    } catch (InterruptedException e) {
      logger.warn("Failed to stop prometheus reporter", e);
    }

    ((MicrometerMetricManager) micrometerMetricManager).getJmxMeterRegistry().stop();
    return true;
  }
}
