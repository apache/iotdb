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

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.DropwizardMetricManager;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Duration;

public class DropwizardPrometheusReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardPrometheusReporter.class);

  private AbstractMetricManager dropwizardMetricManager = null;
  private DisposableServer httpServer = null;

  @Override
  public boolean start() {
    if (httpServer != null) {
      return false;
    }
    int port = MetricConfigDescriptor.getInstance().getMetricConfig().getPrometheusExporterPort();
    httpServer =
        HttpServer.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
            .port(port)
            .route(
                routes ->
                    routes.get(
                        "/metrics",
                        (request, response) -> response.sendString(Mono.just(scrape()))))
            .bindNow();

    LOGGER.info("http server for metrics started, listen on {}", port);
    return true;
  }

  private String scrape() {
    MetricRegistry metricRegistry =
        ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry();
    Writer writer = new StringWriter();
    PrometheusTextWriter prometheusTextWriter = new PrometheusTextWriter(writer);
    DropwizardMetricsExporter dropwizardMetricsExporter =
        new DropwizardMetricsExporter(metricRegistry, prometheusTextWriter);
    String result = "";
    try {
      dropwizardMetricsExporter.scrape();
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

  @Override
  public void setMetricManager(AbstractMetricManager metricManager) {
    this.dropwizardMetricManager = metricManager;
  }
}
