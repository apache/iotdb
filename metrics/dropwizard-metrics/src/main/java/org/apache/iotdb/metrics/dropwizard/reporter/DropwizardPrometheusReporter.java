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

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.DropwizardMetricManager;
import org.apache.iotdb.metrics.dropwizard.reporter.prometheus.PrometheusReporter;
import org.apache.iotdb.metrics.dropwizard.reporter.prometheus.PushGateway;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.MetricFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DropwizardPrometheusReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardPrometheusReporter.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  private MetricManager dropwizardMetricManager = null;
  private PrometheusReporter prometheusReporter = null;

  @Override
  public boolean start() {
    if (prometheusReporter != null) {
      LOGGER.warn("Dropwizard Prometheus Reporter already start!");
      return false;
    }
    String url = metricConfig.getPrometheusReporterConfig().getPrometheusExporterUrl();
    String port = metricConfig.getPrometheusReporterConfig().getPrometheusExporterPort();
    PushGateway pushgateway = new PushGateway(url + ":" + port, "IoTDB-Metric{Dropwizard}");
    try {
      prometheusReporter =
          PrometheusReporter.forRegistry(
                  ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
              .prefixedWith("dropwizard:")
              .filter(MetricFilter.ALL)
              .build(pushgateway);
      prometheusReporter.start(metricConfig.getPushPeriodInSecond(), TimeUnit.SECONDS);
    } catch (Exception e) {
      LOGGER.error("Failed to start Dropwizard JmxReporter, because {}", e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public boolean stop() {
    if (prometheusReporter == null) {
      LOGGER.warn("Dropwizard Prometheus Reporter already stop!");
      return false;
    }
    prometheusReporter.stop();
    prometheusReporter = null;
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.prometheus;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.dropwizardMetricManager = metricManager;
  }
}
