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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.CompositeReporter;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.Prometheus.PrometheusReporter;
import org.apache.iotdb.metrics.dropwizard.Prometheus.PushGateway;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.jmx.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DropwizardCompositeReporter implements CompositeReporter {
  private static final Logger logger = LoggerFactory.getLogger(DropwizardCompositeReporter.class);

  private MetricManager dropwizardMetricManager;
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  private JmxReporter jmxReporter = null;
  private PrometheusReporter prometheusReporter = null;

  @Override
  public boolean start() {
    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      if (!start(reporter)) {
        logger.error("Dropwizard start reporter{" + reporter + "} failed.");
      }
    }
    return true;
  }

  @Override
  public boolean start(String reporter) {
    switch (ReporterType.get(reporter)) {
      case JMX:
        if (!startJmxReporter()) {
          logger.warn("Dropwizard already has reporter: " + reporter);
        }
        break;
      case IOTDB:
        break;
      case PROMETHEUS:
        if (!startPrometheusReporter()) {
          logger.warn("Dropwizard already has reporter: " + reporter);
        }
        break;
      default:
        logger.warn("Dropwizard don't support reporter type {}", reporter);
        return false;
    }
    return true;
  }

  private boolean startPrometheusReporter() {
    if (prometheusReporter != null) {
      return false;
    }
    String url = metricConfig.getPrometheusReporterConfig().getPrometheusExporterUrl();
    String port = metricConfig.getPrometheusReporterConfig().getPrometheusExporterPort();
    PushGateway pushgateway = new PushGateway(url + ":" + port, "Dropwizard");
    try {
      prometheusReporter =
          PrometheusReporter.forRegistry(
                  ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
              .prefixedWith("test:")
              .filter(MetricFilter.ALL)
              .build(pushgateway);
      prometheusReporter.start(metricConfig.getPushPeriodInSecond(), TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error(e.getMessage());
      return false;
    }
    return true;
  }

  private boolean startJmxReporter() {
    if (jmxReporter != null) {
      return false;
    }
    try {
      jmxReporter =
          JmxReporter.forRegistry(
                  ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
              .build();
    } catch (Exception e) {
      logger.error(e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public boolean stop() {
    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      stop(reporter);
    }
    return true;
  }

  @Override
  public boolean stop(String reporter) {
    switch (ReporterType.get(reporter)) {
      case JMX:
        if (!stopJmxReporter()) {
          return false;
        }
        break;
      case IOTDB:
        break;
      case PROMETHEUS:
        if (!stopPrometheusReporter()) {
          return false;
        }
        break;
      default:
        logger.warn("Dropwizard don't support reporter type {}", reporter);
        return false;
    }
    return true;
  }

  private boolean stopJmxReporter() {
    if (jmxReporter != null) {
      jmxReporter.stop();
      jmxReporter = null;
      return true;
    }
    return false;
  }

  private boolean stopPrometheusReporter() {
    if (prometheusReporter != null) {
      prometheusReporter.stop();
      prometheusReporter = null;
      return true;
    }
    return false;
  }

  /**
   * set manager to reporter
   *
   * @param metricManager
   */
  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.dropwizardMetricManager = metricManager;
  }

  @Override
  public String getName() {
    return "DropwizardMetricReporter";
  }
}
