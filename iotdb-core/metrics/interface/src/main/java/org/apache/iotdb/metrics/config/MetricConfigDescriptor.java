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

package org.apache.iotdb.metrics.config;

import org.apache.iotdb.metrics.config.MetricConfig.IoTDBReporterConfig;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.ReporterType;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/** The utils class to load properties. */
public class MetricConfigDescriptor {
  /** The metric config of metric service. */
  private static final MetricConfig metricConfig = new MetricConfig();

  private MetricConfigDescriptor() {
    // empty constructor
  }

  /** Load properties into metric config. */
  public void loadProps(Properties properties, boolean isConfigNode) {
    MetricConfig loadConfig = generateFromProperties(properties, isConfigNode);
    metricConfig.copy(loadConfig);
  }

  /**
   * Load properties into metric config when reload service.
   *
   * @return reload level of metric service
   */
  public ReloadLevel loadHotProps(Properties properties, boolean isConfigNode) {
    MetricConfig newMetricConfig = generateFromProperties(properties, isConfigNode);
    ReloadLevel reloadLevel = ReloadLevel.NOTHING;
    if (!metricConfig.equals(newMetricConfig)) {
      if (!metricConfig.getMetricLevel().equals(newMetricConfig.getMetricLevel())
          || !metricConfig
              .getAsyncCollectPeriodInSecond()
              .equals(newMetricConfig.getAsyncCollectPeriodInSecond())) {
        // restart metric service
        reloadLevel = ReloadLevel.RESTART_METRIC;
      } else if (!metricConfig
          .getInternalReportType()
          .equals(newMetricConfig.getInternalReportType())) {
        // restart internal reporter
        reloadLevel = ReloadLevel.RESTART_INTERNAL_REPORTER;
      } else {
        // restart reporters
        reloadLevel = ReloadLevel.RESTART_REPORTER;
      }
      metricConfig.copy(newMetricConfig);
    }
    return reloadLevel;
  }

  /** Load properties into metric config. */
  private MetricConfig generateFromProperties(Properties properties, boolean isConfigNode) {
    MetricConfig loadConfig = new MetricConfig();

    String reporterList =
        getProperty(
            "metric_reporter_list",
            String.join(
                ",",
                loadConfig.getMetricReporterList().stream()
                    .map(ReporterType::toString)
                    .collect(Collectors.toSet())),
            properties,
            isConfigNode);
    loadConfig.setMetricReporterList(reporterList);

    loadConfig.setMetricLevel(
        MetricLevel.valueOf(
            getProperty(
                "metric_level",
                String.valueOf(loadConfig.getMetricLevel()),
                properties,
                isConfigNode)));

    loadConfig.setAsyncCollectPeriodInSecond(
        Integer.parseInt(
            getProperty(
                "metric_async_collect_period",
                String.valueOf(loadConfig.getAsyncCollectPeriodInSecond()),
                properties,
                isConfigNode)));

    loadConfig.setPrometheusReporterPort(
        Integer.parseInt(
            getProperty(
                "metric_prometheus_reporter_port",
                String.valueOf(loadConfig.getPrometheusReporterPort()),
                properties,
                isConfigNode)));

    IoTDBReporterConfig reporterConfig = loadConfig.getIoTDBReporterConfig();
    reporterConfig.setHost(
        getProperty(
            "metric_iotdb_reporter_host", reporterConfig.getHost(), properties, isConfigNode));

    reporterConfig.setPort(
        Integer.valueOf(
            getProperty(
                "metric_iotdb_reporter_port",
                String.valueOf(reporterConfig.getPort()),
                properties,
                isConfigNode)));

    reporterConfig.setUsername(
        getProperty(
            "metric_iotdb_reporter_username",
            reporterConfig.getUsername(),
            properties,
            isConfigNode));

    reporterConfig.setPassword(
        getProperty(
            "metric_iotdb_reporter_password",
            reporterConfig.getPassword(),
            properties,
            isConfigNode));

    reporterConfig.setMaxConnectionNumber(
        Integer.valueOf(
            getProperty(
                "metric_iotdb_reporter_max_connection_number",
                String.valueOf(reporterConfig.getMaxConnectionNumber()),
                properties,
                isConfigNode)));

    reporterConfig.setLocation(
        getProperty(
            "metric_iotdb_reporter_location",
            reporterConfig.getLocation(),
            properties,
            isConfigNode));

    reporterConfig.setPushPeriodInSecond(
        Integer.valueOf(
            getProperty(
                "metric_iotdb_reporter_push_period",
                String.valueOf(reporterConfig.getPushPeriodInSecond()),
                properties,
                isConfigNode)));
    if (!isConfigNode) {
      loadConfig.setInternalReportType(
          InternalReporterType.valueOf(
              properties.getProperty(
                  "dn_metric_internal_reporter_type",
                  loadConfig.getInternalReportType().toString())));
    }

    return loadConfig;
  }

  /** Get property from confignode or datanode. */
  private String getProperty(
      String target, String defaultValue, Properties properties, boolean isConfigNode) {
    return Optional.ofNullable(
            properties.getProperty((isConfigNode ? "cn_" : "dn_") + target, defaultValue))
        .map(String::trim)
        .orElse(defaultValue);
  }

  private static class MetricConfigDescriptorHolder {
    private static final MetricConfigDescriptor INSTANCE = new MetricConfigDescriptor();
  }

  public static MetricConfigDescriptor getInstance() {
    return MetricConfigDescriptorHolder.INSTANCE;
  }

  public MetricConfig getMetricConfig() {
    return metricConfig;
  }
}
