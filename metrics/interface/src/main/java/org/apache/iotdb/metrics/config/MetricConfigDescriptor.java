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

import org.apache.iotdb.metrics.utils.MetricFrameType;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.stream.Collectors;

/** The utils class to load configure. Read from yaml file. */
public class MetricConfigDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(MetricConfigDescriptor.class);
  /** the metric config of metric service */
  private final MetricConfig metricConfig;

  private MetricConfigDescriptor() {
    metricConfig = new MetricConfig();
  }

  public void loadProps(Properties properties) {
    MetricConfig loadConfig = generateFromProperties(properties);
    metricConfig.copy(loadConfig);
  }

  /**
   * load property file into metric config, use default values if not find.
   *
   * @return reload level of metric service
   */
  public ReloadLevel loadHotProps(Properties properties) {
    MetricConfig newMetricConfig = generateFromProperties(properties);
    ReloadLevel reloadLevel = ReloadLevel.NOTHING;
    if (!metricConfig.equals(newMetricConfig)) {
      if (!metricConfig.getEnableMetric().equals(newMetricConfig.getEnableMetric())) {
        // start service or stop service.
        reloadLevel =
            (newMetricConfig.getEnableMetric())
                ? ReloadLevel.START_METRIC
                : ReloadLevel.STOP_METRIC;
      } else if (metricConfig.getEnableMetric()) {
        // restart reporters or restart service
        if (!metricConfig.getMetricFrameType().equals(newMetricConfig.getMetricFrameType())
            || !metricConfig.getMetricLevel().equals(newMetricConfig.getMetricLevel())
            || !metricConfig
                .getAsyncCollectPeriodInSecond()
                .equals(newMetricConfig.getAsyncCollectPeriodInSecond())) {
          reloadLevel = ReloadLevel.RESTART_METRIC;
        } else {
          reloadLevel = ReloadLevel.RESTART_REPORTER;
        }
      }
      metricConfig.copy(newMetricConfig);
    }
    return reloadLevel;
  }

  /** load properties into metric config */
  private MetricConfig generateFromProperties(Properties properties) {
    MetricConfig loadConfig = new MetricConfig();
    loadConfig.setEnableMetric(
        Boolean.parseBoolean(
            getProperty(
                "enable_metric", String.valueOf(loadConfig.getEnableMetric()), properties)));

    loadConfig.setEnablePerformanceStat(
        Boolean.parseBoolean(
            getProperty(
                "enable_performance_stat",
                String.valueOf(loadConfig.getEnablePerformanceStat()),
                properties)));

    String reporterList =
        getProperty(
            "metric_reporter_list",
            String.join(
                ",",
                loadConfig.getMetricReporterList().stream()
                    .map(ReporterType::toString)
                    .collect(Collectors.toSet())),
            properties);
    loadConfig.setMetricReporterList(reporterList);

    loadConfig.setMetricFrameType(
        MetricFrameType.valueOf(
            getProperty(
                "metric_frame_type", String.valueOf(loadConfig.getMetricFrameType()), properties)));

    loadConfig.setMetricLevel(
        MetricLevel.valueOf(
            getProperty("metric_level", String.valueOf(loadConfig.getMetricLevel()), properties)));

    loadConfig.setAsyncCollectPeriodInSecond(
        Integer.parseInt(
            getProperty(
                "metric_async_collect_period",
                String.valueOf(loadConfig.getAsyncCollectPeriodInSecond()),
                properties)));

    loadConfig.setPrometheusReporterPort(
        Integer.parseInt(
            getProperty(
                "metric_prometheus_reporter_port",
                String.valueOf(loadConfig.getPrometheusReporterPort()),
                properties)));

    MetricConfig.IoTDBReporterConfig reporterConfig = loadConfig.getIoTDBReporterConfig();
    reporterConfig.setHost(
        getProperty("iotdb_reporter_host", reporterConfig.getHost(), properties));

    reporterConfig.setPort(
        Integer.valueOf(
            getProperty(
                "iotdb_reporter_port", String.valueOf(reporterConfig.getPort()), properties)));

    reporterConfig.setUsername(
        getProperty("iotdb_reporter_username", reporterConfig.getUsername(), properties));

    reporterConfig.setPassword(
        getProperty("iotdb_reporter_password", reporterConfig.getPassword(), properties));

    reporterConfig.setMaxConnectionNumber(
        Integer.valueOf(
            getProperty(
                "iotdb_reporter_max_connection_number",
                String.valueOf(reporterConfig.getMaxConnectionNumber()),
                properties)));

    reporterConfig.setLocation(
        getProperty("iotdb_reporter_location", reporterConfig.getLocation(), properties));

    reporterConfig.setPushPeriodInSecond(
        Integer.valueOf(
            getProperty(
                "iotdb_reporter_push_period",
                String.valueOf(reporterConfig.getPushPeriodInSecond()),
                properties)));

    return loadConfig;
  }

  /** Try to get property from confignode or datanode */
  private String getProperty(String target, String defaultValue, Properties properties) {
    return properties.getProperty(
        "dn_" + target, properties.getProperty("cn_" + target, defaultValue));
  }

  /** get the path of metric config file. */
  private String getPropsUrl() {
    // first, try to get conf folder of standalone iotdb or datanode
    String url = System.getProperty(MetricConstant.IOTDB_CONF, null);
    if (url == null) {
      // try to get conf folder from IOTDB_HOME
      url = System.getProperty(MetricConstant.IOTDB_HOME, null);
      if (url != null) {
        url += File.separator + "conf";
      }
    }

    if (url == null) {
      logger.warn(
          "Cannot find IOTDB_CONF environment variable when loading "
              + "config file {}, use default configuration",
          MetricConstant.DATANODE_CONFIG_NAME);
    } else {
      url += (File.separatorChar + MetricConstant.DATANODE_CONFIG_NAME);
      if (new File(url).exists()) {
        return url;
      } else {
        url = null;
      }
    }

    // second, try to get conf folder of confignode
    if (url == null) {
      url = System.getProperty(MetricConstant.CONFIGNODE_CONF, null);
      if (url == null) {
        // try to get conf folder from CONFIGNODE_HOME
        url = System.getProperty(MetricConstant.CONFIGNODE_HOME, null);
        if (url != null) {
          url += File.separator + "conf";
        }
      }
    }

    // finally, return null when not find
    if (url == null) {
      logger.warn(
          "Cannot find CONFIGNODE_CONF environment variable when loading "
              + "config file {}, use default configuration",
          MetricConstant.CONFIG_NODE_CONFIG_NAME);
      return null;
    } else {
      url += (File.separatorChar + MetricConstant.CONFIG_NODE_CONFIG_NAME);
    }

    return url;
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
