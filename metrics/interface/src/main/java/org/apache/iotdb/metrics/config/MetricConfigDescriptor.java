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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/** The utils class to load configure. Read from yaml file. */
public class MetricConfigDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(MetricConfigDescriptor.class);
  /** the metric config of metric service */
  private final MetricConfig metricConfig;

  private MetricConfigDescriptor() {
    metricConfig = loadProps();
  }

  /**
   * load property file into metric config. Use default values if not find.
   *
   * @return metric config
   */
  public MetricConfig loadProps() {
    MetricConfig metricConfig;
    String url = getPropsUrl();
    Constructor constructor = new Constructor(MetricConfig.class);
    Yaml yaml = new Yaml(constructor);
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(url)) {
        logger.info("Start to read config file {}", url);
        metricConfig = (MetricConfig) yaml.load(inputStream);
      } catch (IOException e) {
        logger.warn(
            "Fail to find config file : {} because of {}, use default config.",
            url,
            e.getMessage());
        metricConfig = new MetricConfig();
      }
    } else {
      logger.warn("Fail to find config file, use default config.");
      metricConfig = new MetricConfig();
    }
    return metricConfig;
  }

  /**
   * load property file into metric config, use default values if not find.
   *
   * @return reload level of metric service
   */
  public ReloadLevel loadHotProps() {
    MetricConfig newMetricConfig = loadProps();
    ReloadLevel reloadLevel = ReloadLevel.NOTHING;
    if (newMetricConfig != null && !metricConfig.equals(newMetricConfig)) {
      if (!metricConfig.getEnableMetric().equals(newMetricConfig.getEnableMetric())) {
        // start service or stop service.
        reloadLevel =
            (newMetricConfig.getEnableMetric())
                ? ReloadLevel.START_METRIC
                : ReloadLevel.STOP_METRIC;
      } else if (metricConfig.getEnableMetric()) {
        // restart reporters or restart service
        if (!metricConfig.getMonitorType().equals(newMetricConfig.getMonitorType())
            || !metricConfig.getMetricLevel().equals(newMetricConfig.getMetricLevel())
            || !metricConfig.getPredefinedMetrics().equals(newMetricConfig.getPredefinedMetrics())
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
    // second, try to get conf folder of datanode
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
          "Cannot find IOTDB_CONF and CONFIGNODE_CONF environment variable when loading "
              + "config file {}, use default configuration",
          MetricConstant.CONFIG_NAME);
      return null;
    } else {
      url += (File.separatorChar + MetricConstant.CONFIG_NAME);
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
