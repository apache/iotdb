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
  private MetricConfig metricConfig;

  public MetricConfig getMetricConfig() {
    return metricConfig;
  }

  private MetricConfigDescriptor() {
    loadProps();
  }

  public static MetricConfigDescriptor getInstance() {
    return MetricConfigDescriptorHolder.INSTANCE;
  }

  /**
   * find the config file path.
   *
   * @return the file path
   */
  private String getPropsUrl() {
    String url = System.getProperty(MetricConstant.IOTDB_CONF, null);
    if (url == null) {
      logger.warn(
          "Cannot find IOTDB_CONF environment variable when loading "
              + "config file {}, use default configuration",
          MetricConstant.CONFIG_NAME);
      return null;
    } else {
      url += (File.separatorChar + MetricConstant.CONFIG_NAME);
    }
    return url;
  }

  /** Load a property file and set MetricConfig variables. If not found file, use default value. */
  public void loadProps() {
    String url = getPropsUrl();
    Constructor constructor = new Constructor(MetricConfig.class);
    Yaml yaml = new Yaml(constructor);
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(new File(url))) {
        logger.info("Start to read config file {}", url);
        metricConfig = (MetricConfig) yaml.load(inputStream);
        return;
      } catch (IOException e) {
        logger.warn("Fail to find config file : {}, use default config.", url, e);
      }
    } else {
      logger.warn("Fail to find config file, use default");
    }
    metricConfig = new MetricConfig();
  }

  public ReloadLevel loadHotProperties() {
    String url = getPropsUrl();
    Constructor constructor = new Constructor(MetricConfig.class);
    Yaml yaml = new Yaml(constructor);
    MetricConfig newMetricConfig = null;
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(new File(url))) {
        logger.info("Start to read config file {}", url);
        newMetricConfig = (MetricConfig) yaml.load(inputStream);
      } catch (IOException e) {
        logger.warn("Fail to find config file : {}, use default", url, e);
      }
    } else {
      logger.warn("Fail to find config file, use default");
    }
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
            || !metricConfig
                .getPredefinedMetrics()
                .equals(newMetricConfig.getPredefinedMetrics())) {
          reloadLevel = ReloadLevel.RESTART_METRIC;
        } else {
          reloadLevel = ReloadLevel.RESTART_REPORTER;
        }
      }
      metricConfig.copy(newMetricConfig);
    }
    return reloadLevel;
  }

  private static class MetricConfigDescriptorHolder {
    private static final MetricConfigDescriptor INSTANCE = new MetricConfigDescriptor();
  }
}
