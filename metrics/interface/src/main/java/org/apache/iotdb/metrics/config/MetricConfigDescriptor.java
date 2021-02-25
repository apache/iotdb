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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class MetricConfigDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(MetricConfigDescriptor.class);
  private final MetricConfig metricConfig = new MetricConfig();

  public MetricConfig getMetricConfig() {
    return metricConfig;
  }

  private MetricConfigDescriptor() {
    loadProps();
  }

  public static MetricConfigDescriptor getInstance() {
    return MetricConfigDescriptorHolder.INSTANCE;
  }

  public String getPropsUrl() {
    String url = System.getProperty(MetricConstant.METRIC_CONF, null);
    if (url == null) {
      url = System.getProperty(MetricConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + MetricConfig.CONFIG_NAME;
      } else {
        logger.warn(
            "Cannot find IOTDB_HOME or METRIC_CONF environment variable when loading "
                + "config file {}, use default configuration",
            MetricConfig.CONFIG_NAME);
        // update all data seriesPath
        return null;
      }
    } else {
      url += (File.separatorChar + MetricConfig.CONFIG_NAME);
    }
    return url;
  }

  /** load an property file and set TsfileDBConfig variables. */
  private void loadProps() {

    String url = getPropsUrl();
    Properties properties = System.getProperties();
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(new File(url))) {
        logger.info("Start to read config file {}", url);
        properties.load(inputStream);
      } catch (IOException e) {
        logger.warn("Fail to find config file {}", url, e);
      }
    }

    metricConfig.setEnabled(
        Boolean.parseBoolean(
            properties.getProperty("enable_metric", Boolean.toString(metricConfig.isEnabled()))));

    String reporterList = properties.getProperty("metric_reporter_list");
    if (reporterList != null) {
      metricConfig.setReporterList(getReporterList(reporterList));
    }

    metricConfig.setPushPeriodInSecond(
        Integer.parseInt(
            properties.getProperty(
                "push_period_in_second", Integer.toString(metricConfig.getPushPeriodInSecond()))));

    metricConfig.setPrometheusExporterPort(
        properties.getProperty(
            "prometheus_exporter_port", metricConfig.getPrometheusExporterPort()));

    metricConfig.setIotdbIp(properties.getProperty("iotdb_ip", metricConfig.getIotdbIp()));

    metricConfig.setIotdbPort(properties.getProperty("iotdb_port", metricConfig.getIotdbPort()));

    metricConfig.setIotdbSg(properties.getProperty("iotdb_sg", metricConfig.getIotdbSg()));
    metricConfig.setIotdbUser(properties.getProperty("iotdb_user", metricConfig.getIotdbUser()));
    metricConfig.setIotdbPasswd(
        properties.getProperty("iotdb_passwd", metricConfig.getIotdbPasswd()));
  }

  private List<String> getReporterList(String reporterList) {
    if (reporterList == null) {
      return Collections.emptyList();
    }
    List<String> reporters = new ArrayList<>();
    String[] split = reporterList.split(",");
    for (String reporter : split) {
      reporter = reporter.trim();
      if ("".equals(reporter)) {
        continue;
      }
      reporters.add(reporter);
    }
    return reporters;
  }

  private static class MetricConfigDescriptorHolder {

    private static final MetricConfigDescriptor INSTANCE = new MetricConfigDescriptor();
  }
}
