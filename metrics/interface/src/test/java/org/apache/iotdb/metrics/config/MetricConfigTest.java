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

import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.MetricFrameType;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class MetricConfigTest {
  @Test
  public void testConfigNodeMetricConfig() {
    Properties properties = new Properties();
    properties.setProperty("cn_enable_metric", "true");
    properties.setProperty("cn_enable_performance_stat", "true");
    properties.setProperty("cn_metric_reporter_list", "JMX,PROMETHEUS,IOTDB");
    properties.setProperty("cn_metric_frame_type", "DROPWIZARD");
    properties.setProperty("cn_metric_level", "ALL");
    properties.setProperty("cn_metric_async_collect_period", "10");
    properties.setProperty("cn_metric_prometheus_reporter_port", "9090");
    properties.setProperty("cn_metric_iotdb_reporter_host", "0.0.0.0");
    properties.setProperty("cn_metric_iotdb_reporter_port", "6669");
    properties.setProperty("cn_metric_iotdb_reporter_username", "user");
    properties.setProperty("cn_metric_iotdb_reporter_password", "password");
    properties.setProperty("cn_metric_iotdb_reporter_max_connection_number", "1");
    properties.setProperty("cn_metric_iotdb_reporter_location", "metric");
    properties.setProperty("cn_metric_iotdb_reporter_push_period", "5");

    MetricConfigDescriptor.getInstance().loadProps(properties);

    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

    assertEquals(3, metricConfig.getMetricReporterList().size());
    assertEquals(MetricFrameType.DROPWIZARD, metricConfig.getMetricFrameType());
    assertEquals(MetricLevel.ALL, metricConfig.getMetricLevel());
    assertEquals(10, (int) metricConfig.getAsyncCollectPeriodInSecond());
    assertEquals(9090, (int) metricConfig.getPrometheusReporterPort());

    MetricConfig.IoTDBReporterConfig reporterConfig = metricConfig.getIotdbReporterConfig();
    assertEquals("0.0.0.0", reporterConfig.getHost());
    assertEquals(6669, (int) reporterConfig.getPort());
    assertEquals("user", reporterConfig.getUsername());
    assertEquals("password", reporterConfig.getPassword());
    assertEquals(1, (int) reporterConfig.getMaxConnectionNumber());
    assertEquals("metric", reporterConfig.getLocation());
    assertEquals(5, (int) reporterConfig.getPushPeriodInSecond());
  }

  @Test
  public void testDataNodeMetricConfig() {
    Properties properties = new Properties();
    properties.setProperty("dn_enable_metric", "true");
    properties.setProperty("dn_enable_performance_stat", "true");
    properties.setProperty("dn_metric_reporter_list", "JMX,PROMETHEUS,IOTDB");
    properties.setProperty("dn_metric_frame_type", "DROPWIZARD");
    properties.setProperty("dn_metric_level", "ALL");
    properties.setProperty("dn_metric_async_collect_period", "10");
    properties.setProperty("dn_metric_prometheus_reporter_port", "9090");
    properties.setProperty("dn_metric_iotdb_reporter_host", "0.0.0.0");
    properties.setProperty("dn_metric_iotdb_reporter_port", "6669");
    properties.setProperty("dn_metric_iotdb_reporter_username", "user");
    properties.setProperty("dn_metric_iotdb_reporter_password", "password");
    properties.setProperty("dn_metric_iotdb_reporter_max_connection_number", "1");
    properties.setProperty("dn_metric_iotdb_reporter_location", "metric");
    properties.setProperty("dn_metric_iotdb_reporter_push_period", "5");
    properties.setProperty("dn_metric_internal_reporter_type", "IOTDB");

    MetricConfigDescriptor.getInstance().loadProps(properties);

    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

    assertEquals(3, metricConfig.getMetricReporterList().size());
    assertEquals(MetricFrameType.DROPWIZARD, metricConfig.getMetricFrameType());
    assertEquals(MetricLevel.ALL, metricConfig.getMetricLevel());
    assertEquals(10, (int) metricConfig.getAsyncCollectPeriodInSecond());
    assertEquals(9090, (int) metricConfig.getPrometheusReporterPort());

    MetricConfig.IoTDBReporterConfig reporterConfig = metricConfig.getIotdbReporterConfig();
    assertEquals("0.0.0.0", reporterConfig.getHost());
    assertEquals(6669, (int) reporterConfig.getPort());
    assertEquals("user", reporterConfig.getUsername());
    assertEquals("password", reporterConfig.getPassword());
    assertEquals(1, (int) reporterConfig.getMaxConnectionNumber());
    assertEquals("metric", reporterConfig.getLocation());
    assertEquals(5, (int) reporterConfig.getPushPeriodInSecond());
    assertEquals(InternalReporterType.IOTDB, metricConfig.getInternalReportType());
  }
}
