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

import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MonitorType;

import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricConfigTest {

  @Test
  public void testYamlConfig() {
    String url = this.getClass().getClassLoader().getResource("iotdb-metric.yml").getPath();

    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
    Constructor constructor = new Constructor(MetricConfig.class);
    Yaml yaml = new Yaml(constructor);
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(url)) {
        metricConfig = (MetricConfig) yaml.load(inputStream);
      } catch (IOException e) {
        Assert.fail();
      }
    }

    assertTrue(metricConfig.getEnableMetric());
    assertTrue(metricConfig.getEnablePerformanceStat());
    assertEquals(3, metricConfig.getMetricReporterList().size());
    assertEquals(MonitorType.DROPWIZARD, metricConfig.getMonitorType());
    assertEquals(MetricLevel.ALL, metricConfig.getMetricLevel());
    assertEquals(5, metricConfig.getPredefinedMetrics().size());
    assertEquals(10, (int) metricConfig.getAsyncCollectPeriodInSecond());
    assertEquals(9090, (int) metricConfig.getPrometheusExporterPort());

    MetricConfig.IoTDBReporterConfig reporterConfig = metricConfig.getIoTDBReporterConfig();
    assertEquals("0.0.0.0", reporterConfig.getHost());
    assertEquals(6669, (int) reporterConfig.getPort());
    assertEquals("user", reporterConfig.getUsername());
    assertEquals("password", reporterConfig.getPassword());
    assertEquals(1, (int) reporterConfig.getMaxConnectionNumber());
    assertEquals("metric", reporterConfig.getDatabase());
    assertEquals(5, (int) reporterConfig.getPushPeriodInSecond());
  }
}
