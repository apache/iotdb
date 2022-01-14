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

import org.apache.iotdb.metrics.utils.MonitorType;
import org.apache.iotdb.metrics.utils.ReporterType;

import java.util.Arrays;
import java.util.List;

public class MetricConfig {
  /** enable publishing data. */
  private Boolean enableMetric = false;

  /** The period of data pushed by the reporter to the remote monitoring system. */
  private Integer pushPeriodInSecond = 5;

  /** The of monitor frame */
  private MonitorType monitorType = MonitorType.micrometer;

  /** provide or push metric data to remote system, could be jmx, prometheus, iotdb, etc. */
  private List<ReporterType> metricReporterList =
      Arrays.asList(ReporterType.jmx, ReporterType.prometheus);

  /** the config of prometheus reporter */
  private PrometheusReporterConfig prometheusReporterConfig = new PrometheusReporterConfig();

  public Boolean getEnableMetric() {
    return enableMetric;
  }

  public void setEnableMetric(Boolean enableMetric) {
    this.enableMetric = enableMetric;
  }

  public Integer getPushPeriodInSecond() {
    return pushPeriodInSecond;
  }

  public void setPushPeriodInSecond(Integer pushPeriodInSecond) {
    this.pushPeriodInSecond = pushPeriodInSecond;
  }

  public MonitorType getMonitorType() {
    return monitorType;
  }

  public void setMonitorType(MonitorType monitorType) {
    this.monitorType = monitorType;
  }

  public List<ReporterType> getMetricReporterList() {
    return metricReporterList;
  }

  public void setMetricReporterList(List<ReporterType> metricReporterList) {
    this.metricReporterList = metricReporterList;
  }

  public PrometheusReporterConfig getPrometheusReporterConfig() {
    return prometheusReporterConfig;
  }

  public void setPrometheusReporterConfig(PrometheusReporterConfig prometheusReporterConfig) {
    this.prometheusReporterConfig = prometheusReporterConfig;
  }

  /** the following is prometheus related config. */
  public static class PrometheusReporterConfig {
    private String prometheusExporterUrl = "http://localhost";
    /** the http server's port for prometheus exporter to get metric data. */
    private String prometheusExporterPort = "9091";

    public String getPrometheusExporterUrl() {
      return prometheusExporterUrl;
    }

    public void setPrometheusExporterUrl(String prometheusExporterUrl) {
      this.prometheusExporterUrl = prometheusExporterUrl;
    }

    public String getPrometheusExporterPort() {
      return prometheusExporterPort;
    }

    public void setPrometheusExporterPort(String prometheusExporterPort) {
      this.prometheusExporterPort = prometheusExporterPort;
    }
  }
}
