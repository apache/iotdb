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
import org.apache.iotdb.metrics.utils.ReporterType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MetricConfig {

  /** Is the statistic of operation performance enabled */
  private Boolean enablePerformanceStat = false;

  /** The type of the implementation of metric framework */
  private MetricFrameType metricFrameType = MetricFrameType.MICROMETER;

  /** The list of reporters provide metrics for external tool */
  private List<ReporterType> metricReporterList = Collections.emptyList();

  /** The level of metric service */
  private MetricLevel metricLevel = MetricLevel.CORE;

  /** The period of async collection of some metrics in second */
  private Integer asyncCollectPeriodInSecond = 5;

  /** The export port for prometheus to get metrics */
  private Integer prometheusReporterPort = 9091;

  /** The iotdb config for iotdb reporter to push metric data */
  private IoTDBReporterConfig ioTDBReporterConfig = new IoTDBReporterConfig();

  /** The type of internal reporter */
  private InternalReporterType internalReporterType = InternalReporterType.MEMORY;

  /** The address of iotdb instance that is monitored */
  private String rpcAddress = "0.0.0.0";
  /** The port of iotdb instance that is monitored */
  private Integer rpcPort = 6667;

  public Boolean getEnablePerformanceStat() {
    return enablePerformanceStat;
  }

  public void setEnablePerformanceStat(Boolean enablePerformanceStat) {
    this.enablePerformanceStat = enablePerformanceStat;
  }

  public MetricFrameType getMetricFrameType() {
    return metricFrameType;
  }

  public void setMetricFrameType(MetricFrameType metricFrameType) {
    this.metricFrameType = metricFrameType;
  }

  public List<ReporterType> getMetricReporterList() {
    return metricReporterList;
  }

  public void setMetricReporterList(String metricReporterList) {
    this.metricReporterList = new ArrayList<>();
    for (String type : metricReporterList.split(",")) {
      if (type.trim().length() != 0) {
        this.metricReporterList.add(ReporterType.valueOf(type));
      }
    }
  }

  public InternalReporterType getInternalReportType() {
    return internalReporterType;
  }

  public void setInternalReportType(InternalReporterType internalReporterType) {
    this.internalReporterType = internalReporterType;
  }

  public MetricLevel getMetricLevel() {
    return metricLevel;
  }

  public void setMetricLevel(MetricLevel metricLevel) {
    this.metricLevel = metricLevel;
  }

  public Integer getAsyncCollectPeriodInSecond() {
    return asyncCollectPeriodInSecond;
  }

  public void setAsyncCollectPeriodInSecond(Integer asyncCollectPeriodInSecond) {
    this.asyncCollectPeriodInSecond = asyncCollectPeriodInSecond;
  }

  public Integer getPrometheusReporterPort() {
    return prometheusReporterPort;
  }

  public void setPrometheusReporterPort(Integer prometheusReporterPort) {
    this.prometheusReporterPort = prometheusReporterPort;
  }

  public IoTDBReporterConfig getIoTDBReporterConfig() {
    return ioTDBReporterConfig;
  }

  public void setIoTDBReporterConfig(IoTDBReporterConfig ioTDBReporterConfig) {
    this.ioTDBReporterConfig = ioTDBReporterConfig;
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  public Integer getRpcPort() {
    return rpcPort;
  }

  /** Update rpc address and rpc port of monitored node */
  public void updateRpcInstance(String rpcAddress, int rpcPort) {
    this.rpcAddress = rpcAddress;
    this.rpcPort = rpcPort;
  }

  /** Copy properties from another metric config */
  public void copy(MetricConfig newMetricConfig) {
    enablePerformanceStat = newMetricConfig.getEnablePerformanceStat();
    metricFrameType = newMetricConfig.getMetricFrameType();
    metricReporterList = newMetricConfig.getMetricReporterList();
    metricLevel = newMetricConfig.getMetricLevel();
    asyncCollectPeriodInSecond = newMetricConfig.getAsyncCollectPeriodInSecond();
    prometheusReporterPort = newMetricConfig.getPrometheusReporterPort();

    IoTDBReporterConfig newIoTDBReporterConfig = newMetricConfig.getIoTDBReporterConfig();
    ioTDBReporterConfig.setHost(newIoTDBReporterConfig.getHost());
    ioTDBReporterConfig.setPort(newIoTDBReporterConfig.getPort());
    ioTDBReporterConfig.setUsername(newIoTDBReporterConfig.getUsername());
    ioTDBReporterConfig.setPassword(newIoTDBReporterConfig.getPassword());
    ioTDBReporterConfig.setMaxConnectionNumber(newIoTDBReporterConfig.getMaxConnectionNumber());
    ioTDBReporterConfig.setPushPeriodInSecond(newIoTDBReporterConfig.getPushPeriodInSecond());
    ioTDBReporterConfig.setLocation(newIoTDBReporterConfig.getLocation());

    internalReporterType = newMetricConfig.getInternalReportType();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MetricConfig)) {
      return false;
    }
    MetricConfig anotherMetricConfig = (MetricConfig) obj;
    return enablePerformanceStat.equals(anotherMetricConfig.getEnablePerformanceStat())
        && metricFrameType.equals(anotherMetricConfig.getMetricFrameType())
        && metricReporterList.equals(anotherMetricConfig.getMetricReporterList())
        && metricLevel.equals(anotherMetricConfig.getMetricLevel())
        && asyncCollectPeriodInSecond.equals(anotherMetricConfig.getAsyncCollectPeriodInSecond())
        && prometheusReporterPort.equals(anotherMetricConfig.getPrometheusReporterPort())
        && ioTDBReporterConfig.equals(anotherMetricConfig.getIoTDBReporterConfig())
        && internalReporterType.equals(anotherMetricConfig.getInternalReportType());
  }

  public static class IoTDBReporterConfig {
    /** The host of iotdb that store metric value */
    private String host = "127.0.0.1";
    /** The port of iotdb that store metric value */
    private Integer port = 6667;
    /** The username of iotdb */
    private String username = "root";
    /** The password of iotdb */
    private String password = "root";
    /** The max number of connection */
    private Integer maxConnectionNumber = 3;
    /** The location of iotdb metrics */
    private String location = "metric";
    /** The period of data pushed by the reporter to the remote monitoring system. */
    private Integer pushPeriodInSecond = 15;

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public Integer getPort() {
      return port;
    }

    public void setPort(Integer port) {
      this.port = port;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public Integer getMaxConnectionNumber() {
      return maxConnectionNumber;
    }

    public void setMaxConnectionNumber(Integer maxConnectionNumber) {
      this.maxConnectionNumber = maxConnectionNumber;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Integer getPushPeriodInSecond() {
      return pushPeriodInSecond;
    }

    public void setPushPeriodInSecond(Integer pushPeriodInSecond) {
      this.pushPeriodInSecond = pushPeriodInSecond;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IoTDBReporterConfig that = (IoTDBReporterConfig) o;
      return Objects.equals(host, that.host)
          && Objects.equals(port, that.port)
          && Objects.equals(username, that.username)
          && Objects.equals(password, that.password)
          && Objects.equals(location, that.location)
          && Objects.equals(pushPeriodInSecond, that.pushPeriodInSecond);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, port, username, password, location, pushPeriodInSecond);
    }
  }
}
