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
import org.apache.iotdb.metrics.utils.NodeType;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.metrics.utils.SystemType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MetricConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricConfig.class);
  /** The type of the implementation of metric framework. */
  private MetricFrameType metricFrameType = MetricFrameType.MICROMETER;

  /** The list of reporters provide metrics for external tool. */
  private List<ReporterType> metricReporterList = Collections.emptyList();

  /** The level of metric service. */
  private MetricLevel metricLevel = MetricLevel.CORE;

  /** The period of async collection of some metrics in second. */
  private Integer asyncCollectPeriodInSecond = 5;

  /** The export port for prometheus to get metrics. */
  private Integer prometheusReporterPort = 9091;

  /** The iotdb config for iotdb reporter to push metric data. */
  private final IoTDBReporterConfig iotdbReporterConfig = new IoTDBReporterConfig();

  /** The type of internal reporter. */
  private InternalReporterType internalReporterType = InternalReporterType.MEMORY;

  /** The pid of iotdb instance. */
  private String pid = "";
  /** The running system of iotdb instance. */
  private final SystemType systemType = SystemType.getSystemType();
  /** The type of monitored node. */
  private NodeType nodeType = NodeType.CONFIGNODE;
  /** The name of iotdb cluster. */
  private String clusterName = "defaultCluster";
  /** The id of iotdb node. */
  private int nodeId = 0;

  public MetricConfig() {
    // try to get pid of iotdb instance
    try {
      pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    } catch (Exception e) {
      LOGGER.warn("Failed to get pid, because ", e);
    }
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

  public IoTDBReporterConfig getIotdbReporterConfig() {
    return iotdbReporterConfig;
  }

  public String getPid() {
    return pid;
  }

  public SystemType getSystemType() {
    return systemType;
  }

  public NodeType getNodeType() {
    return nodeType;
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getNodeId() {
    return nodeId;
  }

  /** Update rpc address and rpc port of monitored node. */
  public void updateRpcInstance(String clusterName, int nodeId, NodeType nodeType) {
    this.clusterName = clusterName;
    this.nodeId = nodeId;
    this.nodeType = nodeType;
  }

  /** Copy properties from another metric config. */
  public void copy(MetricConfig newMetricConfig) {
    metricFrameType = newMetricConfig.getMetricFrameType();
    metricReporterList = newMetricConfig.getMetricReporterList();
    metricLevel = newMetricConfig.getMetricLevel();
    asyncCollectPeriodInSecond = newMetricConfig.getAsyncCollectPeriodInSecond();
    prometheusReporterPort = newMetricConfig.getPrometheusReporterPort();
    internalReporterType = newMetricConfig.getInternalReportType();

    iotdbReporterConfig.copy(newMetricConfig.getIotdbReporterConfig());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MetricConfig)) {
      return false;
    }
    MetricConfig anotherMetricConfig = (MetricConfig) obj;
    return metricFrameType.equals(anotherMetricConfig.getMetricFrameType())
        && metricReporterList.equals(anotherMetricConfig.getMetricReporterList())
        && metricLevel.equals(anotherMetricConfig.getMetricLevel())
        && asyncCollectPeriodInSecond.equals(anotherMetricConfig.getAsyncCollectPeriodInSecond())
        && prometheusReporterPort.equals(anotherMetricConfig.getPrometheusReporterPort())
        && iotdbReporterConfig.equals(anotherMetricConfig.getIotdbReporterConfig())
        && internalReporterType.equals(anotherMetricConfig.getInternalReportType());
  }

  public static class IoTDBReporterConfig {
    /** The host of iotdb that store metric value. */
    private String host = "127.0.0.1";
    /** The port of iotdb that store metric value. */
    private Integer port = 6667;
    /** The username of iotdb. */
    private String username = "root";
    /** The password of iotdb. */
    private String password = "root";
    /** The max number of connection. */
    private Integer maxConnectionNumber = 3;
    /** The location of iotdb metrics. */
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

    public void copy(IoTDBReporterConfig iotdbReporterConfig) {
      host = iotdbReporterConfig.getHost();
      port = iotdbReporterConfig.getPort();
      username = iotdbReporterConfig.getUsername();
      password = iotdbReporterConfig.getPassword();
      maxConnectionNumber = iotdbReporterConfig.getMaxConnectionNumber();
      pushPeriodInSecond = iotdbReporterConfig.getPushPeriodInSecond();
      location = iotdbReporterConfig.getLocation();
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
