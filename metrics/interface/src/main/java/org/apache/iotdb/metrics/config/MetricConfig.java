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

import java.util.Arrays;
import java.util.List;

public class MetricConfig {
  private static final Logger logger = LoggerFactory.getLogger(MetricConfig.class);
  static final String CONFIG_NAME = "iotdb-metric.properties";

  /** The period of data pushed by the reporter to the remote monitoring system */
  private int pushPeriodInSecond = 5;

  /** enable publishing data */
  private boolean isEnabled = true;

  /** provide or push metric data to remote system, could be jmx, prometheus, iotdb, etc. */
  private List<String> reporterList =
      Arrays.asList("jmx", "prometheus"); // Collections.singletonList("jmx");

  // the following is prometheus related config
  /** the http server's port for prometheus exporter to get metric data */
  private String prometheusExporterPort = "8090";

  // the following is iotdb related config

  private String iotdbSg = "monitor";
  private String iotdbUser = "root";
  private String iotdbPasswd = "root";
  private String iotdbIp = "127.0.0.1";
  private String iotdbPort = "6667";

  public int getPushPeriodInSecond() {
    return pushPeriodInSecond;
  }

  public void setPushPeriodInSecond(int pushPeriodInSecond) {
    this.pushPeriodInSecond = pushPeriodInSecond;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(boolean enabled) {
    isEnabled = enabled;
  }

  public String getPrometheusExporterPort() {
    return prometheusExporterPort;
  }

  public void setPrometheusExporterPort(String prometheusExporterPort) {
    this.prometheusExporterPort = prometheusExporterPort;
  }

  public List<String> getReporterList() {
    return reporterList;
  }

  public void setReporterList(List<String> reporterList) {
    this.reporterList = reporterList;
  }

  public String getIotdbSg() {
    return iotdbSg;
  }

  public void setIotdbSg(String iotdbSg) {
    this.iotdbSg = iotdbSg;
  }

  public String getIotdbUser() {
    return iotdbUser;
  }

  public void setIotdbUser(String iotdbUser) {
    this.iotdbUser = iotdbUser;
  }

  public String getIotdbPasswd() {
    return iotdbPasswd;
  }

  public void setIotdbPasswd(String iotdbPasswd) {
    this.iotdbPasswd = iotdbPasswd;
  }

  public String getIotdbIp() {
    return iotdbIp;
  }

  public void setIotdbIp(String iotdbIp) {
    this.iotdbIp = iotdbIp;
  }

  public String getIotdbPort() {
    return iotdbPort;
  }

  public void setIotdbPort(String iotdbPort) {
    this.iotdbPort = iotdbPort;
  }
}
