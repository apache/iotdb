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

/** the following is iotdb related config */
public class IoTDBReporterConfig {
  private String iotdbSg = "monitor";
  private String iotdbUser = "root";
  private String iotdbPasswd = "root";
  private String iotdbIp = "127.0.0.1";
  private String iotdbPort = "6667";

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
