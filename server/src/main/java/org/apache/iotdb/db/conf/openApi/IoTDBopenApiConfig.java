/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.conf.openApi;

public class IoTDBopenApiConfig {
  static final String CONFIG_NAME = "iotdb-openapi.properties";
  /** if the startOpenApi is true, we will start OpenApi */
  private boolean startOpenApi = true;

  /** set the OpenApi reset port. */
  private int OpenApiPort = 18080;

  /** enable the OpenApi ssl. */
  private boolean enable_https = false;

  /** openapi ssl key Store Path */
  private String keyStorePath = "";

  /** openapi ssl trust Store Path */
  private String trustStorePath = "";

  /** openapi ssl key Store password */
  private String keyStorePwd = "";

  /** openapi ssl trust Store password */
  private String trustStorePwd = "";

  /** OpenApi ssl timeout */
  private int idleTimeout = 50000;

  public String getTrustStorePwd() {
    return trustStorePwd;
  }

  public void setTrustStorePwd(String trustStorePwd) {
    this.trustStorePwd = trustStorePwd;
  }

  public int getIdleTimeout() {
    return idleTimeout;
  }

  public void setIdleTimeout(int idleTimeout) {
    this.idleTimeout = idleTimeout;
  }

  public boolean isEnable_https() {
    return enable_https;
  }

  public void setEnable_https(boolean enable_https) {
    this.enable_https = enable_https;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public void setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public void setTrustStorePath(String trustStorePath) {
    this.trustStorePath = trustStorePath;
  }

  public String getKeyStorePwd() {
    return keyStorePwd;
  }

  public void setKeyStorePwd(String keyStorePwd) {
    this.keyStorePwd = keyStorePwd;
  }

  public boolean isStartOpenApi() {
    return startOpenApi;
  }

  public void setStartOpenApi(boolean startOpenApi) {
    this.startOpenApi = startOpenApi;
  }

  public int getOpenApiPort() {
    return OpenApiPort;
  }

  public void setOpenApiPort(int openApiPort) {
    OpenApiPort = openApiPort;
  }
}
