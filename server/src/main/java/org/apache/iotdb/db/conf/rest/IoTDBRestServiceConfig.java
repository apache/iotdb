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
package org.apache.iotdb.db.conf.rest;

public class IoTDBRestServiceConfig {
  static final String CONFIG_NAME = "iotdb-rest.properties";
  /** if the enableRestService is true, we will start REST Service */
  private boolean enableRestService = false;

  /** set the REST Service port. */
  private int restServicePort = 18080;

  /** enable the REST Service ssl. */
  private boolean enableHttps = false;

  /** ssl key Store Path */
  private String keyStorePath = "";

  /** ssl trust Store Path */
  private String trustStorePath = "";

  /** ssl key Store password */
  private String keyStorePwd = "";

  /** ssl trust Store password */
  private String trustStorePwd = "";

  /** ssl timeout */
  private int idleTimeout = 50000;

  /** Session expiration time */
  private int sessionExpire = 28800;

  /** max number of session storage */
  private int sessionMaxNum = 100;

  /** init number of session storage */
  private int sessionInitNum = 10;

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

  public int getRestServicePort() {
    return restServicePort;
  }

  public void setRestServicePort(int restServicePort) {
    this.restServicePort = restServicePort;
  }

  public boolean isEnableHttps() {
    return enableHttps;
  }

  public void setEnableHttps(boolean enableHttps) {
    this.enableHttps = enableHttps;
  }

  public boolean isEnableRestService() {
    return enableRestService;
  }

  public void setEnableRestService(boolean enableRestService) {
    this.enableRestService = enableRestService;
  }

  public int getSessionExpire() {
    return sessionExpire;
  }

  public void setSessionExpire(int sessionExpire) {
    this.sessionExpire = sessionExpire;
  }

  public int getSessionMaxNum() {
    return sessionMaxNum;
  }

  public void setSessionMaxNum(int sessionMaxNum) {
    this.sessionMaxNum = sessionMaxNum;
  }

  public int getSessionInitNum() {
    return sessionInitNum;
  }

  public void setSessionInitNum(int sessionInitNum) {
    this.sessionInitNum = sessionInitNum;
  }
}
