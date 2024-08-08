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
package org.apache.iotdb.jdbc;

import org.apache.iotdb.rpc.RpcUtils;

import java.time.ZoneId;

public class IoTDBConnectionParams {

  private String host = Config.IOTDB_DEFAULT_HOST;
  private int port = Config.IOTDB_DEFAULT_PORT;
  private String jdbcUriString;
  private String seriesName = Config.DEFAULT_SERIES_NAME;
  private String username = Config.DEFAULT_USER;
  private String password = Config.DEFAULT_PASSWORD;

  // The version number of the client which used for compatibility in the server
  private Constant.Version version = Config.DEFAULT_VERSION;

  private int thriftDefaultBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;
  private int thriftMaxFrameSize = RpcUtils.THRIFT_FRAME_MAX_SIZE;
  private int networkTimeout = Config.DEFAULT_CONNECTION_TIMEOUT_MS;

  private String timeZone = ZoneId.systemDefault().toString();

  public IoTDBConnectionParams(String url) {
    this.jdbcUriString = url;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getJdbcUriString() {
    return jdbcUriString;
  }

  public void setJdbcUriString(String jdbcUriString) {
    this.jdbcUriString = jdbcUriString;
  }

  public String getSeriesName() {
    return seriesName;
  }

  public void setSeriesName(String seriesName) {
    this.seriesName = seriesName;
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

  public int getThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  public void setThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public Constant.Version getVersion() {
    return version;
  }

  public void setVersion(Constant.Version version) {
    this.version = version;
  }

  public void setNetworkTimeout(int networkTimeout) {
    if (networkTimeout < 0) {
      this.networkTimeout = Config.DEFAULT_CONNECTION_TIMEOUT_MS;
    } else {
      this.networkTimeout = networkTimeout;
    }
  }

  public int getNetworkTimeout() {
    return this.networkTimeout;
  }

  public void setTimeZone(String timeZone) {
    this.timeZone = timeZone;
  }

  public String getTimeZone() {
    return this.timeZone;
  }
}
