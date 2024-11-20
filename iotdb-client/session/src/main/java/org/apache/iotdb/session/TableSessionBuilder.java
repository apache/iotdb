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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.time.ZoneId;
import java.util.List;

public class TableSessionBuilder extends AbstractSessionBuilder {

  private boolean enableCompression = false;
  private int connectionTimeoutInMs = SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS;

  public TableSessionBuilder useSSL(boolean useSSL) {
    this.useSSL = useSSL;
    return this;
  }

  public TableSessionBuilder trustStore(String keyStore) {
    this.trustStore = keyStore;
    return this;
  }

  public TableSessionBuilder trustStorePwd(String keyStorePwd) {
    this.trustStorePwd = keyStorePwd;
    return this;
  }

  public TableSessionBuilder host(String host) {
    this.host = host;
    return this;
  }

  public TableSessionBuilder port(int port) {
    this.rpcPort = port;
    return this;
  }

  public TableSessionBuilder username(String username) {
    this.username = username;
    return this;
  }

  public TableSessionBuilder password(String password) {
    this.pw = password;
    return this;
  }

  public TableSessionBuilder fetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  public TableSessionBuilder zoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  public TableSessionBuilder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    return this;
  }

  public TableSessionBuilder thriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    return this;
  }

  public TableSessionBuilder enableRedirection(boolean enableRedirection) {
    this.enableRedirection = enableRedirection;
    return this;
  }

  public TableSessionBuilder enableRecordsAutoConvertTablet(
      boolean enableRecordsAutoConvertTablet) {
    this.enableRecordsAutoConvertTablet = enableRecordsAutoConvertTablet;
    return this;
  }

  public TableSessionBuilder nodeUrls(List<String> nodeUrls) {
    this.nodeUrls = nodeUrls;
    return this;
  }

  public TableSessionBuilder version(Version version) {
    this.version = version;
    return this;
  }

  public TableSessionBuilder timeOut(long timeOut) {
    this.timeOut = timeOut;
    return this;
  }

  public TableSessionBuilder enableAutoFetch(boolean enableAutoFetch) {
    this.enableAutoFetch = enableAutoFetch;
    return this;
  }

  public TableSessionBuilder maxRetryCount(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
    return this;
  }

  public TableSessionBuilder retryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
    return this;
  }

  public TableSessionBuilder sqlDialect(String sqlDialect) {
    this.sqlDialect = sqlDialect;
    return this;
  }

  public TableSessionBuilder database(String database) {
    this.database = database;
    return this;
  }

  public TableSessionBuilder enableCompression(boolean enableCompression) {
    this.enableCompression = enableCompression;
    return this;
  }

  public TableSessionBuilder connectionTimeoutInMs(int connectionTimeoutInMs) {
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    return this;
  }

  public ITableSession build() throws IoTDBConnectionException {
    if (nodeUrls != null
        && (!SessionConfig.DEFAULT_HOST.equals(host) || rpcPort != SessionConfig.DEFAULT_PORT)) {
      throw new IllegalArgumentException(
          "You should specify either nodeUrls or (host + rpcPort), but not both");
    }
    Session newSession = new Session(this);
    // TODO open
    return newSession;
  }
}
