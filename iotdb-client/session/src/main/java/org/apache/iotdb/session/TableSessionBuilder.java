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
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

/**
 * A builder class for constructing instances of {@link ITableSession}.
 *
 * <p>This builder provides a fluent API for configuring various options such as connection
 * settings, query parameters, and security features.
 *
 * <p>All configurations have reasonable default values, which can be overridden as needed.
 */
public class TableSessionBuilder extends AbstractSessionBuilder {

  private boolean enableCompression = false;
  private int connectionTimeoutInMs = SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS;

  /**
   * Sets the list of node URLs for the IoTDB cluster.
   *
   * @param nodeUrls a list of node URLs.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue Collection.singletonList("localhost:6667")
   */
  public TableSessionBuilder nodeUrls(List<String> nodeUrls) {
    this.nodeUrls = nodeUrls;
    return this;
  }

  /**
   * Sets the username for the connection.
   *
   * @param username the username.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue "root"
   */
  public TableSessionBuilder username(String username) {
    this.username = username;
    return this;
  }

  /**
   * Sets the password for the connection.
   *
   * @param password the password.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue "root"
   */
  public TableSessionBuilder password(String password) {
    this.pw = password;
    return this;
  }

  /**
   * Sets the target database name.
   *
   * @param database the database name.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue null
   */
  public TableSessionBuilder database(String database) {
    this.database = database;
    return this;
  }

  /**
   * Sets the query timeout in milliseconds.
   *
   * @param queryTimeoutInMs the query timeout in milliseconds.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 60000 (1 minute)
   */
  public TableSessionBuilder queryTimeoutInMs(long queryTimeoutInMs) {
    this.timeOut = queryTimeoutInMs;
    return this;
  }

  /**
   * Sets the fetch size for query results.
   *
   * @param fetchSize the fetch size.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 5000
   */
  public TableSessionBuilder fetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  /**
   * Sets the {@link ZoneId} for timezone-related operations.
   *
   * @param zoneId the {@link ZoneId}.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue ZoneId.systemDefault()
   */
  public TableSessionBuilder zoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  /**
   * Sets the default init buffer size for the Thrift client.
   *
   * @param thriftDefaultBufferSize the buffer size in bytes.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 1024 (1 KB)
   */
  public TableSessionBuilder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    return this;
  }

  /**
   * Sets the maximum frame size for the Thrift client.
   *
   * @param thriftMaxFrameSize the maximum frame size in bytes.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 64 * 1024 * 1024 (64 MB)
   */
  public TableSessionBuilder thriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    return this;
  }

  /**
   * Enables or disables redirection for cluster nodes.
   *
   * @param enableRedirection whether to enable redirection.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue true
   */
  public TableSessionBuilder enableRedirection(boolean enableRedirection) {
    this.enableRedirection = enableRedirection;
    return this;
  }

  /**
   * Enables or disables automatic fetching of available DataNodes.
   *
   * @param enableAutoFetch whether to enable automatic fetching.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue true
   */
  public TableSessionBuilder enableAutoFetch(boolean enableAutoFetch) {
    this.enableAutoFetch = enableAutoFetch;
    return this;
  }

  /**
   * Sets the maximum number of retries for connection attempts.
   *
   * @param maxRetryCount the maximum retry count.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 60
   */
  public TableSessionBuilder maxRetryCount(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
    return this;
  }

  /**
   * Sets the interval between retries in milliseconds.
   *
   * @param retryIntervalInMs the interval in milliseconds.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 500 milliseconds
   */
  public TableSessionBuilder retryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
    return this;
  }

  /**
   * Enables or disables SSL for secure connections.
   *
   * @param useSSL whether to enable SSL.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue false
   */
  public TableSessionBuilder useSSL(boolean useSSL) {
    this.useSSL = useSSL;
    return this;
  }

  /**
   * Sets the trust store path for SSL connections.
   *
   * @param keyStore the trust store path.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue null
   */
  public TableSessionBuilder trustStore(String keyStore) {
    this.trustStore = keyStore;
    return this;
  }

  /**
   * Sets the trust store password for SSL connections.
   *
   * @param keyStorePwd the trust store password.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue null
   */
  public TableSessionBuilder trustStorePwd(String keyStorePwd) {
    this.trustStorePwd = keyStorePwd;
    return this;
  }

  /**
   * Enables or disables rpc compression for the connection.
   *
   * @param enableCompression whether to enable compression.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue false
   */
  public TableSessionBuilder enableCompression(boolean enableCompression) {
    this.enableCompression = enableCompression;
    return this;
  }

  /**
   * Sets the connection timeout in milliseconds.
   *
   * @param connectionTimeoutInMs the connection timeout in milliseconds.
   * @return the current {@link TableSessionBuilder} instance.
   * @defaultValue 0 (no timeout)
   */
  public TableSessionBuilder connectionTimeoutInMs(int connectionTimeoutInMs) {
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    return this;
  }

  /**
   * Builds and returns a configured {@link ITableSession} instance.
   *
   * @return a fully configured {@link ITableSession}.
   * @throws IoTDBConnectionException if an error occurs while establishing the connection.
   */
  public ITableSession build() throws IoTDBConnectionException {
    if (nodeUrls == null) {
      this.nodeUrls =
          Collections.singletonList(SessionConfig.DEFAULT_HOST + ":" + SessionConfig.DEFAULT_PORT);
    }
    this.sqlDialect = "table";
    Session newSession = new Session(this);
    newSession.open(enableCompression, connectionTimeoutInMs);
    return new TableSession(newSession);
  }
}
