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

package org.apache.iotdb.session.pool;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ITableSessionPool;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

/**
 * A builder class for constructing instances of {@link ITableSessionPool}.
 *
 * <p>This builder provides a fluent API for configuring a session pool, including connection
 * settings, session parameters, and pool behavior.
 *
 * <p>All configurations have reasonable default values, which can be overridden as needed.
 */
public class TableSessionPoolBuilder extends AbstractSessionPoolBuilder {

  /**
   * Sets the list of node URLs for the IoTDB cluster.
   *
   * @param nodeUrls a list of node URLs.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue Collection.singletonList("localhost:6667")
   */
  public TableSessionPoolBuilder nodeUrls(List<String> nodeUrls) {
    this.nodeUrls = nodeUrls;
    return this;
  }

  /**
   * Sets the maximum size of the session pool.
   *
   * @param maxSize the maximum number of sessions allowed in the pool.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 5
   */
  public TableSessionPoolBuilder maxSize(int maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  /**
   * Sets the username for the connection.
   *
   * @param user the username.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue "root"
   */
  public TableSessionPoolBuilder user(String user) {
    this.username = user;
    return this;
  }

  /**
   * Sets the password for the connection.
   *
   * @param password the password.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue "root"
   */
  public TableSessionPoolBuilder password(String password) {
    this.pw = password;
    return this;
  }

  /**
   * Sets the target database name.
   *
   * @param database the database name.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue null
   */
  public TableSessionPoolBuilder database(String database) {
    this.database = database;
    return this;
  }

  /**
   * Sets the query timeout in milliseconds.
   *
   * @param queryTimeoutInMs the query timeout in milliseconds.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 60000 (1 minute)
   */
  public TableSessionPoolBuilder queryTimeoutInMs(long queryTimeoutInMs) {
    this.timeOut = queryTimeoutInMs;
    return this;
  }

  /**
   * Sets the fetch size for query results.
   *
   * @param fetchSize the fetch size.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 5000
   */
  public TableSessionPoolBuilder fetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  /**
   * Sets the {@link ZoneId} for timezone-related operations.
   *
   * @param zoneId the {@link ZoneId}.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue ZoneId.systemDefault()
   */
  public TableSessionPoolBuilder zoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  /**
   * Sets the timeout for waiting to acquire a session from the pool.
   *
   * @param waitToGetSessionTimeoutInMs the timeout duration in milliseconds.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 60000 (60 seconds)
   */
  public TableSessionPoolBuilder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
    return this;
  }

  /**
   * Sets the default buffer size for the Thrift client.
   *
   * @param thriftDefaultBufferSize the buffer size in bytes.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 1024 (1 KB)
   */
  public TableSessionPoolBuilder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    return this;
  }

  /**
   * Sets the maximum frame size for the Thrift client.
   *
   * @param thriftMaxFrameSize the maximum frame size in bytes.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 64 * 1024 * 1024 (64 MB)
   */
  public TableSessionPoolBuilder thriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    return this;
  }

  /**
   * Enables or disables rpc compression for the connection.
   *
   * @param enableCompression whether to enable compression.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue false
   */
  public TableSessionPoolBuilder enableCompression(boolean enableCompression) {
    this.enableCompression = enableCompression;
    return this;
  }

  /**
   * Enables or disables redirection for cluster nodes.
   *
   * @param enableRedirection whether to enable redirection.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue true
   */
  public TableSessionPoolBuilder enableRedirection(boolean enableRedirection) {
    this.enableRedirection = enableRedirection;
    return this;
  }

  /**
   * Sets the connection timeout in milliseconds.
   *
   * @param connectionTimeoutInMs the connection timeout in milliseconds.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 0 (no timeout)
   */
  public TableSessionPoolBuilder connectionTimeoutInMs(int connectionTimeoutInMs) {
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    return this;
  }

  /**
   * Enables or disables automatic fetching of available DataNodes.
   *
   * @param enableAutoFetch whether to enable automatic fetching.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue true
   */
  public TableSessionPoolBuilder enableAutoFetch(boolean enableAutoFetch) {
    this.enableAutoFetch = enableAutoFetch;
    return this;
  }

  /**
   * Sets the maximum number of retries for connection attempts.
   *
   * @param maxRetryCount the maximum retry count.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 60
   */
  public TableSessionPoolBuilder maxRetryCount(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
    return this;
  }

  /**
   * Sets the interval between retries in milliseconds.
   *
   * @param retryIntervalInMs the interval in milliseconds.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue 500 milliseconds
   */
  public TableSessionPoolBuilder retryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
    return this;
  }

  /**
   * Enables or disables SSL for secure connections.
   *
   * @param useSSL whether to enable SSL.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue false
   */
  public TableSessionPoolBuilder useSSL(boolean useSSL) {
    this.useSSL = useSSL;
    return this;
  }

  /**
   * Sets the trust store path for SSL connections.
   *
   * @param keyStore the trust store path.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue null
   */
  public TableSessionPoolBuilder trustStore(String keyStore) {
    this.trustStore = keyStore;
    return this;
  }

  /**
   * Sets the trust store password for SSL connections.
   *
   * @param keyStorePwd the trust store password.
   * @return the current {@link TableSessionPoolBuilder} instance.
   * @defaultValue null
   */
  public TableSessionPoolBuilder trustStorePwd(String keyStorePwd) {
    this.trustStorePwd = keyStorePwd;
    return this;
  }

  /**
   * Builds and returns a configured {@link ITableSessionPool} instance.
   *
   * @return a fully configured {@link ITableSessionPool}.
   */
  public ITableSessionPool build() {
    if (nodeUrls == null) {
      this.nodeUrls =
          Collections.singletonList(SessionConfig.DEFAULT_HOST + ":" + SessionConfig.DEFAULT_PORT);
    }
    this.sqlDialect = "table";
    SessionPool sessionPool = new SessionPool(this);
    return new TableSessionPool(sessionPool);
  }
}
