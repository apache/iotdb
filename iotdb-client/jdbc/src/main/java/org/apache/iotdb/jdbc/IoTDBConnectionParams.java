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

import org.apache.tsfile.common.conf.TSFileConfig;

import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.Optional;

import static org.apache.iotdb.jdbc.Constant.TREE;

/**
 * Holds all connection parameters parsed from a JDBC URL for an IoTDB connection.
 *
 * <p>This class is populated by {@code IoTDBJDBCUtils} when a JDBC URL of the form
 * {@code jdbc:iotdb://host:port/} is parsed, and is subsequently consumed by
 * {@code IoTDBConnection} to open a Thrift session to the IoTDB server.
 *
 * <p>Default values for all parameters are defined in {@link Config}.
 */
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
  private Charset charset = TSFileConfig.STRING_CHARSET;

  private boolean useSSL = false;
  private String trustStore;
  private String trustStorePwd;

  private String sqlDialect = TREE;

  private String db;

  /**
   * Constructs an {@code IoTDBConnectionParams} instance with the given JDBC URI string.
   *
   * @param url the full JDBC URI string, e.g. {@code jdbc:iotdb://localhost:6667/}
   */
  public IoTDBConnectionParams(String url) {
    this.jdbcUriString = url;
  }

  /**
   * Returns the hostname or IP address of the IoTDB server.
   *
   * @return the host string, defaulting to {@code Config.IOTDB_DEFAULT_HOST}
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the hostname or IP address of the IoTDB server.
   *
   * @param host the hostname or IP address to connect to
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the port number of the IoTDB server.
   *
   * @return the port number, defaulting to {@code Config.IOTDB_DEFAULT_PORT}
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the port number of the IoTDB server.
   *
   * @param port the port number to connect to
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the full JDBC URI string used to establish this connection.
   *
   * @return the JDBC URI string, e.g. {@code jdbc:iotdb://localhost:6667/}
   */
  public String getJdbcUriString() {
    return jdbcUriString;
  }

  /**
   * Sets the full JDBC URI string for this connection.
   *
   * @param jdbcUriString the JDBC URI string to set
   */
  public void setJdbcUriString(String jdbcUriString) {
    this.jdbcUriString = jdbcUriString;
  }

  /**
   * Returns the time-series path prefix (series name) for this connection.
   *
   * @return the series name, defaulting to {@code Config.DEFAULT_SERIES_NAME}
   */
  public String getSeriesName() {
    return seriesName;
  }

  /**
   * Sets the time-series path prefix (series name) for this connection.
   *
   * @param seriesName the series name to set
   */
  public void setSeriesName(String seriesName) {
    this.seriesName = seriesName;
  }

  /**
   * Returns the username used to authenticate with the IoTDB server.
   *
   * @return the username, defaulting to {@code Config.DEFAULT_USER}
   */
  public String getUsername() {
    return username;
  }

  /**
   * Sets the username used to authenticate with the IoTDB server.
   *
   * @param username the username to set
   */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Returns the password used to authenticate with the IoTDB server.
   *
   * @return the password, defaulting to {@code Config.DEFAULT_PASSWORD}
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password used to authenticate with the IoTDB server.
   *
   * @param password the password to set
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Returns the default buffer size (in bytes) used for Thrift RPC communication.
   *
   * @return the Thrift default buffer size
   */
  public int getThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  /**
   * Sets the default buffer size (in bytes) used for Thrift RPC communication.
   *
   * @param thriftDefaultBufferSize the buffer size to set
   */
  public void setThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
  }

  /**
   * Returns the maximum frame size (in bytes) for Thrift RPC communication.
   *
   * <p>Frames larger than this value will be rejected. Increase this value
   * when querying very wide rows or large result sets.
   *
   * @return the Thrift maximum frame size
   */
  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  /**
   * Sets the maximum frame size (in bytes) for Thrift RPC communication.
   *
   * @param thriftMaxFrameSize the maximum frame size to set
   */
  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  /**
   * Returns the client version used for server-side compatibility negotiation.
   *
   * @return the client version constant
   */
  public Constant.Version getVersion() {
    return version;
  }

  /**
   * Sets the client version used for server-side compatibility negotiation.
   *
   * @param version the version constant to set
   */
  public void setVersion(Constant.Version version) {
    this.version = version;
  }

  /**
   * Sets the network timeout in milliseconds for this connection.
   *
   * <p>If a negative value is provided, the timeout is reset to
   * {@code Config.DEFAULT_CONNECTION_TIMEOUT_MS}.
   *
   * @param networkTimeout the timeout in milliseconds; negative values reset to default
   */
  public void setNetworkTimeout(int networkTimeout) {
    if (networkTimeout < 0) {
      this.networkTimeout = Config.DEFAULT_CONNECTION_TIMEOUT_MS;
    } else {
      this.networkTimeout = networkTimeout;
    }
  }

  /**
   * Returns the network timeout in milliseconds for this connection.
   *
   * @return the network timeout in milliseconds
   */
  public int getNetworkTimeout() {
    return this.networkTimeout;
  }

  /**
   * Sets the timezone used when formatting timestamps returned by the IoTDB server.
   *
   * @param timeZone a timezone string accepted by {@code ZoneId.of()},
   *                 e.g. {@code "UTC"} or {@code "Asia/Shanghai"}
   */
  public void setTimeZone(String timeZone) {
    this.timeZone = timeZone;
  }

  /**
   * Returns the timezone string used when formatting timestamps for this connection.
   *
   * @return the timezone string, defaulting to the system default timezone
   */
  public String getTimeZone() {
    return this.timeZone;
  }

  /**
   * Sets the character set used to encode and decode string values for this connection.
   *
   * @param charsetName the IANA charset name, e.g. {@code "UTF-8"}
   * @throws java.nio.charset.UnsupportedCharsetException if the charset name is not recognised
   */
  public void setCharset(String charsetName) {
    this.charset = Charset.forName(charsetName);
  }

  /**
   * Returns the character set used to encode and decode string values for this connection.
   *
   * @return the charset, defaulting to {@code TSFileConfig.STRING_CHARSET}
   */
  public Charset getCharset() {
    return charset;
  }

  /**
   * Returns whether SSL/TLS is enabled for this connection.
   *
   * @return {@code true} if SSL is enabled; {@code false} otherwise
   */
  public boolean isUseSSL() {
    return useSSL;
  }

  /**
   * Sets whether SSL/TLS should be used for this connection.
   *
   * <p>When enabled, {@code trustStore} and {@code trustStorePwd} must also be set.
   *
   * @param useSSL {@code true} to enable SSL; {@code false} to disable
   */
  public void setUseSSL(boolean useSSL) {
    this.useSSL = useSSL;
  }

  /**
   * Returns the file path of the trust store used for SSL/TLS verification.
   *
   * @return the trust store path, or {@code null} if not set
   */
  public String getTrustStore() {
    return trustStore;
  }

  /**
   * Sets the file path of the trust store used for SSL/TLS verification.
   *
   * @param trustStore the absolute or relative path to the trust store file
   */
  public void setTrustStore(String trustStore) {
    this.trustStore = trustStore;
  }

  /**
   * Returns the password for the trust store used in SSL/TLS verification.
   *
   * @return the trust store password, or {@code null} if not set
   */
  public String getTrustStorePwd() {
    return trustStorePwd;
  }

  /**
   * Sets the password for the trust store used in SSL/TLS verification.
   *
   * @param trustStorePwd the trust store password
   */
  public void setTrustStorePwd(String trustStorePwd) {
    this.trustStorePwd = trustStorePwd;
  }

  /**
   * Returns the SQL dialect mode for this connection.
   *
   * <p>IoTDB supports two SQL dialect modes:
   * <ul>
   *   <li>{@code "tree"} — the default tree-model dialect</li>
   *   <li>{@code "table"} — the relational table-model dialect (IoTDB 2.X)</li>
   * </ul>
   *
   * @return the SQL dialect string, defaulting to {@code "tree"}
   */
  public String getSqlDialect() {
    return sqlDialect;
  }

  /**
   * Sets the SQL dialect mode for this connection.
   *
   * @param sqlDialect the dialect string; either {@code "tree"} or {@code "table"}
   */
  public void setSqlDialect(String sqlDialect) {
    this.sqlDialect = sqlDialect;
  }

  /**
   * Returns the target database name for this connection, if specified.
   *
   * <p>This is used in Table Model (IoTDB 2.X) to set the default database
   * context, equivalent to {@code USE database} in SQL.
   *
   * @return an {@code Optional} containing the database name, or empty if not set
   */
  public Optional<String> getDb() {
    return Optional.ofNullable(db);
  }

  /**
   * Sets the target database name for this connection.
   *
   * @param db the database name to use as the default context
   */
  public void setDb(String db) {
    this.db = db;
  }
}
