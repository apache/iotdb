/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.jdbc;

import org.apache.iotdb.jdbc.i18n.JdbcMessages;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class IoTDBDataSource implements DataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataSource.class);

  private String url;
  private String user;
  private String password;
  private String serverName = Config.IOTDB_DEFAULT_HOST;
  private String databaseName;
  private String dataSourceName;
  private String description;
  private String networkProtocol;
  private String roleName;
  private Properties properties;
  private Integer port = Config.IOTDB_DEFAULT_PORT;
  private PrintWriter logWriter;
  private int loginTimeout;

  public IoTDBDataSource() {
    properties = new Properties();
  }

  public IoTDBDataSource(String url, String user, String password, Integer port) {
    this.url = url;
    this.properties = new Properties();
    setUser(user);
    setPassword(password);
    setPort(port);
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
    setProperty(Config.AUTH_USER, user);
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
    setProperty(Config.AUTH_PASSWORD, password);
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    if (port != null && (port < 0 || port > 65535)) {
      throw new IllegalArgumentException("port must be between 0 and 65535");
    }
    this.port = port == null || port == 0 ? Config.IOTDB_DEFAULT_PORT : port;
  }

  public Integer getPortNumber() {
    return getPort();
  }

  public void setPortNumber(Integer portNumber) {
    setPort(portNumber);
  }

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getDataSourceName() {
    return dataSourceName;
  }

  public void setDataSourceName(String dataSourceName) {
    this.dataSourceName = dataSourceName;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getNetworkProtocol() {
    return networkProtocol;
  }

  public void setNetworkProtocol(String networkProtocol) {
    this.networkProtocol = networkProtocol;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  String getConnectionProperty(String key) {
    return properties.getProperty(key);
  }

  void setConnectionProperty(String key, String value) {
    setProperty(key, value);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return createConnection((Properties) properties.clone());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    Properties newProp = (Properties) properties.clone();
    setProperty(newProp, Config.AUTH_USER, username);
    setProperty(newProp, Config.AUTH_PASSWORD, password);
    return createConnection(newProp);
  }

  @Override
  public PrintWriter getLogWriter() {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter printWriter) {
    this.logWriter = printWriter;
  }

  @Override
  public void setLoginTimeout(int i) throws SQLException {
    if (i < 0) {
      throw new SQLException("loginTimeout must be >= 0");
    }
    this.loginTimeout = i;
  }

  @Override
  public int getLoginTimeout() {
    return loginTimeout;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(JdbcMessages.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    return JdbcWrapperUtils.unwrap(this, aClass);
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) {
    return JdbcWrapperUtils.isWrapperFor(this, aClass);
  }

  private Connection createConnection(Properties connectionProperties) throws SQLException {
    applyLoginTimeout(connectionProperties);
    try {
      return new IoTDBConnection(getConnectionUrl(), connectionProperties);
    } catch (TTransportException e) {
      LOGGER.error(JdbcMessages.GET_CONNECTION_ERROR, e);
      throw new SQLException(
          "Connection Error, please check whether the network is available or the server"
              + " has started.",
          e);
    }
  }

  String getConnectionUrl() {
    if (url != null) {
      return url;
    }
    String host =
        serverName == null || serverName.trim().isEmpty() ? Config.IOTDB_DEFAULT_HOST : serverName;
    StringBuilder builder = new StringBuilder(Config.IOTDB_URL_PREFIX);
    builder.append(host).append(':').append(port);
    if (databaseName != null && !databaseName.isEmpty()) {
      builder.append('/').append(databaseName);
    }
    return builder.toString();
  }

  private void applyLoginTimeout(Properties connectionProperties) {
    if (loginTimeout <= 0 || connectionProperties.containsKey(Config.NETWORK_TIMEOUT)) {
      return;
    }
    long timeoutInMs = (long) loginTimeout * 1000;
    connectionProperties.setProperty(
        Config.NETWORK_TIMEOUT, String.valueOf(Math.min(timeoutInMs, Integer.MAX_VALUE)));
  }

  private void setProperty(String key, String value) {
    setProperty(properties, key, value);
  }

  private static void setProperty(Properties properties, String key, String value) {
    if (value == null) {
      properties.remove(key);
    } else {
      properties.setProperty(key, value);
    }
  }
}
