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
  private Properties properties;
  private Integer port = 6667;

  public IoTDBDataSource() {
    properties = new Properties();
  }

  public IoTDBDataSource(String url, String user, String password, Integer port) {
    this.url = url;
    this.properties = new Properties();
    setUser(user);
    setPassword(password);
    if (port != null && port != 0) {
      this.port = port;
    }
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
    this.port = port;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public Connection getConnection() throws SQLException {
    try {
      return new IoTDBConnection(url, properties);
    } catch (TTransportException e) {
      LOGGER.error(JdbcMessages.GET_CONNECTION_ERROR, e);
    }
    return null;
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    try {
      Properties newProp = new Properties();
      setProperty(newProp, Config.AUTH_USER, username);
      setProperty(newProp, Config.AUTH_PASSWORD, password);
      return new IoTDBConnection(url, newProp);
    } catch (TTransportException e) {
      LOGGER.error(JdbcMessages.GET_CONNECTION_ERROR, e);
    }
    return null;
  }

  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter printWriter) {
    // Do nothing
  }

  @Override
  public void setLoginTimeout(int i) {
    // Do nothing
  }

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(JdbcMessages.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    if (isWrapperFor(aClass)) {
      return aClass.cast(this);
    }
    throw new SQLException(JdbcMessages.CANNOT_UNWRAP_TO + aClass);
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) {
    return aClass != null && aClass.isInstance(this);
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
