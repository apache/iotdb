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

import org.apache.thrift.transport.TTransportException;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class IoTDBDataSource implements DataSource {

  private String url;
  private String user;
  private String password;
  private static final String PWD_STR = "password";
  private Properties properties;
  private Integer port = 6667;

  public IoTDBDataSource() {
    properties = new Properties();
  }

  public IoTDBDataSource(String url, String user, String password, Integer port) {
    this.url = url;
    this.properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty(PWD_STR, password);
    if (port != 0) {
      this.port = port;
    }
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
    properties.setProperty("user", user);
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
    properties.setProperty(PWD_STR, password);
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
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Connection getConnection(String username, String password) {
    try {
      Properties newProp = new Properties();
      newProp.setProperty("user", username);
      newProp.setProperty(PWD_STR, password);
      return new IoTDBConnection(url, newProp);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter printWriter) {}

  @Override
  public void setLoginTimeout(int i) {}

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) {
    return false;
  }
}
