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

import org.apache.iotdb.jdbc.i18n.JdbcMessages;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.thrift.transport.TTransportException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.osgi.service.component.annotations.Component;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

@Component(service = java.sql.Driver.class, immediate = true)
public class IoTDBDriver implements Driver {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IoTDBDriver.class);

  /** Is this driver JDBC compliant. */
  private static final boolean IOTDB_JDBC_COMPLIANT = false;

  private static final String[] BOOLEAN_CHOICES = {"true", "false"};
  private static final String[] SQL_DIALECT_CHOICES = {Constant.TREE, Constant.TABLE};
  private static final String[] VERSION_CHOICES =
      Arrays.stream(Constant.Version.values()).map(Enum::name).toArray(String[]::new);

  static {
    try {
      DriverManager.registerDriver(new IoTDBDriver());
    } catch (SQLException e) {
      logger.error(JdbcMessages.REGISTER_DRIVER_ERROR, e);
    }
  }

  private static final String TSFILE_URL_PREFIX = Config.IOTDB_URL_PREFIX + ".*";

  public IoTDBDriver() {
    // This is a constructor.
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && Pattern.matches(TSFILE_URL_PREFIX, url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    try {
      return acceptsURL(url) ? new IoTDBConnection(url, info) : null;
    } catch (TTransportException e) {
      throw new SQLException(
          "Connection Error, please check whether the network is available or the server"
              + " has started.",
          e);
    }
  }

  @Override
  public int getMajorVersion() {
    return Config.DRIVER_MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return Config.DRIVER_MINOR_VERSION;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(JdbcMessages.METHOD_NOT_SUPPORTED);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    Properties properties = info == null ? new Properties() : info;
    return new DriverPropertyInfo[] {
      createProperty(
          Config.AUTH_USER, Config.DEFAULT_USER, "User name for authentication.", properties),
      createSensitiveProperty(Config.AUTH_PASSWORD, "Password for authentication."),
      createProperty(
          Config.DEFAULT_BUFFER_CAPACITY,
          String.valueOf(RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY),
          "Thrift default buffer capacity in bytes.",
          properties),
      createProperty(
          Config.THRIFT_FRAME_MAX_SIZE,
          String.valueOf(RpcUtils.THRIFT_FRAME_MAX_SIZE),
          "Thrift max frame size in bytes.",
          properties),
      createProperty(
          Config.VERSION,
          Config.DEFAULT_VERSION.name(),
          VERSION_CHOICES,
          "Client compatibility version.",
          properties),
      createProperty(
          Config.NETWORK_TIMEOUT,
          String.valueOf(Config.DEFAULT_CONNECTION_TIMEOUT_MS),
          "Network timeout in milliseconds.",
          properties),
      createProperty(
          Config.TIME_ZONE, ZoneId.systemDefault().toString(), "Connection time zone.", properties),
      createProperty(
          Config.CHARSET, TSFileConfig.STRING_CHARSET.name(), "Connection charset.", properties),
      createProperty(
          Config.USE_SSL, "false", BOOLEAN_CHOICES, "Whether to enable SSL.", properties),
      createProperty(Config.TRUST_STORE, null, "SSL trust store path.", properties),
      createSensitiveProperty(Config.TRUST_STORE_PWD, "SSL trust store password."),
      createProperty(
          Config.SQL_DIALECT,
          Constant.TREE,
          SQL_DIALECT_CHOICES,
          "SQL dialect for the connection.",
          properties)
    };
  }

  @Override
  public boolean jdbcCompliant() {
    return IOTDB_JDBC_COMPLIANT;
  }

  private static DriverPropertyInfo createProperty(
      String name, String defaultValue, String description, Properties properties) {
    return createProperty(name, defaultValue, null, description, properties);
  }

  private static DriverPropertyInfo createSensitiveProperty(String name, String description) {
    DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, null);
    propertyInfo.required = false;
    propertyInfo.description = description;
    return propertyInfo;
  }

  private static DriverPropertyInfo createProperty(
      String name,
      String defaultValue,
      String[] choices,
      String description,
      Properties properties) {
    DriverPropertyInfo propertyInfo =
        new DriverPropertyInfo(name, properties.getProperty(name, defaultValue));
    propertyInfo.required = false;
    propertyInfo.choices = choices;
    propertyInfo.description = description;
    return propertyInfo;
  }
}
