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

  private static final String IOTDB_URL_PATTERN = Config.IOTDB_URL_PREFIX + ".*";

  public IoTDBDriver() {
    // This is a constructor.
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && Pattern.matches(IOTDB_URL_PATTERN, url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    try {
      return acceptsURL(url) ? new IoTDBConnection(url, info) : null;
    } catch (TTransportException e) {
      throw new SQLException(
          JdbcMessages
                  .EXCEPTION_CONNECTION_ERROR_PLEASE_CHECK_WHETHER_NETWORK_AVAILABLE_SERVER_CA72E0D6
              + JdbcMessages.EXCEPTION_HAS_STARTED_BD7BC366,
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
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Properties properties = info == null ? new Properties() : (Properties) info.clone();
    if (url != null && acceptsURL(url)) {
      Utils.parseUrl(url, properties);
    }
    return new DriverPropertyInfo[] {
      createProperty(
          Config.AUTH_USER,
          Config.DEFAULT_USER,
          JdbcMessages.MESSAGE_USER_NAME_FOR_AUTHENTICATION_77DCB7F1,
          properties),
      createSensitiveProperty(
          Config.AUTH_PASSWORD, JdbcMessages.MESSAGE_PASSWORD_FOR_AUTHENTICATION_4D6EDD1E),
      createProperty(
          Config.DEFAULT_BUFFER_CAPACITY,
          String.valueOf(RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY),
          JdbcMessages.MESSAGE_THRIFT_DEFAULT_BUFFER_CAPACITY_IN_BYTES_58ED1E5F,
          properties),
      createProperty(
          Config.THRIFT_FRAME_MAX_SIZE,
          String.valueOf(RpcUtils.THRIFT_FRAME_MAX_SIZE),
          JdbcMessages.MESSAGE_THRIFT_MAX_FRAME_SIZE_IN_BYTES_158D5D1C,
          properties),
      createProperty(
          Config.VERSION,
          Config.DEFAULT_VERSION.name(),
          VERSION_CHOICES,
          JdbcMessages.MESSAGE_CLIENT_COMPATIBILITY_VERSION_9EE5FB89,
          properties),
      createProperty(
          Config.NETWORK_TIMEOUT,
          String.valueOf(Config.DEFAULT_CONNECTION_TIMEOUT_MS),
          JdbcMessages.MESSAGE_NETWORK_TIMEOUT_IN_MILLISECONDS_2443578F,
          properties),
      createProperty(
          Config.TIME_ZONE,
          ZoneId.systemDefault().toString(),
          JdbcMessages.MESSAGE_CONNECTION_TIME_ZONE_9B0E092D,
          properties),
      createProperty(
          Config.CHARSET,
          TSFileConfig.STRING_CHARSET.name(),
          JdbcMessages.MESSAGE_CONNECTION_CHARSET_F72FB844,
          properties),
      createProperty(
          Config.USE_SSL,
          "false",
          BOOLEAN_CHOICES,
          JdbcMessages.MESSAGE_WHETHER_TO_ENABLE_SSL_F2608B05,
          properties),
      createProperty(
          Utils.RPC_COMPRESS,
          String.valueOf(Config.rpcThriftCompressionEnable),
          BOOLEAN_CHOICES,
          JdbcMessages.MESSAGE_WHETHER_TO_ENABLE_RPC_THRIFT_COMPRESSION_C65D1138,
          properties),
      createProperty(
          Config.TRUST_STORE, null, JdbcMessages.MESSAGE_SSL_TRUST_STORE_PATH_1D832EA9, properties),
      createSensitiveProperty(
          Config.TRUST_STORE_PWD, JdbcMessages.MESSAGE_SSL_TRUST_STORE_PASSWORD_CDFFB811),
      createProperty(
          Config.KEY_STORE, null, JdbcMessages.MESSAGE_SSL_KEY_STORE_PATH_24E4E925, properties),
      createSensitiveProperty(
          Config.KEY_STORE_PWD, JdbcMessages.MESSAGE_SSL_KEY_STORE_PASSWORD_8D36E9E7),
      createProperty(
          Config.SSL_PROTOCOL,
          Config.DEFAULT_SSL_PROTOCOL,
          JdbcMessages.MESSAGE_SSL_PROTOCOL_7ED42B2B,
          properties),
      createProperty(
          Config.SQL_DIALECT,
          Constant.TREE,
          SQL_DIALECT_CHOICES,
          JdbcMessages.MESSAGE_SQL_DIALECT_FOR_THE_CONNECTION_35A5F380,
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
