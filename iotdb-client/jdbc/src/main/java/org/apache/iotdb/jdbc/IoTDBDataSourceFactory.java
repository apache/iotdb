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

import org.ops4j.pax.jdbc.common.BeanConfig;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class IoTDBDataSourceFactory implements DataSourceFactory {

  private final Logger logger = LoggerFactory.getLogger(IoTDBDataSourceFactory.class);

  @Override
  public DataSource createDataSource(Properties properties) throws SQLException {
    IoTDBDataSource ds = new IoTDBDataSource();
    setProperties(ds, properties);
    return ds;
  }

  public void setProperties(IoTDBDataSource ds, Properties prop) throws SQLException {
    Properties properties = prop == null ? new Properties() : (Properties) prop.clone();
    String url = removeStringProperty(properties, DataSourceFactory.JDBC_URL);
    if (url != null) {
      ds.setUrl(url);
    }

    String user = removeStringProperty(properties, DataSourceFactory.JDBC_USER);
    if (user != null) {
      ds.setUser(user);
    }

    String password = removeStringProperty(properties, DataSourceFactory.JDBC_PASSWORD);
    if (password != null) {
      ds.setPassword(password);
    }

    String serverName = removeStringProperty(properties, DataSourceFactory.JDBC_SERVER_NAME);
    if (serverName != null) {
      ds.setServerName(serverName);
    }

    Integer portNumber = removeIntegerProperty(properties, DataSourceFactory.JDBC_PORT_NUMBER);
    if (portNumber != null) {
      ds.setPortNumber(portNumber);
    }

    String databaseName = removeStringProperty(properties, DataSourceFactory.JDBC_DATABASE_NAME);
    if (databaseName != null) {
      ds.setDatabaseName(databaseName);
    }

    String dataSourceName =
        removeStringProperty(properties, DataSourceFactory.JDBC_DATASOURCE_NAME);
    if (dataSourceName != null) {
      ds.setDataSourceName(dataSourceName);
    }

    String description = removeStringProperty(properties, DataSourceFactory.JDBC_DESCRIPTION);
    if (description != null) {
      ds.setDescription(description);
    }

    String networkProtocol =
        removeStringProperty(properties, DataSourceFactory.JDBC_NETWORK_PROTOCOL);
    if (networkProtocol != null) {
      ds.setNetworkProtocol(networkProtocol);
    }

    String roleName = removeStringProperty(properties, DataSourceFactory.JDBC_ROLE_NAME);
    if (roleName != null) {
      ds.setRoleName(roleName);
    }

    removeUnsupportedPoolProperties(properties);

    logger.info(JdbcMessages.REMAINING_PROPERTIES, properties.size());

    if (!properties.isEmpty()) {
      BeanConfig.configure(ds, properties);
    }
  }

  @Override
  public ConnectionPoolDataSource createConnectionPoolDataSource(Properties properties)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(JdbcMessages.METHOD_NOT_SUPPORTED);
  }

  @Override
  public XADataSource createXADataSource(Properties properties) throws SQLException {
    throw new SQLFeatureNotSupportedException(JdbcMessages.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Driver createDriver(Properties properties) {
    return new IoTDBDriver();
  }

  private static String removeStringProperty(Properties properties, String key) {
    Object value = properties.remove(key);
    return value == null ? null : value.toString();
  }

  private static Integer removeIntegerProperty(Properties properties, String key)
      throws SQLException {
    Object value = properties.remove(key);
    if (value == null) {
      return null;
    }
    try {
      int integerValue = Integer.parseInt(value.toString());
      if (integerValue < 0 || integerValue > 65535) {
        throw new NumberFormatException(value.toString());
      }
      return integerValue;
    } catch (NumberFormatException e) {
      throw new SQLException("Invalid JDBC property " + key + ": " + value, e);
    }
  }

  private static void removeUnsupportedPoolProperties(Properties properties) {
    properties.remove(DataSourceFactory.JDBC_INITIAL_POOL_SIZE);
    properties.remove(DataSourceFactory.JDBC_MAX_IDLE_TIME);
    properties.remove(DataSourceFactory.JDBC_MAX_POOL_SIZE);
    properties.remove(DataSourceFactory.JDBC_MAX_STATEMENTS);
    properties.remove(DataSourceFactory.JDBC_MIN_POOL_SIZE);
    properties.remove(DataSourceFactory.JDBC_PROPERTY_CYCLE);
  }
}
