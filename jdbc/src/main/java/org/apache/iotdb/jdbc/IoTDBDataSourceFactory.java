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

import org.ops4j.pax.jdbc.common.BeanConfig;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import java.sql.Driver;
import java.util.Properties;

public class IoTDBDataSourceFactory implements DataSourceFactory {

  private final Logger logger = LoggerFactory.getLogger(IoTDBDataSourceFactory.class);

  @Override
  public DataSource createDataSource(Properties properties) {
    IoTDBDataSource ds = new IoTDBDataSource();
    setProperties(ds, properties);
    return ds;
  }

  public void setProperties(IoTDBDataSource ds, Properties prop) {
    Properties properties = (Properties) prop.clone();
    String url = (String) properties.remove(DataSourceFactory.JDBC_URL);
    if (url != null) {
      ds.setUrl(url);
      logger.info("URL set {}", url);
    }

    String user = (String) properties.remove(DataSourceFactory.JDBC_USER);
    ds.setUser(user);
    logger.info("User set {}", user);

    String password = (String) properties.remove(DataSourceFactory.JDBC_PASSWORD);
    ds.setPassword(password);
    logger.info("Password set {}", password);

    logger.info("Remaining properties {}", properties.size());

    if (!properties.isEmpty()) {
      BeanConfig.configure(ds, properties);
    }
  }

  @Override
  public ConnectionPoolDataSource createConnectionPoolDataSource(Properties properties) {
    return null;
  }

  @Override
  public XADataSource createXADataSource(Properties properties) {
    return null;
  }

  @Override
  public Driver createDriver(Properties properties) {
    return new IoTDBDriver();
  }
}
