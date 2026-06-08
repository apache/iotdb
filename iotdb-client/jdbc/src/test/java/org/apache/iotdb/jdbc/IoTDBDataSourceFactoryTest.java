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

import org.junit.Test;
import org.osgi.service.jdbc.DataSourceFactory;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class IoTDBDataSourceFactoryTest {

  @Test
  public void testCreateDataSourceAllowsNullProperties() {
    DataSource dataSource = new IoTDBDataSourceFactory().createDataSource(null);

    assertTrue(dataSource instanceof IoTDBDataSource);
    IoTDBDataSource iotdbDataSource = (IoTDBDataSource) dataSource;
    assertNull(iotdbDataSource.getUrl());
    assertNull(iotdbDataSource.getUser());
    assertNull(iotdbDataSource.getPassword());
  }

  @Test
  public void testCreateDataSourceAllowsUrlOnlyProperties() {
    String url = "jdbc:iotdb://localhost:6667";
    Properties properties = new Properties();
    properties.setProperty(DataSourceFactory.JDBC_URL, url);

    IoTDBDataSource dataSource =
        (IoTDBDataSource) new IoTDBDataSourceFactory().createDataSource(properties);

    assertEquals(url, dataSource.getUrl());
    assertNull(dataSource.getUser());
    assertNull(dataSource.getPassword());
    assertEquals(url, properties.getProperty(DataSourceFactory.JDBC_URL));
  }

  @Test
  public void testDataSourceAllowsClearingUserAndPassword() {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    dataSource.setUser("root");
    dataSource.setPassword("root");
    dataSource.setUser(null);
    dataSource.setPassword(null);

    assertNull(dataSource.getUser());
    assertNull(dataSource.getPassword());
  }

  @Test
  public void testDataSourceConstructorAllowsNullPort() {
    IoTDBDataSource dataSource =
        new IoTDBDataSource("jdbc:iotdb://localhost:6667", null, null, null);

    assertEquals(Integer.valueOf(6667), dataSource.getPort());
  }

  @Test
  public void testDataSourceWrapperMethods() throws SQLException {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    assertTrue(dataSource.isWrapperFor(IoTDBDataSource.class));
    assertTrue(dataSource.isWrapperFor(DataSource.class));
    assertFalse(dataSource.isWrapperFor(String.class));
    assertFalse(dataSource.isWrapperFor(null));
    assertSame(dataSource, dataSource.unwrap(IoTDBDataSource.class));
    assertSame(dataSource, dataSource.unwrap(DataSource.class));
  }

  @Test(expected = SQLException.class)
  public void testDataSourceUnwrapRejectsUnsupportedClass() throws SQLException {
    new IoTDBDataSource().unwrap(String.class);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testDataSourceParentLoggerIsUnsupported() throws SQLException {
    new IoTDBDataSource().getParentLogger();
  }

  @Test(expected = SQLException.class)
  public void testDataSourceConnectionWithCredentialsThrowsInvalidUrl() throws SQLException {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    dataSource.setUrl("jdbc:iotdb://test");
    dataSource.getConnection("root", "root");
  }
}
