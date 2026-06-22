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

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBDriverTest {

  @Test
  public void testAcceptsUrl() {
    IoTDBDriver driver = new IoTDBDriver();

    assertTrue(driver.acceptsURL("jdbc:iotdb://localhost:6667"));
    assertFalse(driver.acceptsURL(null));
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306"));
  }

  @Test
  public void testDriverVersion() {
    IoTDBDriver driver = new IoTDBDriver();

    assertEquals(Config.DRIVER_MAJOR_VERSION, driver.getMajorVersion());
    assertEquals(Config.DRIVER_MINOR_VERSION, driver.getMinorVersion());
  }

  @Test
  public void testGetPropertyInfo() throws SQLException {
    IoTDBDriver driver = new IoTDBDriver();
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, "root");
    properties.setProperty(Config.AUTH_PASSWORD, "secret");
    properties.setProperty(Config.USE_SSL, "true");
    properties.setProperty(Config.SSL_PROTOCOL, "TLSv1.3");
    properties.setProperty(Config.TRUST_STORE_PWD, "trust-store-secret");

    DriverPropertyInfo[] propertyInfos =
        driver.getPropertyInfo("jdbc:iotdb://localhost:6667", properties);

    assertTrue(propertyInfos.length > 0);
    assertEquals("root", findProperty(propertyInfos, Config.AUTH_USER).value);
    assertNull(findProperty(propertyInfos, Config.AUTH_PASSWORD).value);
    assertNull(findProperty(propertyInfos, Config.TRUST_STORE_PWD).value);
    assertEquals("true", findProperty(propertyInfos, Config.USE_SSL).value);
    assertEquals("TLSv1.3", findProperty(propertyInfos, Config.SSL_PROTOCOL).value);
    assertEquals(
        Arrays.asList("true", "false"),
        Arrays.asList(findProperty(propertyInfos, Config.USE_SSL).choices));
    assertEquals(
        Arrays.asList(Constant.TREE, Constant.TABLE),
        Arrays.asList(findProperty(propertyInfos, Config.SQL_DIALECT).choices));
    assertEquals(Config.DEFAULT_VERSION.name(), findProperty(propertyInfos, Config.VERSION).value);
  }

  @Test
  public void testGetPropertyInfoAllowsNullProperties() throws SQLException {
    IoTDBDriver driver = new IoTDBDriver();

    DriverPropertyInfo[] propertyInfos =
        driver.getPropertyInfo("jdbc:iotdb://localhost:6667", null);

    assertNotNull(propertyInfos);
    assertEquals(Config.DEFAULT_USER, findProperty(propertyInfos, Config.AUTH_USER).value);
  }

  @Test
  public void testGetPropertyInfoMergesUrlPropertiesWithoutMutatingInput() throws SQLException {
    IoTDBDriver driver = new IoTDBDriver();
    Properties properties = new Properties();
    properties.setProperty(Config.USE_SSL, "false");
    boolean originalRpcCompression = Config.rpcThriftCompressionEnable;
    Config.rpcThriftCompressionEnable = false;
    try {
      DriverPropertyInfo[] propertyInfos =
          driver.getPropertyInfo(
              "jdbc:iotdb://localhost:6667?user=url-user&thrift_default_buffer_capacity=1024&thrift_max_frame_size=2048&use_ssl=true&sql_dialect=table&network_timeout=123&rpc_compress=true",
              properties);

      assertEquals("url-user", findProperty(propertyInfos, Config.AUTH_USER).value);
      assertEquals("1024", findProperty(propertyInfos, Config.DEFAULT_BUFFER_CAPACITY).value);
      assertEquals("2048", findProperty(propertyInfos, Config.THRIFT_FRAME_MAX_SIZE).value);
      assertEquals("true", findProperty(propertyInfos, Config.USE_SSL).value);
      assertEquals(Constant.TABLE, findProperty(propertyInfos, Config.SQL_DIALECT).value);
      assertEquals("123", findProperty(propertyInfos, Config.NETWORK_TIMEOUT).value);
      assertEquals("true", findProperty(propertyInfos, Utils.RPC_COMPRESS).value);
      assertEquals("false", properties.getProperty(Config.USE_SSL));
      assertFalse(Config.rpcThriftCompressionEnable);
    } finally {
      Config.rpcThriftCompressionEnable = originalRpcCompression;
    }
  }

  @Test(expected = SQLException.class)
  public void testGetPropertyInfoRejectsInvalidIoTDBUrl() throws SQLException {
    new IoTDBDriver().getPropertyInfo("jdbc:iotdb://localhost", new Properties());
  }

  @Test(expected = SQLException.class)
  public void testGetPropertyInfoRejectsInvalidUrlProperties() throws SQLException {
    new IoTDBDriver()
        .getPropertyInfo("jdbc:iotdb://localhost:6667?network_timeout=-1", new Properties());
  }

  private static DriverPropertyInfo findProperty(DriverPropertyInfo[] propertyInfos, String name) {
    for (DriverPropertyInfo propertyInfo : propertyInfos) {
      if (name.equals(propertyInfo.name)) {
        return propertyInfo;
      }
    }
    fail("Missing driver property: " + name);
    return null;
  }
}
