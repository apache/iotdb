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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UtilsTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testParseURL() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host1 = "localhost";
    int port = 6667;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host1, port), properties);
    assertEquals(host1, params.getHost());
    assertEquals(port, params.getPort());
    assertEquals(userName, params.getUsername());
    assertEquals(userPwd, params.getPassword());

    params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s", host1, port), properties);
    assertEquals(params.getHost(), host1);
    assertEquals(params.getPort(), port);
    assertEquals(params.getUsername(), userName);
    assertEquals(params.getPassword(), userPwd);
  }

  @Test
  public void testParseIPV6URL() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host1 =
        "AD80:E32B:CA25:B3AE:DA4A:DAAF:EEAE:BBBE,AD80:E32B:CA25:B3AE:DAAA:DAAF:CADE:EEAE:BBBE,AD80:E32B:CA25:B3AE:DA4A:DAAF:EEAE:BBBE";
    int port = 6667;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host1, port), properties);
    assertEquals(host1, params.getHost());
    assertEquals(port, params.getPort());
    assertEquals(userName, params.getUsername());
    assertEquals(userPwd, params.getPassword());

    params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s", host1, port), properties);
    assertEquals(params.getHost(), host1);
    assertEquals(params.getPort(), port);
    assertEquals(params.getUsername(), userName);
    assertEquals(params.getPassword(), userPwd);
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongUrl1() throws IoTDBURLException {
    Properties properties = new Properties();
    Utils.parseUrl("jdbc:iotdb//test6667", properties);
  }

  @Test
  public void testParseDomainName() throws IoTDBURLException {
    Properties properties = new Properties();
    final IoTDBConnectionParams params = Utils.parseUrl("jdbc:iotdb://test:6667", properties);

    assertEquals("test", params.getHost());
    assertEquals(6667, params.getPort());
  }

  @Test
  public void testParseUrlAllowsNullProperties() throws IoTDBURLException {
    IoTDBConnectionParams params = Utils.parseUrl("jdbc:iotdb://test:6667", null);

    assertEquals("test", params.getHost());
    assertEquals(6667, params.getPort());
    assertEquals(Config.DEFAULT_USER, params.getUsername());
    assertEquals(Config.DEFAULT_PASSWORD, params.getPassword());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongUrl2() throws IoTDBURLException {
    Properties properties = new Properties();
    Utils.parseUrl("jdbc:iotdb//test:test:test:6667:6667", properties);
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongUrl3() throws IoTDBURLException {
    Properties properties = new Properties();
    Utils.parseUrl("jdbc:iotdb//6667?rpc_compress=1", properties);
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongUrl4() throws IoTDBURLException {
    Properties properties = new Properties();
    Utils.parseUrl("jdbc:iotdb//6667?rpc_compress=true&aaa=bbb", properties);
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongUrlWithoutPort() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://test", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongPort() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host = "localhost";
    int port = 66699999;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host, port), properties);
  }

  @Test
  public void testVerifySuccess() {
    try {
      RpcUtils.verifySuccess(RpcUtils.SUCCESS_STATUS);
    } catch (Exception e) {
      fail();
    }

    try {
      TSStatus errorStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      RpcUtils.verifySuccess(errorStatus);
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testRpcCompress() throws IoTDBURLException {
    Properties properties = new Properties();
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?rpc_compress=true", properties);
    assertTrue(Config.rpcThriftCompressionEnable);
  }

  @Test
  public void testParseUrlParamValueAllowsEqualsSign() throws IoTDBURLException {
    Properties properties = new Properties();

    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?trust_store_pwd=a=b=c", properties);

    assertEquals("a=b=c", properties.getProperty(Config.TRUST_STORE_PWD));
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseUrlParamRejectsEmptyValue() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?use_ssl=", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseUrlParamRejectsTrailingSeparator() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?use_ssl=true&", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseUrlParamRejectsInvalidBooleanValue() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?use_ssl=abc", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseUrlParamRejectsInvalidVersionValue() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?version=bad", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseUrlParamRejectsInvalidNetworkTimeoutValue() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?network_timeout=bad", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseUrlParamRejectsInvalidSqlDialectValue() throws IoTDBURLException {
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?sql_dialect=bad", new Properties());
  }

  @Test
  public void testParseUrlRejectsInvalidConnectionProperties() {
    assertInvalidProperty(Config.DEFAULT_BUFFER_CAPACITY, "bad");
    assertInvalidProperty(Config.THRIFT_FRAME_MAX_SIZE, "bad");
    assertInvalidProperty(Config.VERSION, "bad");
    assertInvalidProperty(Config.NETWORK_TIMEOUT, "bad");
    assertInvalidProperty(Config.TIME_ZONE, "bad-time-zone");
    assertInvalidProperty(Config.CHARSET, "bad-charset");
    assertInvalidProperty(Config.USE_SSL, "bad");
    assertInvalidProperty(Config.SQL_DIALECT, "bad");
  }

  private static void assertInvalidProperty(String key, String value) {
    Properties properties = new Properties();
    properties.setProperty(key, value);
    try {
      Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667", properties);
    } catch (IoTDBURLException e) {
      assertTrue(e.getMessage().contains(key));
      return;
    }
    fail("Expected IoTDBURLException for invalid property " + key);
  }
}
