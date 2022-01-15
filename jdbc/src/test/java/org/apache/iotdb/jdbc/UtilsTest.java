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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
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
}
