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

import java.util.Optional;
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

  @Test
  public void testParseBracketedIPV6URL() throws IoTDBURLException {
    String host = "AD80:E32B:CA25:B3AE:DA4A:DAAF:EEAE:BBBE";
    int port = 6667;
    Properties properties = new Properties();

    IoTDBConnectionParams params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "[%s]:%s/", host, port), properties);
    assertEquals(host, params.getHost());
    assertEquals(port, params.getPort());

    params =
        Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "[%s]:%s", host, port), properties);
    assertEquals(host, params.getHost());
    assertEquals(port, params.getPort());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseBracketedIPV6URLWithoutPortSeparator() throws IoTDBURLException {
    Utils.parseUrl(Config.IOTDB_URL_PREFIX + "[::1]6667", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseBracketedIPV6URLWithoutPort() throws IoTDBURLException {
    Utils.parseUrl(Config.IOTDB_URL_PREFIX + "[::1]:", new Properties());
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseBracketedIPV6URLWithoutEndMark() throws IoTDBURLException {
    Utils.parseUrl(Config.IOTDB_URL_PREFIX + "[::1:6667", new Properties());
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

  @Test
  public void testRpcCompress() throws IoTDBURLException {
    Properties properties = new Properties();
    Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667?rpc_compress=true", properties);
    assertTrue(Config.rpcThriftCompressionEnable);
  }

  @Test
  public void testParseSslConfig() throws IoTDBURLException {
    Properties properties = new Properties();
    properties.setProperty(Config.TRUST_STORE, "/tmp/truststore.p12");
    properties.setProperty(Config.TRUST_STORE_PWD, "trust_pass");
    properties.setProperty(Config.KEY_STORE, "/tmp/keystore.p12");
    properties.setProperty(Config.KEY_STORE_PWD, "key_pass");
    IoTDBConnectionParams params =
        Utils.parseUrl(
            "jdbc:iotdb://127.0.0.1:6667?use_ssl=true&ssl_protocol=ProviderProtocol", properties);

    assertTrue(params.isUseSSL());
    assertEquals("ProviderProtocol", params.getSslProtocol());
    assertEquals("/tmp/truststore.p12", params.getTrustStore());
    assertEquals("trust_pass", params.getTrustStorePwd());
    assertEquals("/tmp/keystore.p12", params.getKeyStore());
    assertEquals("key_pass", params.getKeyStorePwd());
  }

  @Test
  public void testParseSslConfigFromUrl() throws IoTDBURLException {
    IoTDBConnectionParams params =
        Utils.parseUrl(
            "jdbc:iotdb://127.0.0.1:6667?use_ssl=true"
                + "&trust_store=/tmp/url-truststore.p12"
                + "&trust_store_pwd=url_trust_pass"
                + "&key_store=/tmp/url-keystore.p12"
                + "&key_store_pwd=url_key_pass"
                + "&ssl_protocol=ProviderProtocol",
            new Properties());

    assertTrue(params.isUseSSL());
    assertEquals("ProviderProtocol", params.getSslProtocol());
    assertEquals("/tmp/url-truststore.p12", params.getTrustStore());
    assertEquals("url_trust_pass", params.getTrustStorePwd());
    assertEquals("/tmp/url-keystore.p12", params.getKeyStore());
    assertEquals("url_key_pass", params.getKeyStorePwd());
  }

  @Test
  public void testParseSslConfigFromBracketedIpv6Url() throws IoTDBURLException {
    IoTDBConnectionParams params =
        Utils.parseUrl(
            "jdbc:iotdb://[::1]:6667/ipv6_db?use_ssl=true&sql_dialect=table", new Properties());

    assertEquals("::1", params.getHost());
    assertEquals(6667, params.getPort());
    assertEquals(Optional.of("ipv6_db"), params.getDb());
    assertEquals(Constant.TABLE_DIALECT, params.getSqlDialect());
    assertTrue(params.isUseSSL());
  }
}
