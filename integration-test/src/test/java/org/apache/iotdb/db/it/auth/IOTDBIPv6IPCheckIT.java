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

package org.apache.iotdb.db.it.auth;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.IPv6TestUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.IoTDBSetConfigurationIT.checkConfigFileContains;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IOTDBIPv6IPCheckIT {

  private String previousTestNodeAddress;

  @Before
  public void setUp() throws Exception {
    IPv6TestUtils.assumeIPv6LoopbackAvailable();
    previousTestNodeAddress = IPv6TestUtils.setTestNodeAddressToIPv6Loopback();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      IPv6TestUtils.restoreTestNodeAddress(previousTestNodeAddress);
    }
  }

  @Test
  public void testIPv6WhiteListWithExactMatchAllowsConnection() throws Exception {
    setConfiguration("enable_white_list='true', enable_black_list='false', white_ip_list='::1'");
    assertConfiguration("enable_white_list=true", "white_ip_list=::1");

    try (Connection ignored = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      Assert.assertFalse(ignored.isClosed());
    }
  }

  @Test
  public void testIPv6WhiteListWithCidrAllowsConnection() throws Exception {
    setConfiguration(
        "enable_white_list='true', enable_black_list='false', white_ip_list='::1/128'");
    assertConfiguration("enable_white_list=true", "white_ip_list=::1/128");

    try (Connection ignored = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      Assert.assertFalse(ignored.isClosed());
    }
  }

  @Test
  public void testIPv6BlackListWithExactMatchRejectsConnection() throws Exception {
    setConfiguration("enable_white_list='false', enable_black_list='true', black_ip_list='::1'");
    assertConfiguration("enable_black_list=true", "black_ip_list=::1");

    assertConnectionRejected();
  }

  @Test
  public void testIPv6BlackListWithCidrRejectsConnection() throws Exception {
    setConfiguration(
        "enable_white_list='false', enable_black_list='true', black_ip_list='::1/128'");
    assertConfiguration("enable_black_list=true", "black_ip_list=::1/128");

    assertConnectionRejected();
  }

  @Test
  public void testIPv6BlackListTakesPrecedenceOverWhiteList() throws Exception {
    setConfiguration(
        "enable_white_list='true', enable_black_list='true', white_ip_list='::1',"
            + " black_ip_list='::1'");
    assertConfiguration(
        "enable_white_list=true",
        "enable_black_list=true",
        "white_ip_list=::1",
        "black_ip_list=::1");

    assertConnectionRejected();
  }

  private void setConfiguration(String configuration) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION " + configuration);
    }
  }

  private void assertConfiguration(String... expectedContents) {
    for (AbstractNodeWrapper nodeWrapper : EnvFactory.getEnv().getConfigNodeWrapperList()) {
      Assert.assertTrue(checkConfigFileContains(nodeWrapper, expectedContents));
    }
    for (AbstractNodeWrapper nodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      Assert.assertTrue(checkConfigFileContains(nodeWrapper, expectedContents));
    }
  }

  private void assertConnectionRejected() {
    Assert.assertThrows(
        SQLException.class, () -> EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT));
  }
}
