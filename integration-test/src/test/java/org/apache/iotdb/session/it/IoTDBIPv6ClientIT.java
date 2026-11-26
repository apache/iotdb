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

package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.IPv6TestUtils;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;

import static org.apache.iotdb.it.utils.IPv6TestUtils.IPV6_LOOPBACK_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBIPv6ClientIT {

  private static String previousTestNodeAddress;

  @BeforeClass
  public static void setUp() {
    IPv6TestUtils.assumeIPv6LoopbackAvailable();
    previousTestNodeAddress = IPv6TestUtils.setTestNodeAddressToIPv6Loopback();
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
    IPv6TestUtils.restoreTestNodeAddress(previousTestNodeAddress);
  }

  @Test
  public void clientsCanConnectThroughIPv6Loopback() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNode.getIp());
    assertTrue(dataNode.getIpAndPortString().startsWith("[::1]:"));

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + dataNode.getIpAndPortString(),
                System.getProperty("User", "root"),
                System.getProperty("Password", "root"));
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ipv6_client");
      statement.execute("CREATE TIMESERIES root.ipv6_client.d1.s1 INT64");
      statement.execute("INSERT INTO root.ipv6_client.d1(time, s1) VALUES (1, 100)");
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.ipv6_client.d1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals(100L, resultSet.getLong(2));
        assertFalse(resultSet.next());
      }
    }

    try (ISession session =
        EnvFactory.getEnv()
            .getSessionConnection(Collections.singletonList(dataNode.getIpAndPortString()))) {
      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT s1 FROM root.ipv6_client.d1")) {
        Assert.assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getTimestamp());
        assertEquals(100L, record.getFields().get(0).getLongV());
        Assert.assertFalse(dataSet.hasNext());
      }
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT s1 FROM root.ipv6_client.d1")) {
        Assert.assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getTimestamp());
        assertEquals(100L, record.getFields().get(0).getLongV());
        Assert.assertFalse(dataSet.hasNext());
      }
    }

    verifyTableSession(dataNode);
  }

  private static void verifyTableSession(final DataNodeWrapper dataNode)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session =
        EnvFactory.getEnv()
            .getTableSessionConnection(Collections.singletonList(dataNode.getIpAndPortString()))) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS ipv6_table");
      session.executeNonQueryStatement("USE ipv6_table");
      session.executeNonQueryStatement("CREATE TABLE table1(tag1 STRING TAG, s1 INT64 FIELD)");
      session.executeNonQueryStatement("INSERT INTO table1(time, tag1, s1) VALUES (1, 'd1', 200)");
      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT time, s1 FROM table1 WHERE tag1 = 'd1'")) {
        Assert.assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getFields().get(0).getLongV());
        assertEquals(200L, record.getFields().get(1).getLongV());
        Assert.assertFalse(dataSet.hasNext());
      }
    }
  }
}
