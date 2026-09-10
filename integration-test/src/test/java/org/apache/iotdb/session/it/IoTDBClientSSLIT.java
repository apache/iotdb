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
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({
  LocalStandaloneIT.class,
  ClusterIT.class,
  TableLocalStandaloneIT.class,
  TableClusterIT.class
})
public class IoTDBClientSSLIT {

  private static final String STORE_PASSWORD = "thrift";
  private static String keyDir;

  @BeforeClass
  public static void setUp() throws Exception {
    keyDir =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator;

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableThriftClientSSL(true)
        .setKeyStorePath(keyStorePath())
        .setKeyStorePwd(STORE_PASSWORD)
        .setTrustStorePath(trustStorePath())
        .setTrustStorePwd(STORE_PASSWORD)
        .setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    try (ISession session = newSSLSession()) {
      deleteTreeDatabase(session, "root.client_ssl_tree");
      deleteTreeDatabase(session, "root.client_ssl_jdbc");
    } catch (Exception ignored) {
      // ignored
    }
    try (ITableSession session = newSSLTableSession()) {
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS client_ssl_table");
    } catch (Exception ignored) {
      // ignored
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void nonSSLClientCanNotConnectToSSLPort() {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final Session session =
        new Session.Builder().host(dataNode.getIp()).port(dataNode.getPort()).build();

    assertThrows(IoTDBConnectionException.class, session::open);
  }

  @Test
  public void treeSessionCanConnectWithSSL() throws Exception {
    try (ISession session = newSSLSession()) {
      session.executeNonQueryStatement("CREATE DATABASE root.client_ssl_tree");
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.client_ssl_tree.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      session.executeNonQueryStatement(
          "INSERT INTO root.client_ssl_tree.d1(time, s1) VALUES (1, 11)");

      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT s1 FROM root.client_ssl_tree.d1")) {
        assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getTimestamp());
        assertEquals(11, record.getFields().get(0).getIntV());
        assertFalse(dataSet.hasNext());
      }
    }
  }

  @Test
  public void tableSessionCanConnectWithSSL() throws Exception {
    try (ITableSession session = newSSLTableSession()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS client_ssl_table");
      session.executeNonQueryStatement("USE client_ssl_table");
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS ssl_table (tag1 STRING TAG, value INT32 FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO ssl_table(time, tag1, value) VALUES (1, 'tag1', 22)");

      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT time, value FROM ssl_table WHERE tag1 = 'tag1'")) {
        assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getFields().get(0).getLongV());
        assertEquals(22, record.getFields().get(1).getIntV());
        assertFalse(dataSet.hasNext());
      }
    }
  }

  @Test
  public void jdbcCanConnectWithSSL() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + dataNode.getIpAndPortString(), sslProperties());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.client_ssl_jdbc");
      statement.execute(
          "CREATE TIMESERIES root.client_ssl_jdbc.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.client_ssl_jdbc.d1(time, s1) VALUES (1, 33)");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.client_ssl_jdbc.d1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals(33, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
    }
  }

  private static ISession newSSLSession() throws IoTDBConnectionException {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final Session session =
        new Session.Builder()
            .host(dataNode.getIp())
            .port(dataNode.getPort())
            .useSSL(true)
            .trustStore(trustStorePath())
            .trustStorePwd(STORE_PASSWORD)
            .build();
    session.open();
    return session;
  }

  private static ITableSession newSSLTableSession() throws IoTDBConnectionException {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    return new TableSessionBuilder()
        .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
        .useSSL(true)
        .trustStore(trustStorePath())
        .trustStorePwd(STORE_PASSWORD)
        .build();
  }

  private static Properties sslProperties() {
    final Properties properties = new Properties();
    properties.put("user", SessionConfig.DEFAULT_USER);
    properties.put("password", SessionConfig.DEFAULT_PASSWORD);
    properties.put(Config.USE_SSL, Boolean.TRUE.toString());
    properties.put(Config.TRUST_STORE, trustStorePath());
    properties.put(Config.TRUST_STORE_PWD, STORE_PASSWORD);
    return properties;
  }

  private void deleteTreeDatabase(final ISession session, final String database) {
    try {
      session.executeNonQueryStatement("DELETE DATABASE " + database);
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static String keyStorePath() {
    return keyDir + "test-keystore";
  }

  private static String trustStorePath() {
    return keyDir + "test-truststore";
  }
}
