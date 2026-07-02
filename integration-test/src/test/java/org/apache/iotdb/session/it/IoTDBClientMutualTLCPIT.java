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
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TLCPIT;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.security.GeneralSecurityException;
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
@Category(TLCPIT.class)
public class IoTDBClientMutualTLCPIT {

  private static final String TLCP_PROTOCOL = "TLCPv1.1";
  private static final String TLCP_KEY_MANAGER_TYPE = "NewSunX509";
  private static final String TLCP_TRUST_MANAGER_TYPE = "PKIX";
  private static final String STORE_PASSWORD = "thrift";
  private static String keyDir;
  private static boolean clusterStarted;

  @BeforeClass
  public static void setUp() throws Exception {
    assumeTLCPEnabled();
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
        .setThriftSSLClientAuth(true)
        .setKeyStorePath(keyStorePath())
        .setKeyStorePwd(STORE_PASSWORD)
        .setTrustStorePath(trustStorePath())
        .setTrustStorePwd(STORE_PASSWORD)
        .setSslProtocol(TLCP_PROTOCOL);
    EnvFactory.getEnv().initClusterEnvironment();
    clusterStarted = true;
  }

  @After
  public void tearDown() {
    try (ISession session = newMutualTLCPSession()) {
      deleteTreeDatabase(session, "root.client_tlcp_tree");
      deleteTreeDatabase(session, "root.client_tlcp_pool");
      deleteTreeDatabase(session, "root.client_tlcp_jdbc");
    } catch (Exception ignored) {
      // ignored
    }
    try (ITableSession session = newMutualTLCPTableSession()) {
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS client_tlcp_table");
    } catch (Exception ignored) {
      // ignored
    }
  }

  @AfterClass
  public static void tearDownClass() {
    if (clusterStarted) {
      EnvFactory.getEnv().cleanClusterEnvironment();
      clusterStarted = false;
    }
  }

  @Test
  public void sslClientWithoutKeyStoreCanNotConnectWhenClientAuthRequired() {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final Session session =
        new Session.Builder()
            .host(dataNode.getIp())
            .port(dataNode.getPort())
            .useSSL(true)
            .trustStore(trustStorePath())
            .trustStorePwd(STORE_PASSWORD)
            .sslProtocol(TLCP_PROTOCOL)
            .enableAutoFetch(false)
            .build();

    assertThrows(IoTDBConnectionException.class, session::open);
  }

  @Test
  public void treeSessionCanConnectWithMutualTLCP() throws Exception {
    try (ISession session = newMutualTLCPSession()) {
      session.executeNonQueryStatement("CREATE DATABASE root.client_tlcp_tree");
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.client_tlcp_tree.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      session.executeNonQueryStatement(
          "INSERT INTO root.client_tlcp_tree.d1(time, s1) VALUES (1, 11)");

      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT s1 FROM root.client_tlcp_tree.d1")) {
        assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getTimestamp());
        assertEquals(11, record.getFields().get(0).getIntV());
        assertFalse(dataSet.hasNext());
      }
    }
  }

  @Test
  public void sessionPoolCanConnectWithMutualTLCP() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final ISessionPool pool =
        new SessionPool.Builder()
            .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
            .maxSize(1)
            .useSSL(true)
            .trustStore(trustStorePath())
            .trustStorePwd(STORE_PASSWORD)
            .keyStore(keyStorePath())
            .keyStorePwd(STORE_PASSWORD)
            .sslProtocol(TLCP_PROTOCOL)
            .build();
    try {
      pool.executeNonQueryStatement("CREATE DATABASE root.client_tlcp_pool");
      pool.executeNonQueryStatement(
          "CREATE TIMESERIES root.client_tlcp_pool.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      pool.executeNonQueryStatement(
          "INSERT INTO root.client_tlcp_pool.d1(time, s1) VALUES (1, 22)");

      try (SessionDataSetWrapper dataSet =
          pool.executeQueryStatement("SELECT s1 FROM root.client_tlcp_pool.d1")) {
        assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getTimestamp());
        assertEquals(22, record.getFields().get(0).getIntV());
        assertFalse(dataSet.hasNext());
      }
    } finally {
      pool.close();
    }
  }

  @Test
  public void tableSessionCanConnectWithMutualTLCP() throws Exception {
    try (ITableSession session = newMutualTLCPTableSession()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS client_tlcp_table");
      session.executeNonQueryStatement("USE client_tlcp_table");
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS tlcp_table (tag1 STRING TAG, value INT32 FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO tlcp_table(time, tag1, value) VALUES (1, 'tag1', 33)");

      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT time, value FROM tlcp_table WHERE tag1 = 'tag1'")) {
        assertTrue(dataSet.hasNext());
        final RowRecord record = dataSet.next();
        assertEquals(1L, record.getFields().get(0).getLongV());
        assertEquals(33, record.getFields().get(1).getIntV());
        assertFalse(dataSet.hasNext());
      }
    }
  }

  @Test
  public void jdbcCanConnectWithMutualTLCP() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + dataNode.getIpAndPortString(), mutualTLCPProperties());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.client_tlcp_jdbc");
      statement.execute(
          "CREATE TIMESERIES root.client_tlcp_jdbc.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.client_tlcp_jdbc.d1(time, s1) VALUES (1, 44)");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM root.client_tlcp_jdbc.d1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals(44, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
    }
  }

  private static ISession newMutualTLCPSession() throws IoTDBConnectionException {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final Session session =
        new Session.Builder()
            .host(dataNode.getIp())
            .port(dataNode.getPort())
            .useSSL(true)
            .trustStore(trustStorePath())
            .trustStorePwd(STORE_PASSWORD)
            .keyStore(keyStorePath())
            .keyStorePwd(STORE_PASSWORD)
            .sslProtocol(TLCP_PROTOCOL)
            .build();
    session.open();
    return session;
  }

  private static ITableSession newMutualTLCPTableSession() throws IoTDBConnectionException {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    return new TableSessionBuilder()
        .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
        .useSSL(true)
        .trustStore(trustStorePath())
        .trustStorePwd(STORE_PASSWORD)
        .keyStore(keyStorePath())
        .keyStorePwd(STORE_PASSWORD)
        .sslProtocol(TLCP_PROTOCOL)
        .build();
  }

  private static Properties mutualTLCPProperties() {
    final Properties properties = new Properties();
    properties.put("user", SessionConfig.DEFAULT_USER);
    properties.put("password", SessionConfig.DEFAULT_PASSWORD);
    properties.put(Config.USE_SSL, Boolean.TRUE.toString());
    properties.put(Config.TRUST_STORE, trustStorePath());
    properties.put(Config.TRUST_STORE_PWD, STORE_PASSWORD);
    properties.put(Config.KEY_STORE, keyStorePath());
    properties.put(Config.KEY_STORE_PWD, STORE_PASSWORD);
    properties.put(Config.SSL_PROTOCOL, TLCP_PROTOCOL);
    return properties;
  }

  private void deleteTreeDatabase(final ISession session, final String database) {
    try {
      session.executeNonQueryStatement("DELETE DATABASE " + database);
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static void assumeTLCPEnabled() {
    Assume.assumeTrue("TLCP IT is disabled", Boolean.getBoolean("tlcp.it"));
    try {
      SSLContext.getInstance(TLCP_PROTOCOL);
      KeyManagerFactory.getInstance(TLCP_KEY_MANAGER_TYPE);
      TrustManagerFactory.getInstance(TLCP_TRUST_MANAGER_TYPE);
    } catch (GeneralSecurityException e) {
      Assume.assumeNoException("TLCPv1.1 is not supported by current JDK", e);
    }
  }

  private static String keyStorePath() {
    return keyDir + "test-keystore-gm";
  }

  private static String trustStorePath() {
    return keyDir + "test-truststore-gm";
  }
}
