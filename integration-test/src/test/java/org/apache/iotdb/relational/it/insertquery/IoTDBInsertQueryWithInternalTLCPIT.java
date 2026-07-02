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

package org.apache.iotdb.relational.it.insertquery;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TLCPIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.RpcSslUtils;

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
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category(TLCPIT.class)
public class IoTDBInsertQueryWithInternalTLCPIT {

  private static final String TLCP_PROTOCOL = "TLCPv1.1";
  private static final String TLCP_KEY_MANAGER_TYPE = "NewSunX509";
  private static final String TLCP_TRUST_MANAGER_TYPE = "PKIX";
  private static final String STORE_PASSWORD = "thrift";
  private static final String KEY_DIR =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;
  private static boolean clusterStarted;

  @BeforeClass
  public static void setUp() {
    assumeTLCPEnabled();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000)
        .setEnableInternalSSL(true)
        .setKeyStorePath(keyStorePath())
        .setKeyStorePwd(STORE_PASSWORD)
        .setTrustStorePath(trustStorePath())
        .setTrustStorePwd(STORE_PASSWORD)
        .setSslProtocol(TLCP_PROTOCOL);
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(true);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath(keyStorePath());
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd(STORE_PASSWORD);
    CommonDescriptor.getInstance().getConfig().setTrustStorePath(trustStorePath());
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd(STORE_PASSWORD);
    CommonDescriptor.getInstance().getConfig().setSslProtocol(TLCP_PROTOCOL);
    RpcSslUtils.configure(TLCP_PROTOCOL);
    EnvFactory.getEnv().initClusterEnvironment();
    clusterStarted = true;
  }

  @AfterClass
  public static void tearDown() {
    Exception exception = null;
    if (clusterStarted) {
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP DATABASE IF EXISTS internal_tlcp");
      } catch (Exception e) {
        exception = e;
      } finally {
        EnvFactory.getEnv().cleanClusterEnvironment();
        clusterStarted = false;
      }
    }
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(false);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath("");
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePath("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd("");
    CommonDescriptor.getInstance().getConfig().setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
    RpcSslUtils.configure(SessionConfig.DEFAULT_SSL_PROTOCOL);
    if (exception != null) {
      fail(exception.getMessage());
    }
  }

  @Test
  public void tableInsertQueryCanWorkWithInternalTLCP() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS internal_tlcp");
      statement.execute("USE internal_tlcp");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS vehicle(device_id STRING TAG, temperature INT32 FIELD)");
      statement.execute("INSERT INTO vehicle(time, device_id, temperature) VALUES (1, 'd1', 36)");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT time, temperature FROM vehicle WHERE device_id = 'd1'")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals(36, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
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
    return KEY_DIR + "test-keystore-gm";
  }

  private static String trustStorePath() {
    return KEY_DIR + "test-truststore-gm";
  }
}
