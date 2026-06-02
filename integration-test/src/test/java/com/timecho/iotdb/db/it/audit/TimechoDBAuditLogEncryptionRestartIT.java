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

package com.timecho.iotdb.db.it.audit;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class TimechoDBAuditLogEncryptionRestartIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TimechoDBAuditLogEncryptionRestartIT.class);

  private static final String ENCRYPT_TYPE = "com.timecho.iotdb.commons.encrypt.AES128.AES128";
  private static final String ENCRYPT_TOKEN = "thisisourtestkey";
  private static final String DATABASE = "audit_encryption_restart";
  private static final String CREATE_DATABASE_SQL = "CREATE DATABASE " + DATABASE;
  private static final String CREATE_TABLE_SQL =
      "CREATE TABLE audit_encryption_restart_table(device STRING TAG, s STRING FIELD)";
  private static final String INSERT_SQL =
      "INSERT INTO audit_encryption_restart_table(time, device, s) VALUES(1, 'd1', 'v1')";
  private static final long POLL_TIMEOUT_MS = 30_000;
  private static final long POLL_INTERVAL_MS = 500;

  private String originalEncryptionToken;

  @Before
  public void setUp() throws Exception {
    originalEncryptionToken = DataNodeWrapper.getEncryptionToken();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setEnableAuditLog(true)
        .setEncryptType(ENCRYPT_TYPE);
    DataNodeWrapper.setEncryptionToken(ENCRYPT_TOKEN);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
    DataNodeWrapper.setEncryptionToken(originalEncryptionToken);
  }

  @Test
  public void testAuditLogEncryptionAfterRestartWithTDEEnabled() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(CREATE_DATABASE_SQL);
      statement.execute("USE " + DATABASE);
      statement.execute(CREATE_TABLE_SQL);
      statement.execute(INSERT_SQL);
    }

    waitForAuditSql(CREATE_DATABASE_SQL);

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("flush");
    }

    TestUtils.restartCluster(EnvFactory.getEnv());

    LOGGER.info("Restart finished, validating encrypted audit log readability with TDE enabled.");
    assertAuditSqlReadableFromTableModel(CREATE_DATABASE_SQL);
    assertAuditTsFileReadableFromTreeModel();
    LOGGER.info("Encrypted audit log is readable from both table and tree model after restart.");
  }

  private static void waitForAuditSql(String sqlString) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      while (System.currentTimeMillis() < deadline) {
        if (containsAuditSql(statement, sqlString)) {
          return;
        }
        TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
      }
    }
    Assert.fail("Timed out waiting for audit log SQL: " + sqlString);
  }

  private static void assertAuditSqlReadableFromTableModel(String sqlString) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      Assert.assertTrue(
          "Expected encrypted audit log SQL should be readable after restart: " + sqlString,
          containsAuditSql(statement, sqlString));
    }
  }

  private static boolean containsAuditSql(Statement statement, String sqlString) throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT sql_string FROM __audit.audit_log WHERE sql_string = '"
                + escapeSqlLiteral(sqlString)
                + "' ORDER BY time")) {
      return resultSet.next() && sqlString.equals(resultSet.getString("sql_string"));
    }
  }

  private static void assertAuditTsFileReadableFromTreeModel() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE")) {
      Assert.assertTrue(
          "Expected encrypted audit log TsFile should be readable after restart", resultSet.next());
    }
  }

  private static String escapeSqlLiteral(String sqlString) {
    return sqlString.replace("'", "''");
  }
}
