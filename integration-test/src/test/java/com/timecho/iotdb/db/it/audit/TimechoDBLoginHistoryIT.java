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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBLoginHistoryIT {

  private static final long POLL_TIMEOUT_MS = 30_000;
  private static final long POLL_INTERVAL_MS = 500;

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void loginHistoryViewShouldBeCreatedAfterFirstSuccessfulLogin() throws Exception {
    initClusterWithAuditLog(true);

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      waitUntilLoginHistoryVisible(statement);
      Assert.assertTrue(
          "Expected root successful login record in __audit.login_history",
          hasRootSuccessfulLoginRecord(statement));
    }
  }

  @Test
  public void loginHistoryViewShouldNotBeCreatedWhenAuditLogDisabled() throws Exception {
    initClusterWithAuditLog(false);

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      Assert.assertFalse(
          "login_history should not be visible when audit log is disabled",
          isLoginHistoryVisible(statement));
      Assert.assertThrows(
          SQLException.class, () -> statement.executeQuery("SELECT * FROM __audit.login_history"));
    }
  }

  private static void initClusterWithAuditLog(boolean enableAuditLog) throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setEnableAuditLog(enableAuditLog);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  private static void waitUntilLoginHistoryVisible(Statement statement) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    SQLException lastException = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (isLoginHistoryVisible(statement)) {
          return;
        }
      } catch (SQLException e) {
        lastException = e;
      }
      TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
    }
    if (lastException != null) {
      Assert.fail(
          "Timed out waiting for __audit.login_history to appear. Last error: "
              + lastException.getMessage());
    }
    Assert.fail("Timed out waiting for __audit.login_history to appear");
  }

  private static boolean isLoginHistoryVisible(Statement statement) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM information_schema.tables")) {
      int databaseIndex = findColumn(resultSet, "database");
      int tableNameIndex = findColumn(resultSet, "table_name");
      while (resultSet.next()) {
        if ("__audit".equals(resultSet.getString(databaseIndex))
            && "login_history".equals(resultSet.getString(tableNameIndex))) {
          return true;
        }
      }
      return false;
    }
  }

  private static boolean hasRootSuccessfulLoginRecord(Statement statement) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM __audit.login_history")) {
      int usernameIndex = findColumn(resultSet, "username");
      int resultIndex = findColumn(resultSet, "result");
      while (resultSet.next()) {
        if ("root".equals(resultSet.getString(usernameIndex))
            && resultSet.getBoolean(resultIndex)) {
          return true;
        }
      }
      return false;
    }
  }

  private static int findColumn(ResultSet resultSet, String expectedColumn) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    String normalizedExpected = expectedColumn.toLowerCase(Locale.ENGLISH);
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String label = metaData.getColumnLabel(i);
      String name = metaData.getColumnName(i);
      if (matchesColumn(label, normalizedExpected) || matchesColumn(name, normalizedExpected)) {
        return i;
      }
    }
    throw new SQLException("Column " + expectedColumn + " does not exist in result set");
  }

  private static boolean matchesColumn(String actualColumn, String expectedColumn) {
    if (actualColumn == null) {
      return false;
    }
    String normalizedActual = actualColumn.toLowerCase(Locale.ENGLISH);
    return normalizedActual.equals(expectedColumn)
        || normalizedActual.endsWith("." + expectedColumn);
  }
}
