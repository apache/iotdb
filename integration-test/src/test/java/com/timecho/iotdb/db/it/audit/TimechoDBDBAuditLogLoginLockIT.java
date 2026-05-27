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

import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBDBAuditLogLoginLockIT {

  private static final int FAILED_LOGIN_ATTEMPTS = 5;
  private static final long POLL_TIMEOUT_MS = 30_000;
  private static final long POLL_INTERVAL_MS = 500;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setEnableAuditLog(true)
        .setAuditableOperationType(
            new StringJoiner(",")
                .add(AuditLogOperation.DDL.toString())
                .add(AuditLogOperation.DML.toString())
                .add(AuditLogOperation.QUERY.toString())
                .add(AuditLogOperation.CONTROL.toString())
                .toString())
        .setAuditableOperationLevel(PrivilegeLevel.GLOBAL.toString())
        .setAuditableOperationResult("SUCCESS,FAIL");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testLockAndUnlockAuditLog() throws Exception {
    // 1. Create test user
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE USER locktest 'LockT@12345678'");
    }

    // 2. Trigger failed logins to reach threshold and lock the account
    for (int i = 0; i < FAILED_LOGIN_ATTEMPTS; i++) {
      try {
        EnvFactory.getEnv()
            .getConnection("locktest", "wrongpassword", BaseEnv.TABLE_SQL_DIALECT)
            .close();
      } catch (Exception ignored) {
      }
    }

    // 3. Attempt login while account is locked (triggers LOGIN_EXCEED_LIMIT)
    try {
      EnvFactory.getEnv()
          .getConnection("locktest", "LockT@12345678", BaseEnv.TABLE_SQL_DIALECT)
          .close();
    } catch (Exception ignored) {
    }

    // 4. Manual unlock
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("ALTER USER locktest ACCOUNT UNLOCK");
    }

    // 5. Poll for audit logs with timeout
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {

      // Verify LOGIN_FAIL_MAX_TIMES audit event (account locked)
      waitForAuditEvent(stmt, "LOGIN_FAIL_MAX_TIMES");
      try (ResultSet rs =
          stmt.executeQuery(
              "SELECT log FROM __audit.audit_log"
                  + " WHERE audit_event_type = 'LOGIN_FAIL_MAX_TIMES' ORDER BY time")) {
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("log").toLowerCase().contains("locked"));
      }

      // Verify LOGIN_EXCEED_LIMIT audit event (login rejected while locked)
      waitForAuditEvent(stmt, "LOGIN_EXCEED_LIMIT");
      try (ResultSet rs =
          stmt.executeQuery(
              "SELECT log FROM __audit.audit_log"
                  + " WHERE audit_event_type = 'LOGIN_EXCEED_LIMIT' ORDER BY time")) {
        Assert.assertTrue(rs.next());
        String log = rs.getString("log").toLowerCase();
        Assert.assertTrue(log.contains("rejected") || log.contains("locked"));
      }

      // Verify ACCOUNT_UNLOCKED audit event (manual unlock)
      waitForAuditEvent(stmt, "ACCOUNT_UNLOCKED");
      try (ResultSet rs =
          stmt.executeQuery(
              "SELECT log FROM __audit.audit_log"
                  + " WHERE audit_event_type = 'ACCOUNT_UNLOCKED' ORDER BY time")) {
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("log").toLowerCase().contains("unlocked"));
      }
    }
  }

  private void waitForAuditEvent(Statement stmt, String eventType) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      try (ResultSet rs =
          stmt.executeQuery(
              "SELECT count(*) FROM __audit.audit_log"
                  + " WHERE audit_event_type = '"
                  + eventType
                  + "'")) {
        if (rs.next() && rs.getInt(1) > 0) {
          return;
        }
      }
      TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
    }
    Assert.fail("Timed out waiting for audit event: " + eventType);
  }
}
