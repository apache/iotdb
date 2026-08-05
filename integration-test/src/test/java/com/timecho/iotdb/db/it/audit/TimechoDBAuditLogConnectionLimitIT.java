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

import org.apache.iotdb.commons.audit.AuditEventType;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBAuditLogConnectionLimitIT {

  private static final String USERNAME = "connection_limit_user";
  private static final String PASSWORD = "ConnectionLimit@123";
  private static final long POLL_TIMEOUT_MS = 30_000;
  private static final long POLL_INTERVAL_MS = 500;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDnRpcMaxConcurrentClientNum(12)
        .setTimestampPrecision("ns")
        .setEnableAuditLog(true)
        .setAuditableOperationType(AuditLogOperation.CONTROL.toString())
        .setAuditableOperationLevel(PrivilegeLevel.GLOBAL.toString())
        .setAuditableOperationResult("FAIL")
        .setAuditableControlEventType(AuditEventType.LOGIN_RESOURCE_RESTRICT.toString());
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMaxSessionPerUserRejectionAuditLog() throws Exception {
    try (Connection adminConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = adminConnection.createStatement()) {
      statement.execute("CREATE USER " + USERNAME + " '" + PASSWORD + "'");
      // ClusterTestConnection opens one write session and one read session in this environment.
      statement.execute("ALTER USER " + USERNAME + " SET MAX_SESSION_PER_USER 2");

      try (Connection ignored =
          EnvFactory.getEnv().getConnection(USERNAME, PASSWORD, BaseEnv.TABLE_SQL_DIALECT)) {
        SQLException exception =
            Assert.assertThrows(
                SQLException.class,
                () -> {
                  try (Connection rejected =
                      EnvFactory.getEnv()
                          .getConnection(USERNAME, PASSWORD, BaseEnv.TABLE_SQL_DIALECT)) {
                    // The connection must be rejected before this block is entered.
                  }
                });
        Assert.assertTrue(exception.getMessage().contains("maximum limit"));

        waitForAuditEvent(statement);
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT username, audit_event_type, operation_type, result, log"
                    + " FROM __audit.audit_log"
                    + " WHERE audit_event_type = 'LOGIN_RESOURCE_RESTRICT'"
                    + " AND username = '"
                    + USERNAME
                    + "' ORDER BY time DESC LIMIT 1")) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(USERNAME, resultSet.getString("username"));
          Assert.assertEquals(
              AuditEventType.LOGIN_RESOURCE_RESTRICT.toString(),
              resultSet.getString("audit_event_type"));
          Assert.assertEquals(
              AuditLogOperation.CONTROL.toString(), resultSet.getString("operation_type"));
          Assert.assertFalse(resultSet.getBoolean("result"));
          Assert.assertTrue(resultSet.getString("log").contains("maximum limit"));
        }
      }
    }
  }

  private void waitForAuditEvent(Statement statement) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(*) FROM __audit.audit_log"
                  + " WHERE audit_event_type = 'LOGIN_RESOURCE_RESTRICT'"
                  + " AND username = '"
                  + USERNAME
                  + "'")) {
        if (resultSet.next() && resultSet.getInt(1) > 0) {
          return;
        }
      }
      TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
    }
    Assert.fail("Timed out waiting for LOGIN_RESOURCE_RESTRICT audit event");
  }
}
