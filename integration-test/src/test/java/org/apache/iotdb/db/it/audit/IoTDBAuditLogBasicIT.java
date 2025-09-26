/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.audit;

import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAuditLogBasicIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAuditLogBasicIT.class);

  /**
   * Ensure the Audit log behave exactly the same as we expected, including number, sequence and
   * content.
   */
  private static final List<String> AUDIT_TABLE_COLUMNS =
      Arrays.asList(
          AbstractAuditLogger.AUDIT_LOG_NODE_ID,
          AbstractAuditLogger.AUDIT_LOG_USER_ID,
          AbstractAuditLogger.AUDIT_LOG_USERNAME,
          AbstractAuditLogger.AUDIT_LOG_CLI_HOSTNAME,
          AbstractAuditLogger.AUDIT_LOG_AUDIT_EVENT_TYPE,
          AbstractAuditLogger.AUDIT_LOG_OPERATION_TYPE,
          AbstractAuditLogger.AUDIT_LOG_PRIVILEGE_TYPE,
          AbstractAuditLogger.AUDIT_LOG_PRIVILEGE_LEVEL,
          AbstractAuditLogger.AUDIT_LOG_RESULT,
          AbstractAuditLogger.AUDIT_LOG_DATABASE,
          AbstractAuditLogger.AUDIT_LOG_SQL_STRING,
          AbstractAuditLogger.AUDIT_LOG_LOG);

  private static final List<String> AUDIT_TABLE_DATA_TYPES =
      Arrays.asList(
          "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "BOOLEAN",
          "STRING", "STRING", "STRING");

  private static final List<String> AUDIT_TABLE_CATEGORIES =
      Arrays.asList(
          "TAG", "TAG", "FIELD", "FIELD", "FIELD", "FIELD", "FIELD", "FIELD", "FIELD", "FIELD",
          "FIELD", "FIELD");

  private static final boolean ENABLE_AUDIT_LOG = true;
  private static final String AUDITABLE_OPERATION_TYPE =
      new StringJoiner(",")
          .add(AuditLogOperation.DDL.toString())
          .add(AuditLogOperation.DML.toString())
          .add(AuditLogOperation.QUERY.toString())
          .add(AuditLogOperation.CONTROL.toString())
          .toString();

  private static final String AUDITABLE_OPERATION_LEVEL = PrivilegeLevel.GLOBAL.toString();

  private static final String AUDITABLE_OPERATION_RESULT = "SUCCESS,FAIL";

  @BeforeClass
  public static void setUp() throws SQLException {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAuditLog(ENABLE_AUDIT_LOG)
        .setAuditableOperationType(AUDITABLE_OPERATION_TYPE)
        .setAuditableOperationLevel(AUDITABLE_OPERATION_LEVEL)
        .setAuditableOperationResult(AUDITABLE_OPERATION_RESULT);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // Ensure there exists audit database in tree model
      ResultSet resultSet = statement.executeQuery("SHOW DATABASES root.__audit");
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals("root.__audit", resultSet.getString(1));
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // Ensure there exists audit table
      ResultSet resultSet = statement.executeQuery("DESC __audit.audit_log");
      resultSet.next(); // Skip time column
      int cnt = 0;
      while (resultSet.next()) {
        Assert.assertEquals(AUDIT_TABLE_COLUMNS.get(cnt), resultSet.getString(1));
        Assert.assertEquals(AUDIT_TABLE_DATA_TYPES.get(cnt), resultSet.getString(2));
        Assert.assertEquals(AUDIT_TABLE_CATEGORIES.get(cnt), resultSet.getString(3));
        cnt++;
      }
      Assert.assertEquals(AUDIT_TABLE_COLUMNS.size(), cnt);
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static final List<String> TABLE_MODEL_AUDIT_SQLS =
      Arrays.asList(
          "CREATE DATABASE test",
          //          "SHOW DATABASES",
          "ALTER DATABASE test SET PROPERTIES TTL='INF'",
          "USE test",
          "CREATE TABLE table1(t1 STRING TAG, a1 STRING ATTRIBUTE, s1 TEXT FIELD)",
          "SHOW TABLES",
          "DESC table1",
          "ALTER TABLE table1 set properties TTL='INF'",
          "INSERT INTO table1(time, t1, a1, s1) values(1, 't1', 'a1', 's1')",
          "DELETE FROM table1",
          "DROP TABLE table1",
          "DROP DATABASE IF EXISTS test");
  private static final List<List<String>> TABLE_MODEL_AUDIT_FIELDS =
      Arrays.asList(
          // Start DataNode
          Arrays.asList(
              "node_1",
              "u_none",
              "null",
              "null",
              "CHANGE_AUDIT_OPTION",
              "CONTROL",
              "[AUDIT]",
              "GLOBAL",
              "true",
              "null",
              "null",
              "Successfully start the Audit service with configurations (auditableOperationType [DDL, DML, QUERY, CONTROL], auditableOperationLevel GLOBAL, auditableOperationResult SUCCESS,FAIL)"),
          // Create password history TODO: @Hongzhi Gao move password history under __audit
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "null",
              "null",
              "true",
              "root.__system",
              "null",
              "User root (ID=0) requests authority on object root.__system with result true"),
          // Show audit database TODO: Fix typo in tree model
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[MANAGE_DATABASE]",
              "GLOBAL",
              "true",
              "[root.__audit]",
              "null",
              "User root (ID=0) requests authority on object root.__audit with result true"),
          // Desc audit table
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[READ_SCHEMA]",
              "OBJECT",
              "true",
              "__audit",
              "DESC __audit.audit_log",
              "User root (ID=0) requests authority on object audit_log with result true"),
          // Create database
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[CREATE]",
              "OBJECT",
              "true",
              "test",
              "CREATE DATABASE test",
              "User root (ID=0) requests authority on object test with result true"),
          // Show database TODO: Enable only when the order of show databases authentication fixed
          //        Arrays.asList(
          //          "node_1",
          //          "u_0",
          //          "root",
          //          "127.0.0.1",
          //          "OBJECT_AUTHENTICATION",
          //          "QUERY",
          //          "[READ_SCHEMA]",
          //          "OBJECT",
          //          "true",
          //          "test",
          //          "SHOW DATABASES",
          //          "User root (ID=0) requests authority on object test with result true"),
          //          Arrays.asList(
          //              "node_1",
          //              "u_0",
          //              "root",
          //              "127.0.0.1",
          //              "OBJECT_AUTHENTICATION",
          //              "QUERY",
          //              "[READ_SCHEMA]",
          //              "OBJECT",
          //              "true",
          //              "information_schema",
          //              "SHOW DATABASES",
          //              "User root (ID=0) requests authority on object information_schema with
          // result true"),
          //          Arrays.asList(
          //              "node_1",
          //              "u_0",
          //              "root",
          //              "127.0.0.1",
          //              "OBJECT_AUTHENTICATION",
          //              "QUERY",
          //              "[READ_SCHEMA]",
          //              "OBJECT",
          //              "true",
          //              "__audit",
          //              "SHOW DATABASES",
          //              "User root (ID=0) requests authority on object test with result true"),
          // Alter database
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[ALTER]",
              "OBJECT",
              "true",
              "test",
              "ALTER DATABASE test SET PROPERTIES TTL='INF'",
              "User root (ID=0) requests authority on object test with result true"),
          // Use database TODO: Find out why twice
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[READ_SCHEMA]",
              "OBJECT",
              "true",
              "test",
              "USE test",
              "User root (ID=0) requests authority on object test with result true"),
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[READ_SCHEMA]",
              "OBJECT",
              "true",
              "test",
              "USE test",
              "User root (ID=0) requests authority on object test with result true"),
          // Create table
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SYSTEM]",
              "GLOBAL",
              "true",
              "test",
              "CREATE TABLE table1(t1 STRING TAG, a1 STRING ATTRIBUTE, s1 TEXT FIELD)",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Show table
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[READ_SCHEMA]",
              "OBJECT",
              "true",
              "test",
              "SHOW TABLES",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Desc table
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[READ_SCHEMA]",
              "OBJECT",
              "true",
              "test",
              "DESC table1",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Insert into table
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DML",
              "[INSERT]",
              "OBJECT",
              "true",
              "test",
              "INSERT INTO table1(time, t1, a1, s1) values(1, 't1', 'a1', 's1')",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Delete table TODO: find the delete SQL
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DML",
              "[DELETE]",
              "OBJECT",
              "true",
              "test",
              "null",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Drop database
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[DROP]",
              "OBJECT",
              "true",
              "test",
              "DROP DATABASE IF EXISTS test",
              "User root (ID=0) requests authority on object test with result true"),
          // Select audit log TODO: find out why twice
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[SELECT]",
              "OBJECT",
              "true",
              "__audit",
              "null",
              "User root (ID=0) requests authority on object __audit with result true"),
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[SELECT]",
              "OBJECT",
              "true",
              "__audit",
              "null",
              "User root (ID=0) requests authority on object __audit with result true"));

  @Test
  public void basicAuditLogTestForTableModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : TABLE_MODEL_AUDIT_SQLS) {
        statement.execute(sql);
      }
      int count = 0;
      ResultSet resultSet = statement.executeQuery("SELECT * FROM __audit.audit_log");
      while (resultSet.next()) {
        LOGGER.info("Expected audit log: {}", TABLE_MODEL_AUDIT_FIELDS.get(count));
        List<String> actualFields = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
          actualFields.add(resultSet.getString(i + 1));
        }
        LOGGER.info("Actual audit log: {}", actualFields);
        List<String> expectedFields = TABLE_MODEL_AUDIT_FIELDS.get(count);
        for (int i = 1; i <= 11; i++) {
          Assert.assertEquals(expectedFields.get(i - 1), resultSet.getString(i + 1));
        }
        Assert.assertTrue(resultSet.getString(13).contains(expectedFields.get(11)));
        count++;
      }
      Assert.assertEquals(TABLE_MODEL_AUDIT_FIELDS.size(), count);
    }
  }
}
