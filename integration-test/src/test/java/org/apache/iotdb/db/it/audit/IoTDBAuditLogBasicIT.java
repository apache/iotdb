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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This test class ensures the audit log behave exactly the same as we expected, including the
 * number, sequence and content of the audit logs.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAuditLogBasicIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAuditLogBasicIT.class);
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

  @Before
  public void setUp() throws SQLException {
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

  @After
  public void tearDown() {
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
          "CREATE USER user1 'IoTDB@2021abc'",
          "CREATE role role1",
          "GRANT SELECT, ALTER, INSERT, DELETE ON test.table1 TO ROLE role1",
          "GRANT ROLE role1 TO user1",
          "LIST USER",
          "LIST ROLE",
          "DROP ROLE role1",
          "DROP USER user1",
          "INSERT INTO table1(time, t1, a1, s1) values(1, 't1', 'a1', 's1')",
          "SELECT * FROM table1",
          "DELETE FROM table1",
          "DROP TABLE table1",
          "DROP DATABASE IF EXISTS test");
  private static final List<List<String>> TABLE_MODEL_AUDIT_FIELDS =
      Arrays.asList(
          // Start audit service
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
          // Show audit database
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[MANAGE_DATABASE]",
              "OBJECT",
              "true",
              "[root.__audit]",
              "SHOW DATABASES root.__audit",
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
          // Use database, twice for both read and write connections
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
              "[CREATE]",
              "OBJECT",
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
          // Create user
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "CREATE USER user1 ...",
              "User root (ID=0) requests authority on object user1 with result true"),
          // Create role
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "CREATE role role1",
              "User root (ID=0) requests authority on object role1 with result true"),
          // Grant privileges to role
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "test",
              "GRANT SELECT, ALTER, INSERT, DELETE ON test.table1 TO ROLE role1",
              "User root (ID=0) requests authority on object role1 with result true"),
          // Grant role to user
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "GRANT ROLE role1 TO user1",
              "User root (ID=0) requests authority on object user: user1, role: role1 with result true"),
          // List user TODO: whether to include user object?
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "LIST USER",
              "User root (ID=0) requests authority on object null with result true"),
          // List role TODO: whether to include role object?
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "QUERY",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "LIST ROLE",
              "User root (ID=0) requests authority on object null with result true"),
          // Drop role
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "DROP ROLE role1",
              "User root (ID=0) requests authority on object role1 with result true"),
          // Drop user
          Arrays.asList(
              "node_1",
              "u_0",
              "root",
              "127.0.0.1",
              "OBJECT_AUTHENTICATION",
              "DDL",
              "[SECURITY]",
              "GLOBAL",
              "true",
              "null",
              "DROP USER user1",
              "User root (ID=0) requests authority on object user1 with result true"),
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
              "INSERT INTO table1(time, t1, a1, s1) values(...)",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Select from table, including fetch device
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
              "test",
              "SELECT * FROM table1",
              "User root (ID=0) requests authority on object table1 with result true"),
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
              "test",
              "fetch device for query",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Delete table
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
              "DELETE FROM table1",
              "User root (ID=0) requests authority on object table1 with result true"),
          // Drop table
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
              "DROP TABLE table1",
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
          // Select audit log
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
              "SELECT * FROM __audit.audit_log ORDER BY TIME",
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
              "fetch device for query",
              "User root (ID=0) requests authority on object __audit with result true"));
  private static final Set<Integer> TABLE_INDEX_FOR_CONTAIN =
      Stream.of(11, 12).collect(Collectors.toSet());

  @Test
  public void basicAuditLogTestForTableModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : TABLE_MODEL_AUDIT_SQLS) {
        statement.execute(sql);
      }
      int count = 0;
      ResultSet resultSet = statement.executeQuery("SELECT * FROM __audit.audit_log ORDER BY TIME");
      while (resultSet.next()) {
        LOGGER.info("Expected audit log: {}", TABLE_MODEL_AUDIT_FIELDS.get(count));
        List<String> actualFields = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
          actualFields.add(resultSet.getString(i + 1));
        }
        LOGGER.info("Actual audit log: {}", actualFields);
        List<String> expectedFields = TABLE_MODEL_AUDIT_FIELDS.get(count);
        for (int i = 1; i <= 12; i++) {
          if (TABLE_INDEX_FOR_CONTAIN.contains(i)) {
            Assert.assertTrue(resultSet.getString(i + 1).contains(expectedFields.get(i - 1)));
            continue;
          }
          Assert.assertEquals(expectedFields.get(i - 1), resultSet.getString(i + 1));
        }

        count++;
      }
      Assert.assertEquals(TABLE_MODEL_AUDIT_FIELDS.size(), count);
    }
  }

  private static final List<String> TREE_MODEL_AUDIT_SQLS =
      Arrays.asList(
          "CREATE DATABASE root.test",
          "CREATE TIMESERIES root.test.d1.s2 WITH DATATYPE=INT64",
          "CREATE ALIGNED TIMESERIES root.test.d2(s1 BOOLEAN, s2 INT64)",
          "ALTER TIMESERIES root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
          "CREATE USER user1 'IoTDB@2021abc'",
          "CREATE role role1",
          "GRANT READ_DATA, WRITE_DATA ON root.test TO ROLE role1",
          "GRANT ROLE role1 TO user1",
          "LIST USER",
          "LIST ROLE",
          "DROP ROLE role1",
          "DROP USER user1",
          "INSERT INTO root.test.d1(timestamp,s2) VALUES(1,1)",
          "INSERT INTO root.test.d2(timestamp,s1,s2) ALIGNED VALUES(1,true,1)",
          "SELECT ** FROM root.test",
          "DELETE FROM root.test.d2",
          "DROP TIMESERIES root.test.d1.s2",
          "set ttl to root.test.** 360000",
          "DELETE DATABASE root.test");
  private static final List<List<String>> TREE_MODEL_AUDIT_FIELDS =
      Arrays.asList(
          // Start audit service
          Arrays.asList(
              "root.__audit.log.node_1.u_none",
              "true",
              "GLOBAL",
              "[AUDIT]",
              "null",
              "CONTROL",
              "Successfully start the Audit service with configurations (auditableOperationType [DDL, DML, QUERY, CONTROL], auditableOperationLevel GLOBAL, auditableOperationResult SUCCESS,FAIL)",
              "null",
              "CHANGE_AUDIT_OPTION",
              "null",
              "null"),
          // Show audit database
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[MANAGE_DATABASE]",
              "[root.__audit]",
              "QUERY",
              "User root (ID=0) requests authority on object root.__audit with result true",
              "SHOW DATABASES root.__audit",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Desc audit table
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[READ_SCHEMA]",
              "__audit",
              "QUERY",
              "User root (ID=0) requests authority on object audit_log with result true",
              "DESC __audit.audit_log",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Create database
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[MANAGE_DATABASE]",
              "root.test",
              "DDL",
              "User root (ID=0) requests authority on object root.test with result true",
              "CREATE DATABASE root.test",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Create (aligned) timeseries TODO: fill database if necessary, same as follows
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_SCHEMA]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object [root.test.d1.s2] with result true",
              "CREATE TIMESERIES root.test.d1.s2 WITH DATATYPE=INT64",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_SCHEMA]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object [root.test.d2.s1, root.test.d2.s2] with result true",
              "CREATE ALIGNED TIMESERIES root.test.d2(s1 BOOLEAN, s2 INT64)",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Alter timeseries
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_SCHEMA]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object [root.test.d2.s1] with result true",
              "ALTER TIMESERIES root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Create user
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_USER]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object user1 with result true",
              "CREATE USER user1 ...",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Create role
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_ROLE]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object role1 with result true",
              "CREATE role role1",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Grant privileges to role
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[SECURITY]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object role1 with result true",
              "GRANT READ_DATA, WRITE_DATA ON root.test TO ROLE role1",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Grant role to user
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_ROLE]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object user: user1, role: role1 with result true",
              "GRANT ROLE role1 TO user1",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // List user TODO: whether to include user object?
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_USER]",
              "null",
              "QUERY",
              "User root (ID=0) requests authority on object null with result true",
              "LIST USER",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // List role TODO: whether to include role object?
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_ROLE]",
              "null",
              "QUERY",
              "User root (ID=0) requests authority on object null with result true",
              "LIST ROLE",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Drop role
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_ROLE]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object role1 with result true",
              "DROP ROLE role1",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Drop user
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[MANAGE_USER]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object user1 with result true",
              "DROP USER user1",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Insert into (aligned) timeseries
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_DATA]",
              "null",
              "DML",
              "User root (ID=0) requests authority on object [root.test.d1.s2] with result true",
              "INSERT INTO root.test.d1(timestamp,s2) VALUES(...)",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_DATA]",
              "null",
              "DML",
              "User root (ID=0) requests authority on object [root.test.d2.s1, root.test.d2.s2] with result true",
              "INSERT INTO root.test.d2(timestamp,s1,s2) ALIGNED VALUES(...)",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Select all timeseries
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[READ_DATA]",
              "null",
              "QUERY",
              "User root (ID=0) requests authority on object [root.test.**] with result true",
              "SELECT ** FROM root.test",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Delete timeseries data
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_DATA]",
              "null",
              "DML",
              "User root (ID=0) requests authority on object [root.test.d2] with result true",
              "DELETE FROM root.test.d2",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Drop timeseries
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[WRITE_SCHEMA]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object [root.test.d1.s2] with result true",
              "DROP TIMESERIES root.test.d1.s2",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Set TTL to devices
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "GLOBAL",
              "[SYSTEM]",
              "null",
              "DDL",
              "User root (ID=0) requests authority on object [root.test.**] with result true",
              "set ttl to root.test.** 360000",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Delete database
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[MANAGE_DATABASE]",
              "[root.test]",
              "DDL",
              "User root (ID=0) requests authority on object [root.test] with result true",
              "DELETE DATABASE root.test",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"),
          // Select audit log
          Arrays.asList(
              "root.__audit.log.node_1.u_0",
              "true",
              "OBJECT",
              "[READ_DATA]",
              "null",
              "QUERY",
              "User root (ID=0) requests authority on object [root.__audit.log.**.*] with result true",
              "SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE",
              "OBJECT_AUTHENTICATION",
              "127.0.0.1",
              "root"));
  private static final Set<Integer> TREE_INDEX_FOR_CONTAIN =
      Stream.of(7).collect(Collectors.toSet());

  @Test
  public void basicAuditLogTestForTreeModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : TREE_MODEL_AUDIT_SQLS) {
        statement.execute(sql);
      }
      int count = 0;
      ResultSet resultSet =
          statement.executeQuery("SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE");
      while (resultSet.next()) {
        LOGGER.info("Expected audit log: {}", TREE_MODEL_AUDIT_FIELDS.get(count));
        List<String> actualFields = new ArrayList<>();
        for (int i = 1; i <= 11; i++) {
          actualFields.add(resultSet.getString(i + 1));
        }
        LOGGER.info("Actual audit log: {}", actualFields);
        List<String> expectedFields = TREE_MODEL_AUDIT_FIELDS.get(count);
        for (int i = 1; i <= 11; i++) {
          if (TREE_INDEX_FOR_CONTAIN.contains(i)) {
            Assert.assertTrue(resultSet.getString(i + 1).contains(expectedFields.get(i - 1)));
            continue;
          }
          Assert.assertEquals(expectedFields.get(i - 1), resultSet.getString(i + 1));
        }

        count++;
      }
      Assert.assertEquals(TREE_MODEL_AUDIT_FIELDS.size(), count);
    }
  }
}
