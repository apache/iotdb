package com.timecho.iotdb.db.it.audit;

import org.apache.iotdb.db.it.audit.AuditLogSet;
import org.apache.iotdb.isession.SessionConfig;
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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.AUDITABLE_OPERATION_LEVEL;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.AUDITABLE_OPERATION_RESULT;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.AUDITABLE_OPERATION_TYPE;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.AUDIT_TABLE_CATEGORIES;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.AUDIT_TABLE_COLUMNS;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.AUDIT_TABLE_DATA_TYPES;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.ENABLE_AUDIT_LOG;
import static org.apache.iotdb.db.it.audit.IoTDBAuditLogBasicIT.closeConnectionCompletely;

/**
 * This test class ensures the audit log behave exactly the same as we expected when enable
 * separation of powers, including the number, sequence and content of the audit logs.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBAuditLogXSeparationOfPowersIT {

  private static final int SQL_EXECUTE_INTERVAL_IN_MS = 100;
  private static final boolean ENABLE_SEPARATION_OF_POWERS = true;

  @Before
  public void setUp() throws SQLException, InterruptedException {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns") // For preventing audit log conflict in test envs
        .setEnableAuditLog(ENABLE_AUDIT_LOG)
        .setAuditableOperationType(AUDITABLE_OPERATION_TYPE)
        .setAuditableOperationLevel(AUDITABLE_OPERATION_LEVEL)
        .setAuditableOperationResult(AUDITABLE_OPERATION_RESULT);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment();

    // Activate separation of powers
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    statement.execute("set configuration enable_separation_of_powers='true'");
    closeConnectionCompletely(connection);

    // Check audit database and table view
    connection = EnvFactory.getEnv().getAuditAdminConnection(BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SHOW DATABASES root.__audit");
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("root.__audit", resultSet.getString(1));
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getAuditAdminConnection(BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    resultSet = statement.executeQuery("DESC __audit.audit_log");
    resultSet.next(); // Skip time column
    int cnt = 0;
    while (resultSet.next()) {
      Assert.assertEquals(AUDIT_TABLE_COLUMNS.get(cnt), resultSet.getString(1));
      Assert.assertEquals(AUDIT_TABLE_DATA_TYPES.get(cnt), resultSet.getString(2));
      Assert.assertEquals(AUDIT_TABLE_CATEGORIES.get(cnt), resultSet.getString(3));
      cnt++;
    }
    Assert.assertEquals(AUDIT_TABLE_COLUMNS.size(), cnt);
    closeConnectionCompletely(connection);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static final List<String> TABLE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN =
      Arrays.asList(
          "CREATE DATABASE test",
          "USE test",
          //	"SHOW CURRENT_DATABASE",
          "SHOW DATABASES",
          "ALTER DATABASE test SET PROPERTIES TTL='INF'",
          "CREATE TABLE table1(t1 STRING TAG, a1 STRING ATTRIBUTE, s1 STRING FIELD)",
          "SHOW TABLES",
          "DESC table1",
          "ALTER TABLE table1 set properties TTL='INF'");
  private static final List<String> TABLE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN =
      Arrays.asList(
          "CREATE USER user1 'TimechoDB@2021'",
          "CREATE USER user2 'TimechoDB@2021'",
          "CREATE ROLE role1",
          "GRANT SELECT,ALTER,INSERT,DELETE ON TEST.TABLE1 TO USER user1",
          "GRANT SELECT ON ANY TO ROLE role1",
          "GRANT ROLE role1 TO user2",
          "list user",
          "list role");
  private static final List<String> TABLE_MODEL_AUDIT_SQLS_USER_USER1 =
      Arrays.asList(
          "LIST PRIVILEGES OF USER user1",
          "USE test",
          "INSERT INTO table1(time, t1, a1, s1) values(1, 't1', 'a1', 's1')",
          "SELECT * FROM table1",
          "UPDATE table1 SET a1 = t1",
          "DELETE FROM table1");
  private static final List<String> TABLE_MODEL_AUDIT_SQLS_USER_USER2 =
      Arrays.asList(
          "LIST PRIVILEGES OF USER user2",
          "LIST PRIVILEGES OF ROLE role1",
          "list user",
          "list role",
          "USE test",
          "INSERT INTO table1(time, t1, a1, s1) values(1, 't1', 'a1', 's1')",
          "SELECT * FROM table1",
          "UPDATE table1 SET a1 = t1",
          "DELETE FROM table1");
  private static final List<String> TABLE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN_FINAL =
      Arrays.asList("USE test", "DROP TABLE table1", "DROP DATABASE IF EXISTS test");
  private static final List<String> TABLE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN_FINAL =
      Arrays.asList(
          "REVOKE SELECT,ALTER,INSERT,DELETE ON TEST.TABLE1 FROM USER user1",
          "REVOKE SELECT ON ANY FROM ROLE role1",
          "DROP USER user1",
          "DROP USER user2",
          "DROP ROLE role1");
  private static final List<AuditLogSet> TABLE_MODEL_AUDIT_FIELDS =
      Arrays.asList(
          // Start audit service
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_4",
                  "__internal_auditor",
                  "null",
                  "CHANGE_AUDIT_OPTION",
                  "CONTROL",
                  "[AUDIT]",
                  "GLOBAL",
                  "true",
                  "null",
                  "null",
                  "Successfully start the Audit service with configurations (auditableOperationType [DDL, DML, QUERY, CONTROL], auditableOperationLevel GLOBAL, auditableOperationResult SUCCESS,FAIL)")),
          // Environment init login/logout
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_0",
                  "root",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User root (ID=0), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_0",
                  "root",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // Activate separation of powers, TODO: Implement audit for SET CONFIGURATION
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_0",
                  "root",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User root (ID=0), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_0",
                  "root",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User root (ID=0), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_0",
                  "root",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_0",
                  "root",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // Show audit log database
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[AUDIT]",
                  "GLOBAL",
                  "true",
                  "[root.__audit]",
                  "SHOW DATABASES root.__audit",
                  "User audit_admin (ID=3) requests authority on object root.__audit with result true"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[READ_SCHEMA]",
                  "OBJECT",
                  "true",
                  "[root.__audit]",
                  "SHOW DATABASES root.__audit",
                  "User audit_admin (ID=3) requests authority on object [root.__audit] with result true"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "false",
                  "[root.__audit]",
                  "SHOW DATABASES root.__audit",
                  "User audit_admin (ID=3) requests authority on object [root.__audit] with result false"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // Desc audit log table view
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[AUDIT]",
                  "GLOBAL",
                  "true",
                  "__audit",
                  "DESC __audit.audit_log",
                  "User audit_admin (ID=3) requests authority on object audit_log with result true"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_3",
                  "audit_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // =============================Audit user sys_admin=============================
          // sys_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session")),
          // Create database
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[CREATE]",
                  "OBJECT",
                  "true",
                  "test",
                  "CREATE DATABASE test",
                  "User sys_admin (ID=1) requests authority on object test with result true")),
          // Use database, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "USE test",
                  "User sys_admin (ID=1) requests authority on object test with result true"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "USE test",
                  "User sys_admin (ID=1) requests authority on object test with result true")),
          // Show database
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "SHOW DATABASES",
                  "User sys_admin (ID=1) requests authority on object test with result true"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[AUDIT]",
                  "GLOBAL",
                  "false",
                  "__audit",
                  "SHOW DATABASES",
                  "User sys_admin (ID=1) requests authority on object __audit with result false")),
          // Alter database
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[ALTER]",
                  "OBJECT",
                  "true",
                  "test",
                  "ALTER DATABASE test SET PROPERTIES TTL='INF'",
                  "User sys_admin (ID=1) requests authority on object test with result true")),
          // Create table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[CREATE]",
                  "OBJECT",
                  "true",
                  "test",
                  "CREATE TABLE table1(t1 STRING TAG, a1 STRING ATTRIBUTE, s1 STRING FIELD)",
                  "User sys_admin (ID=1) requests authority on object table1 with result true")),
          // Show table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "SHOW TABLES",
                  "User sys_admin (ID=1) requests authority on object table1 with result true")),
          // Desc table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "DESC table1",
                  "User sys_admin (ID=1) requests authority on object table1 with result true")),
          // sys_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // =============================Audit user security_admin=============================
          // security_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session")),
          // Create user
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "CREATE USER user1 ...",
                  "User security_admin (ID=2) requests authority on object user1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "CREATE USER user2 ...",
                  "User security_admin (ID=2) requests authority on object user2 with result true")),
          // Create role
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "CREATE ROLE role1",
                  "User security_admin (ID=2) requests authority on object role1 with result true")),
          // Grant privileges: pri->user, pri->role, role->user
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "test",
                  "GRANT SELECT,ALTER,INSERT,DELETE ON TEST.TABLE1 TO USER user1",
                  "User security_admin (ID=2) requests authority on object user1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "GRANT SELECT ON ANY TO ROLE role1",
                  "User security_admin (ID=2) requests authority on object role1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "GRANT ROLE role1 TO user2",
                  "User security_admin (ID=2) requests authority on object user: user2, role: role1 with result true")),
          // List user/role, the authentication object is null since the security_admin can list all
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "list user",
                  "User security_admin (ID=2) requests authority on object null with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "list role",
                  "User security_admin (ID=2) requests authority on object null with result true")),
          // security_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // =============================Audit user user1=============================
          // User1 login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User user1 (ID=10000), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User user1 (ID=10000), opens Session")),
          // List privileges of user1
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "null",
                  "GLOBAL",
                  "true",
                  "null",
                  "LIST PRIVILEGES OF USER user1",
                  "User user1 (ID=10000) requests authority on object user1 with result true")),
          // Use database, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[ALTER]",
                  "OBJECT",
                  "true",
                  "test",
                  "USE test",
                  "User user1 (ID=10000) requests authority on object test with result true"),
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[ALTER]",
                  "OBJECT",
                  "true",
                  "test",
                  "USE test",
                  "User user1 (ID=10000) requests authority on object test with result true")),
          // Insert into table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DML",
                  "[INSERT]",
                  "OBJECT",
                  "true",
                  "test",
                  "INSERT INTO table1(time, t1, a1, s1) values(...)",
                  "User user1 (ID=10000) requests authority on object table1 with result true")),
          // Select from table, including fetch device
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SELECT]",
                  "OBJECT",
                  "true",
                  "test",
                  "SELECT * FROM table1",
                  "User user1 (ID=10000) requests authority on object table1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SELECT]",
                  "OBJECT",
                  "true",
                  "test",
                  "fetch device for query",
                  "User user1 (ID=10000) requests authority on object table1 with result true")),
          // Update table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DML",
                  "[INSERT]",
                  "OBJECT",
                  "true",
                  "test",
                  "UPDATE table1 SET a1 = t1",
                  "User user1 (ID=10000) requests authority on object table1 with result true")),
          // Delete from table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DML",
                  "[DELETE]",
                  "OBJECT",
                  "true",
                  "test",
                  "DELETE FROM table1",
                  "User user1 (ID=10000) requests authority on object table1 with result true")),
          // User1 logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_10000",
                  "user1",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // =============================Audit user user2=============================
          // User2 login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User user2 (ID=10001), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User user2 (ID=10001), opens Session")),
          // List privileges of user2, role1
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "null",
                  "GLOBAL",
                  "true",
                  "null",
                  "LIST PRIVILEGES OF USER user2",
                  "User user2 (ID=10001) requests authority on object user2 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "null",
                  "GLOBAL",
                  "true",
                  "null",
                  "LIST PRIVILEGES OF ROLE role1",
                  "User user2 (ID=10001) requests authority on object role1 with result true")),
          // List user and role, only him/herself
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "null",
                  "GLOBAL",
                  "true",
                  "null",
                  "list user",
                  "User user2 (ID=10001) requests authority on object user2 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "null",
                  "GLOBAL",
                  "true",
                  "null",
                  "list role",
                  "User user2 (ID=10001) requests authority on object null with result true")),
          // Use database, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SELECT]",
                  "OBJECT",
                  "true",
                  "test",
                  "USE test",
                  "User user2 (ID=10001) requests authority on object test with result true"),
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SELECT]",
                  "OBJECT",
                  "true",
                  "test",
                  "USE test",
                  "User user2 (ID=10001) requests authority on object test with result true")),
          // Failed to insert since no privilege
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DML",
                  "[INSERT]",
                  "OBJECT",
                  "false",
                  "test",
                  "INSERT INTO table1(time, t1, a1, s1) values(...)",
                  "User user2 (ID=10001) requests authority on object table1 with result false")),
          // Select from table1, including fetch device
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SELECT]",
                  "OBJECT",
                  "true",
                  "test",
                  "SELECT * FROM table1",
                  "User user2 (ID=10001) requests authority on object table1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SELECT]",
                  "OBJECT",
                  "true",
                  "test",
                  "fetch device for query",
                  "User user2 (ID=10001) requests authority on object table1 with result true")),
          // Update table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DML",
                  "[INSERT]",
                  "OBJECT",
                  "false",
                  "test",
                  "UPDATE table1 SET a1 = t1",
                  "User user2 (ID=10001) requests authority on object table1 with result false")),
          // Delete from table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DML",
                  "[DELETE]",
                  "OBJECT",
                  "false",
                  "test",
                  "DELETE FROM table1",
                  "User user2 (ID=10001) requests authority on object table1 with result false")),
          // User2 logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_10001",
                  "user2",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // =============================Audit user sys_admin final=============================
          // sys_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session")),
          // Use database, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "USE test",
                  "User sys_admin (ID=1) requests authority on object test with result true"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "QUERY",
                  "[SYSTEM]",
                  "GLOBAL",
                  "true",
                  "test",
                  "USE test",
                  "User sys_admin (ID=1) requests authority on object test with result true")),
          // Drop table
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[DROP]",
                  "OBJECT",
                  "true",
                  "test",
                  "DROP TABLE table1",
                  "User sys_admin (ID=1) requests authority on object table1 with result true")),
          // Drop database
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[DROP]",
                  "OBJECT",
                  "true",
                  "test",
                  "DROP DATABASE IF EXISTS test",
                  "User sys_admin (ID=1) requests authority on object test with result true")),
          // sys_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_1",
                  "sys_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")),
          // =============================Audit user security_admin
          // final=============================
          // security_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGIN",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session")),
          // Revoke user/role privilege
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "test",
                  "REVOKE SELECT,ALTER,INSERT,DELETE ON TEST.TABLE1 FROM USER user1",
                  "User security_admin (ID=2) requests authority on object user1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "REVOKE SELECT ON ANY FROM ROLE role1",
                  "User security_admin (ID=2) requests authority on object role1 with result true")),
          // Drop user, role
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "DROP USER user1",
                  "User security_admin (ID=2) requests authority on object user1 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "DROP USER user2",
                  "User security_admin (ID=2) requests authority on object user2 with result true"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "OBJECT_AUTHENTICATION",
                  "DDL",
                  "[SECURITY]",
                  "GLOBAL",
                  "true",
                  "null",
                  "DROP ROLE role1",
                  "User security_admin (ID=2) requests authority on object role1 with result true")),
          // security_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing"),
              Arrays.asList(
                  "node_1",
                  "u_2",
                  "security_admin",
                  "127.0.0.1",
                  "LOGOUT",
                  "CONTROL",
                  "null",
                  "GLOBAL",
                  "true",
                  "",
                  "",
                  "is closing")));
  // =============================Audit user audit_admin=============================
  // audit_admin login, twice for both read and write connections
  //          new AuditLogSet(
  //              Arrays.asList(
  //                  "node_1",
  //                  "u_3",
  //                  "audit_admin",
  //                  "127.0.0.1",
  //                  "LOGIN",
  //                  "CONTROL",
  //                  "null",
  //                  "GLOBAL",
  //                  "true",
  //                  "",
  //                  "",
  //                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens
  // Session"),
  //              Arrays.asList(
  //                  "node_1",
  //                  "u_3",
  //                  "audit_admin",
  //                  "127.0.0.1",
  //                  "LOGIN",
  //                  "CONTROL",
  //                  "null",
  //                  "GLOBAL",
  //                  "true",
  //                  "",
  //                  "",
  //                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens
  // Session")),
  //          // Select audit log
  //          new AuditLogSet(
  //              Arrays.asList(
  //                  "node_1",
  //                  "u_3",
  //                  "audit_admin",
  //                  "127.0.0.1",
  //                  "OBJECT_AUTHENTICATION",
  //                  "QUERY",
  //                  "[SELECT]",
  //                  "OBJECT",
  //                  "true",
  //                  "__audit",
  //                  "SELECT * FROM __audit.audit_log ORDER BY TIME",
  //                  "User audit_admin (ID=3) requests authority on object __audit with result
  // true"),
  //              Arrays.asList(
  //                  "node_1",
  //                  "u_3",
  //                  "audit_admin",
  //                  "127.0.0.1",
  //                  "OBJECT_AUTHENTICATION",
  //                  "QUERY",
  //                  "[SELECT]",
  //                  "OBJECT",
  //                  "true",
  //                  "__audit",
  //                  "fetch device for query",
  //                  "User audit_admin (ID=3) requests authority on object __audit with result
  // true")));
  private static final Set<Integer> TABLE_INDEX_FOR_CONTAIN =
      Stream.of(11, 12).collect(Collectors.toSet());

  @Test
  public void auditLogXSeparationOfPowersTestForTableModel()
      throws SQLException, InterruptedException {
    Connection connection = EnvFactory.getEnv().getSysAdminConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    for (String sql : TABLE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getSecurityAdminConnection(BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TABLE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    connection =
        EnvFactory.getEnv()
            .getConnection("user1", SessionConfig.DEFAULT_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TABLE_MODEL_AUDIT_SQLS_USER_USER1) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    connection =
        EnvFactory.getEnv()
            .getConnection("user2", SessionConfig.DEFAULT_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TABLE_MODEL_AUDIT_SQLS_USER_USER2) {
      try {
        statement.execute(sql);
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      } catch (SQLException e) {
        // Ignore, only record audit log
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      }
    }
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getSysAdminConnection(BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TABLE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN_FINAL) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getSecurityAdminConnection(BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TABLE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN_FINAL) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    // Wait for audit log to be flushed
    TimeUnit.SECONDS.sleep(10);
    connection = EnvFactory.getEnv().getAuditAdminConnection(BaseEnv.TABLE_SQL_DIALECT);
    statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM __audit.audit_log ORDER BY TIME");
    for (AuditLogSet expectedAuditLogSet : TABLE_MODEL_AUDIT_FIELDS) {
      expectedAuditLogSet.containAuditLog(resultSet, TABLE_INDEX_FOR_CONTAIN, 12);
    }
    //    Assert.assertFalse(resultSet.next());
  }

  private static final List<String> TREE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN =
      Arrays.asList(
          "CREATE DATABASE root.test",
          "show databases",
          "COUNT databases",
          "set ttl to root.test.** INF",
          "create timeseries root.test.d1.s1 with datatype=BOOLEAN",
          "create timeseries root.test.d1.s2 with datatype=INT64",
          "create timeseries root.test.d1.s3 with datatype=TEXT",
          "show timeseries",
          "COUNT TIMESERIES root.test",
          "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3, tag4=v4",
          "ALTER timeseries root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
          "delete from root.test.d1.s3",
          "drop timeseries root.test.d1.*");
  private static final List<String> TREE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN =
      Arrays.asList(
          "CREATE USER user1 'TimechoDB@2021'",
          "CREATE USER user2 'TimechoDB@2021'",
          "CREATE ROLE role1",
          "GRANT READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** TO USER user1",
          "GRANT READ ON root.test.** TO ROLE role1",
          "GRANT ROLE role1 TO user2",
          "list user",
          "list role");
  private static final List<String> TREE_MODEL_AUDIT_SQLS_USER_USER1 =
      Arrays.asList(
          "LIST PRIVILEGES OF USER user1",
          "create timeseries root.test.d1.s1 with datatype=BOOLEAN",
          "create timeseries root.test.d1.s2 with datatype=INT64",
          "create timeseries root.test.d1.s3 with datatype=TEXT",
          "CREATE ALIGNED TIMESERIES root.test.d2(s1 BOOLEAN, s2 INT64, s3 TEXT)",
          "insert into root.test.d1(timestamp,s1,s2,s3) values(1,true,1,'1')",
          "insert into root.test.d2(timestamp,s1,s2,s3) aligned values(1,true,1,'1')",
          "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3, tag4=v4",
          "ALTER timeseries root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
          "select * from root.test.d1 order by time",
          "select * from root.test.d2 order by time",
          "delete from root.test.d1.s3",
          "delete from root.test.d2.s3");
  private static final List<String> TREE_MODEL_AUDIT_SQLS_USER_USER2 =
      Arrays.asList(
          "LIST PRIVILEGES OF USER user2",
          "LIST PRIVILEGES OF ROLE role1",
          "list user",
          "list role",
          "create timeseries root.test.d1.s1 with datatype=BOOLEAN",
          "create timeseries root.test.d1.s2 with datatype=INT64",
          "create timeseries root.test.d1.s3 with datatype=TEXT",
          "CREATE ALIGNED TIMESERIES root.test.d2(s1 BOOLEAN, s2 INT64, s3 TEXT)",
          "insert into root.test.d1(timestamp,s1,s2,s3) values(1,true,1,'1')",
          "insert into root.test.d2(timestamp,s1,s2,s3) aligned values(1,true,1,'1')",
          "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3, tag4=v4",
          "ALTER timeseries root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
          "select * from root.test.d1 order by time",
          "select * from root.test.d2 order by time",
          "delete from root.test.d1.s3",
          "delete from root.test.d2.s3");
  private static final List<String> TREE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN_FINAL =
      Arrays.asList("DELETE DATABASE root.test");
  private static final List<String> TREE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN_FINAL =
      Arrays.asList(
          "REVOKE READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** FROM USER user1",
          "REVOKE READ ON root.test.** FROM ROLE role1",
          "DROP USER user1",
          "DROP USER user2",
          "DROP ROLE role1");
  private static final List<AuditLogSet> TREE_MODEL_AUDIT_FIELDS =
      Arrays.asList(
          // Start audit service
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_4",
                  "true",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "CONTROL",
                  "Successfully start the Audit service with configurations (auditableOperationType [DDL, DML, QUERY, CONTROL], auditableOperationLevel GLOBAL, auditableOperationResult SUCCESS,FAIL)",
                  "null",
                  "CHANGE_AUDIT_OPTION",
                  "null",
                  "__internal_auditor")),
          // Environment setup login/logout
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_0",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User root (ID=0), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "root"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_0",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "root")),
          // Activate separation of powers, TODO: Audit set configuration
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_0",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User root (ID=0), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "root"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_0",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User root (ID=0), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "root"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_0",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "root"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_0",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "root")),
          // Show audit log database
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "[AUDIT]",
                  "[root.__audit]",
                  "QUERY",
                  "User audit_admin (ID=3) requests authority on object root.__audit with result true",
                  "SHOW DATABASES root.__audit",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "OBJECT",
                  "[READ_SCHEMA]",
                  "[root.__audit]",
                  "QUERY",
                  "User audit_admin (ID=3) requests authority on object [root.__audit] with result true",
                  "SHOW DATABASES root.__audit",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "false",
                  "GLOBAL",
                  "[SYSTEM]",
                  "[root.__audit]",
                  "QUERY",
                  "User audit_admin (ID=3) requests authority on object [root.__audit] with result false",
                  "SHOW DATABASES root.__audit",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "audit_admin")),
          // Desc audit log table view
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "[AUDIT]",
                  "__audit",
                  "QUERY",
                  "User audit_admin (ID=3) requests authority on object audit_log with result true",
                  "DESC __audit.audit_log",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "audit_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_3",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "audit_admin")),
          // =============================Audit user sys_admin=============================
          // sys_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "sys_admin")),
          // Create database
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "[SYSTEM]",
                  "root.test",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object root.test with result true",
                  "CREATE DATABASE root.test",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // Show database
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "[SYSTEM]",
                  "[root.**]",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object [root.**] with result true",
                  "show databases",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "OBJECT",
                  "[MANAGE_DATABASE]",
                  "[root.**]",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object [root.**] with result true",
                  "show databases",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "[root.**]",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object root.__audit with result false",
                  "show databases",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // Count database
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "[root.**]",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object root.__audit with result false",
                  "COUNT databases",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "[SYSTEM]",
                  "[root.**]",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object [root.**] with result true",
                  "COUNT databases",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "OBJECT",
                  "[MANAGE_DATABASE]",
                  "[root.**]",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object [root.**] with result true",
                  "COUNT databases",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // Set TTL to database
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "[SYSTEM]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.**] with result true",
                  "set ttl to root.test.** INF",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // sys_admin cannot create (aligned) timeseries
          // TODO: fill database if necessary, same as follows
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.d1.s1] with result false",
                  "create timeseries root.test.d1.s1 with datatype=BOOLEAN",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.d1.s2] with result false",
                  "create timeseries root.test.d1.s2 with datatype=INT64",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.d1.s3] with result false",
                  "create timeseries root.test.d1.s3 with datatype=TEXT",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // Show timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object root.__audit with result false",
                  "show timeseries",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "OBJECT",
                  "[READ_SCHEMA]",
                  "null",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object [root.**] with result true",
                  "show timeseries",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // Count timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object root.__audit with result false",
                  "COUNT TIMESERIES root.test",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "OBJECT",
                  "[READ_SCHEMA]",
                  "null",
                  "QUERY",
                  "User sys_admin (ID=1) requests authority on object [root.test] with result true",
                  "COUNT TIMESERIES root.test",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // sys_admin cannot alter timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.d1.s1] with result false",
                  "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3, tag4=v4",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.d2.s1] with result false",
                  "ALTER timeseries root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // sys_admin cannot delete timeseries data
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User sys_admin (ID=1) requests authority on object [root.test.d1.s3] with result false",
                  "delete from root.test.d1.s3",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // sys_admin cannot drop timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test.d1.*] with result false",
                  "drop timeseries root.test.d1.*",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // sys_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "sys_admin")),
          // =============================Audit user security_admin=============================
          // security_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "security_admin")),
          // Create user/role
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "CREATE USER user1 ...",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user2 with result true",
                  "CREATE USER user2 ...",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object role1 with result true",
                  "CREATE ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin")),
          // Grant privileges: pri->user, pri->role, role->user
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "GRANT READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** TO USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "GRANT READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** TO USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "GRANT READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** TO USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "GRANT READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** TO USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object role1 with result true",
                  "GRANT READ ON root.test.** TO ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object role1 with result true",
                  "GRANT READ ON root.test.** TO ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user: user2, role: role1 with result true",
                  "GRANT ROLE role1 TO user2",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin")),
          // List user/role, the target object is null since the root can list all,
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "QUERY",
                  "User security_admin (ID=2) requests authority on object null with result true",
                  "list user",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "QUERY",
                  "User security_admin (ID=2) requests authority on object null with result true",
                  "list role",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin")),
          // security_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "security_admin")),
          // =============================Audit user user1=============================
          // User1 login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User user1 (ID=10000), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User user1 (ID=10000), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "user1")),
          // List privilege of user1
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "GLOBAL",
                  "null",
                  "null",
                  "QUERY",
                  "User user1 (ID=10000) requests authority on object user1 with result true",
                  "LIST PRIVILEGES OF USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1")),
          // Create (aligned) timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.s1] with result true",
                  "create timeseries root.test.d1.s1 with datatype=BOOLEAN",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.s2] with result true",
                  "create timeseries root.test.d1.s2 with datatype=INT64",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.s3] with result true",
                  "create timeseries root.test.d1.s3 with datatype=TEXT",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user1 (ID=10000) requests authority on object [root.test.d2.s1, root.test.d2.s2, root.test.d2.s3] with result true",
                  "CREATE ALIGNED TIMESERIES root.test.d2(s1 BOOLEAN, s2 INT64, s3 TEXT)",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1")),
          // Insert into (aligned) timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.s1, root.test.d1.s2, root.test.d1.s3] with result true",
                  "insert into root.test.d1(timestamp,s1,s2,s3) values(...)",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user1 (ID=10000) requests authority on object [root.test.d2.s1, root.test.d2.s2, root.test.d2.s3] with result true",
                  "insert into root.test.d2(timestamp,s1,s2,s3) aligned values(...)",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1")),
          // Alter timeseries
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.s1] with result true",
                  "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3, tag4=v4",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user1 (ID=10000) requests authority on object [root.test.d2.s1] with result true",
                  "ALTER timeseries root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1")),
          // Select timeseries data
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "QUERY",
                  "User user1 (ID=10000) requests authority on object root.__audit with result false",
                  "select * from root.test.d1 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[READ_DATA]",
                  "null",
                  "QUERY",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.*] with result true",
                  "select * from root.test.d1 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "QUERY",
                  "User user1 (ID=10000) requests authority on object root.__audit with result false",
                  "select * from root.test.d2 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[READ_DATA]",
                  "null",
                  "QUERY",
                  "User user1 (ID=10000) requests authority on object [root.test.d2.*] with result true",
                  "select * from root.test.d2 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1")),
          // Delete timeseries data
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user1 (ID=10000) requests authority on object [root.test.d1.s3] with result true",
                  "delete from root.test.d1.s3",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user1 (ID=10000) requests authority on object [root.test.d2.s3] with result true",
                  "delete from root.test.d2.s3",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user1")),
          // user1 logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "user1"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10000",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "user1")),
          // =============================Audit user user2=============================
          // User2 login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User user2 (ID=10001), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User user2 (ID=10001), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "user2")),
          // List privilege of user2/role1
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object user2 with result true",
                  "LIST PRIVILEGES OF USER user2",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object role1 with result true",
                  "LIST PRIVILEGES OF ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // List user/role, can only see him/herself
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object user2 with result true",
                  "list user",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              // List role, can only see his/hers roles
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object null with result false",
                  "list role",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "[null]",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object user2 with result true",
                  "list role",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // Create (aligned) timeseries failed
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.s1] with result false",
                  "create timeseries root.test.d1.s1 with datatype=BOOLEAN",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.s2] with result false",
                  "create timeseries root.test.d1.s2 with datatype=INT64",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.s3] with result false",
                  "create timeseries root.test.d1.s3 with datatype=TEXT",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user2 (ID=10001) requests authority on object [root.test.d2.s1, root.test.d2.s2, root.test.d2.s3] with result false",
                  "CREATE ALIGNED TIMESERIES root.test.d2(s1 BOOLEAN, s2 INT64, s3 TEXT)",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // Insert into (aligned) timeseries failed
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.s1, root.test.d1.s2, root.test.d1.s3] with result false",
                  "insert into root.test.d1(timestamp,s1,s2,s3) values(...)",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user2 (ID=10001) requests authority on object [root.test.d2.s1, root.test.d2.s2, root.test.d2.s3] with result false",
                  "insert into root.test.d2(timestamp,s1,s2,s3) aligned values(...)",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // Alter timeseries failed
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.s1] with result false",
                  "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3, tag4=v4",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_SCHEMA]",
                  "null",
                  "DDL",
                  "User user2 (ID=10001) requests authority on object [root.test.d2.s1] with result false",
                  "ALTER timeseries root.test.d2.s1 ADD TAGS tag3=v3, tag4=v4",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // Select timeseries data
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object root.__audit with result false",
                  "select * from root.test.d1 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "OBJECT",
                  "[READ_DATA]",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.*] with result true",
                  "select * from root.test.d1 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "GLOBAL",
                  "[AUDIT]",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object root.__audit with result false",
                  "select * from root.test.d2 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "OBJECT",
                  "[READ_DATA]",
                  "null",
                  "QUERY",
                  "User user2 (ID=10001) requests authority on object [root.test.d2.*] with result true",
                  "select * from root.test.d2 order by time",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // Delete timeseries data failed
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user2 (ID=10001) requests authority on object [root.test.d1.s3] with result false",
                  "delete from root.test.d1.s3",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "false",
                  "OBJECT",
                  "[WRITE_DATA]",
                  "null",
                  "DML",
                  "User user2 (ID=10001) requests authority on object [root.test.d2.s3] with result false",
                  "delete from root.test.d2.s3",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "user2")),
          // user2 logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "user2"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_10001",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "user2")),
          // =============================Audit user sys_admin final=============================
          // sys_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User sys_admin (ID=1), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "sys_admin")),
          // Delete database
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "[SYSTEM]",
                  "[root.test]",
                  "DDL",
                  "User sys_admin (ID=1) requests authority on object [root.test] with result true",
                  "DELETE DATABASE root.test",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "sys_admin")),
          // sys_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "sys_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_1",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "sys_admin")),
          // =============================Audit user security_admin
          // final=============================
          // security_admin login, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "IoTDB: Login status: Login successfully. User security_admin (ID=2), opens Session",
                  "",
                  "LOGIN",
                  "127.0.0.1",
                  "security_admin")),
          // Revoke privileges
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "REVOKE READ_SCHEMA, WRITE_SCHEMA, READ_DATA, WRITE_DATA ON root.test.** FROM USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object role1 with result true",
                  "REVOKE READ ON root.test.** FROM ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object role1 with result true",
                  "REVOKE READ ON root.test.** FROM ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin")),
          // Drop user/role
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user1 with result true",
                  "DROP USER user1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object user2 with result true",
                  "DROP USER user2",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "[SECURITY]",
                  "null",
                  "DDL",
                  "User security_admin (ID=2) requests authority on object role1 with result true",
                  "DROP ROLE role1",
                  "OBJECT_AUTHENTICATION",
                  "127.0.0.1",
                  "security_admin")),
          // security_admin logout, twice for both read and write connections
          new AuditLogSet(
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "security_admin"),
              Arrays.asList(
                  "root.__audit.log.node_1.u_2",
                  "true",
                  "GLOBAL",
                  "null",
                  "",
                  "CONTROL",
                  "is closing",
                  "",
                  "LOGOUT",
                  "127.0.0.1",
                  "security_admin")));
  // =============================Audit user audit_admin=============================
  // audit_admin login, twice for both read and write connections
  //          new AuditLogSet(
  //              Arrays.asList(
  //                  "root.__audit.log.node_1.u_3",
  //                  "true",
  //                  "GLOBAL",
  //                  "null",
  //                  "",
  //                  "CONTROL",
  //                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens
  // Session",
  //                  "",
  //                  "LOGIN",
  //                  "127.0.0.1",
  //                  "audit_admin"),
  //              Arrays.asList(
  //                  "root.__audit.log.node_1.u_3",
  //                  "true",
  //                  "GLOBAL",
  //                  "null",
  //                  "",
  //                  "CONTROL",
  //                  "IoTDB: Login status: Login successfully. User audit_admin (ID=3), opens
  // Session",
  //                  "",
  //                  "LOGIN",
  //                  "127.0.0.1",
  //                  "audit_admin")),
  //          // Select audit log
  //          new AuditLogSet(
  //              Arrays.asList(
  //                  "root.__audit.log.node_1.u_3",
  //                  "true",
  //                  "GLOBAL",
  //                  "[AUDIT]",
  //                  "null",
  //                  "QUERY",
  //                  "User audit_admin (ID=3) requests authority on object root.__audit with result
  // true",
  //                  "SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE",
  //                  "OBJECT_AUTHENTICATION",
  //                  "127.0.0.1",
  //                  "audit_admin"),
  //              Arrays.asList(
  //                  "root.__audit.log.node_1.u_3",
  //                  "true",
  //                  "OBJECT",
  //                  "[READ_DATA]",
  //                  "null",
  //                  "QUERY",
  //                  "User audit_admin (ID=3) requests authority on object [root.__audit.log.**.*]
  // with result true",
  //                  "SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE",
  //                  "OBJECT_AUTHENTICATION",
  //                  "127.0.0.1",
  //                  "audit_admin")));
  private static final Set<Integer> TREE_INDEX_FOR_CONTAIN =
      Stream.of(7).collect(Collectors.toSet());

  @Test
  public void auditLogXSeparationOfPowersTestForTreeModel()
      throws SQLException, InterruptedException {
    Connection connection = EnvFactory.getEnv().getSysAdminConnection(BaseEnv.TREE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    for (String sql : TREE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN) {
      try {
        statement.execute(sql);
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      } catch (SQLException e) {
        // Ignore, only record audit log
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      }
    }
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getSecurityAdminConnection(BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TREE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    connection =
        EnvFactory.getEnv()
            .getConnection("user1", SessionConfig.DEFAULT_PASSWORD, BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TREE_MODEL_AUDIT_SQLS_USER_USER1) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    connection =
        EnvFactory.getEnv()
            .getConnection("user2", SessionConfig.DEFAULT_PASSWORD, BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TREE_MODEL_AUDIT_SQLS_USER_USER2) {
      try {
        statement.execute(sql);
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      } catch (SQLException e) {
        // Ignore, only record audit log
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      }
    }
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getSysAdminConnection(BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TREE_MODEL_AUDIT_SQLS_USER_SYS_ADMIN_FINAL) {
      try {
        statement.execute(sql);
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      } catch (SQLException e) {
        // Ignore, only record audit log
        TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
      }
    }
    closeConnectionCompletely(connection);
    connection = EnvFactory.getEnv().getSecurityAdminConnection(BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    for (String sql : TREE_MODEL_AUDIT_SQLS_USER_SECURITY_ADMIN_FINAL) {
      statement.execute(sql);
      TimeUnit.MILLISECONDS.sleep(SQL_EXECUTE_INTERVAL_IN_MS);
    }
    closeConnectionCompletely(connection);
    // Wait for audit log to be flushed
    TimeUnit.SECONDS.sleep(10);
    connection = EnvFactory.getEnv().getAuditAdminConnection(BaseEnv.TREE_SQL_DIALECT);
    statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE");
    for (AuditLogSet expectedAuditLogSet : TREE_MODEL_AUDIT_FIELDS) {
      expectedAuditLogSet.containAuditLog(resultSet, TREE_INDEX_FOR_CONTAIN, 11);
    }
    //    Assert.assertFalse(resultSet.next());
  }
}
