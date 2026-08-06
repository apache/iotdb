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

package org.apache.iotdb.relational.it.schema;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBTableAuthHAIT {

  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBTableAuthHAIT.class);

  private static final String DATABASE_NAME = "test_auth_db";
  private static final String TABLE_NAME = "test_tb";
  private static final String TREE_DB_NAME = "root.test_auth_tree";

  private static final String TEST_USER = "test_user";
  private static final String TEST_USER_INITIAL_PWD = "Test_user@336699!";
  private static final String TEST_USER_NEW_PWD = "New_pass@2024Pwd!";

  private static final String HA_USER = "ha_user";
  private static final String HA_USER_PWD = "Ha_user@123456!";

  private static final String RENAMED_USER = "renamed_user";
  private static final String TEST_ROLE = "test_role";
  private static final String HA_ROLE = "ha_role";

  private static void initCluster() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(20000);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  private static void cleanCluster() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void preTestData(
      final Statement rootStmt, final String databaseName, final String tableName)
      throws SQLException {
    // Table model setup
    rootStmt.execute("CREATE DATABASE " + databaseName);
    rootStmt.execute("USE " + databaseName);
    rootStmt.execute("CREATE TABLE " + tableName + " (dev STRING TAG, s1 INT32 FIELD)");
    rootStmt.execute("INSERT INTO " + tableName + "(time, dev, s1) VALUES(1, 'dev1', 100)");

    // Tree model setup
    rootStmt.execute("SET SQL_DIALECT=tree");
    rootStmt.execute("CREATE TIMESERIES " + TREE_DB_NAME + ".dev1.s1 WITH DATATYPE=INT32");
    rootStmt.execute("INSERT INTO " + TREE_DB_NAME + ".dev1(time, s1) VALUES(1, 100)");
    rootStmt.execute("SET SQL_DIALECT=table");

    // Create user and role
    rootStmt.execute("CREATE USER " + TEST_USER + " '" + TEST_USER_INITIAL_PWD + "'");
    rootStmt.execute("CREATE ROLE " + TEST_ROLE);
  }

  @Test
  public void testAuthHAWithOneDataNodeDown() throws Exception {
    initCluster();
    try {
      final DataNodeWrapper liveDN0 = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper liveDN1 = EnvFactory.getEnv().getDataNodeWrapper(1);
      final DataNodeWrapper victimDN2 = EnvFactory.getEnv().getDataNodeWrapper(2);

      // Prepare data (all 3 DNs alive)
      try (final Connection rootConn =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDN0,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          final Statement rootStmt = rootConn.createStatement()) {
        preTestData(rootStmt, DATABASE_NAME, TABLE_NAME);
      }

      // Take one DataNode down
      victimDN2.stop();
      Assert.assertFalse("victim DataNode should be stopped", victimDN2.isAlive());

      // Execute all HA tests via live DN-0, verify effects via DN-1
      try (final Connection rootConn =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDN0,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          final Statement rootStmt = rootConn.createStatement()) {

        executeUserManagementHA(rootStmt, liveDN0, liveDN1);
        executeRoleManagementHA(rootStmt);
        executeTablePermissionHA(rootStmt, liveDN0, liveDN1);
        executeTableRoleBasedPermissionHA(rootStmt, liveDN1);
        executeTreePermissionHA(rootStmt, liveDN0, liveDN1);
        executeTreeRoleBasedPermissionHA(rootStmt, liveDN1);
        executeCleanup(rootStmt, liveDN1);
      }
    } finally {
      cleanCluster();
    }
  }

  // ==================== User Management ====================

  private void executeUserManagementHA(
      final Statement rootStmt, final DataNodeWrapper liveDN0, final DataNodeWrapper liveDN1)
      throws Exception {

    // Step 1: CREATE USER
    LOGGER.info("1. start to test high availability of CREATE USER");
    assertStatementEffect(
        rootStmt,
        "CREATE USER " + HA_USER + " '" + HA_USER_PWD + "'",
        () -> userExists(rootStmt, HA_USER),
        "CREATE USER must succeed");

    // Step 2: ALTER USER SET PASSWORD
    LOGGER.info("2. start to test high availability of ALTER USER SET PASSWORD");
    rootStmt.execute("ALTER USER " + TEST_USER + " SET PASSWORD '" + TEST_USER_NEW_PWD + "'");
    // Verify: old password fails, new password succeeds on DN-1
    assertConnectionFails(
        liveDN1,
        TEST_USER,
        TEST_USER_INITIAL_PWD,
        BaseEnv.TABLE_SQL_DIALECT,
        "old password should fail after password change");
    try (Connection newConn =
            EnvFactory.getEnv()
                .getConnection(liveDN1, TEST_USER, TEST_USER_NEW_PWD, BaseEnv.TABLE_SQL_DIALECT);
        Statement s = newConn.createStatement()) {
      s.executeQuery("LIST USER");
    }

    // Step 3: ALTER USER RENAME TO
    LOGGER.info("3. start to test high availability of ALTER USER RENAME TO");
    rootStmt.execute("ALTER USER " + TEST_USER + " RENAME TO " + RENAMED_USER);
    assertTrue("old user should not exist", !userExists(rootStmt, TEST_USER));
    assertTrue("new user should exist", userExists(rootStmt, RENAMED_USER));
    // Verify: old name fails, new name succeeds on DN-1
    assertConnectionFails(
        liveDN1,
        TEST_USER,
        TEST_USER_NEW_PWD,
        BaseEnv.TABLE_SQL_DIALECT,
        "old user name should fail after rename");
    try (Connection renamedConn =
            EnvFactory.getEnv()
                .getConnection(
                    liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, BaseEnv.TABLE_SQL_DIALECT);
        Statement s = renamedConn.createStatement()) {
      s.executeQuery("LIST USER");
    }

    // Step 4: LIST USER
    LOGGER.info("4. start to test high availability of LIST USER");
    assertTrue("LIST USER should return results", listHasRows(rootStmt, "LIST USER"));
  }

  // ==================== Role Management ====================

  private void executeRoleManagementHA(final Statement rootStmt) throws Exception {

    // Step 6: CREATE ROLE
    LOGGER.info("5. start to test high availability of CREATE ROLE");
    assertStatementEffect(
        rootStmt,
        "CREATE ROLE " + HA_ROLE,
        () -> roleExists(rootStmt, HA_ROLE),
        "CREATE ROLE must succeed");

    // Grant ha_role to renamed_user, then verify
    rootStmt.execute("GRANT ROLE " + HA_ROLE + " TO " + RENAMED_USER);

    // Step 6: LIST ROLE OF USER renamed_user
    LOGGER.info("6. start to test high availability of LIST ROLE OF USER");
    assertTrue(
        "LIST ROLE OF USER renamed_user should contain ha_role",
        userHasRole(rootStmt, RENAMED_USER, HA_ROLE));
  }

  // ==================== Table Model Permission Management ====================

  private void executeTablePermissionHA(
      final Statement rootStmt, final DataNodeWrapper liveDN0, final DataNodeWrapper liveDN1)
      throws Exception {

    final String testTable = DATABASE_NAME + "." + TABLE_NAME;

    // Step 8: GRANT ROLE to user
    LOGGER.info("7. start to test high availability of GRANT ROLE (table model)");
    assertStatementEffect(
        rootStmt,
        "GRANT ROLE " + TEST_ROLE + " TO " + RENAMED_USER,
        () -> userHasRole(rootStmt, RENAMED_USER, TEST_ROLE),
        "GRANT ROLE must succeed");

    // Step 9: GRANT SELECT ON TABLE
    LOGGER.info("8. start to test high availability of GRANT SELECT ON TABLE");
    assertStatementEffect(
        rootStmt,
        "GRANT SELECT ON TABLE " + testTable + " TO USER " + RENAMED_USER,
        () -> userHasPrivilege(rootStmt, RENAMED_USER, "SELECT"),
        "GRANT SELECT ON TABLE must succeed");
    // Verify: SELECT succeeds on DN-1
    tableUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, true);

    // Step 10: Verify NO INSERT permission (never granted)
    LOGGER.info("9. start to test NO INSERT permission enforcement (table model)");
    tableUserInsert(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, false);

    // Step 11: GRANT INSERT ON TABLE
    LOGGER.info("10. start to test high availability of GRANT INSERT ON TABLE");
    assertStatementEffect(
        rootStmt,
        "GRANT INSERT ON TABLE " + testTable + " TO USER " + RENAMED_USER,
        () -> userHasPrivilege(rootStmt, RENAMED_USER, "INSERT"),
        "GRANT INSERT ON TABLE must succeed");
    tableUserInsert(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, true);

    // Step 12: REVOKE INSERT ON TABLE
    LOGGER.info("11. start to test high availability of REVOKE INSERT ON TABLE");
    assertStatementEffect(
        rootStmt,
        "REVOKE INSERT ON TABLE " + testTable + " FROM USER " + RENAMED_USER,
        () -> !userHasPrivilege(rootStmt, RENAMED_USER, "INSERT"),
        "REVOKE INSERT ON TABLE must succeed");
    tableUserInsert(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, false);

    // Step 13: REVOKE SELECT ON TABLE
    LOGGER.info("12. start to test high availability of REVOKE SELECT ON TABLE");
    assertStatementEffect(
        rootStmt,
        "REVOKE SELECT ON TABLE " + testTable + " FROM USER " + RENAMED_USER,
        () -> !userHasPrivilege(rootStmt, RENAMED_USER, "SELECT"),
        "REVOKE SELECT ON TABLE must succeed");
    tableUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, false);

    // Step 14: GRANT SYSTEM
    LOGGER.info("13. start to test high availability of GRANT SYSTEM");
    assertStatementEffect(
        rootStmt,
        "GRANT SYSTEM TO USER " + RENAMED_USER,
        () -> userHasPrivilege(rootStmt, RENAMED_USER, "SYSTEM"),
        "GRANT SYSTEM must succeed");

    // Step 15: REVOKE SYSTEM
    LOGGER.info("14. start to test high availability of REVOKE SYSTEM");
    assertStatementEffect(
        rootStmt,
        "REVOKE SYSTEM FROM USER " + RENAMED_USER,
        () -> !userHasPrivilege(rootStmt, RENAMED_USER, "SYSTEM"),
        "REVOKE SYSTEM must succeed");
  }

  // ==================== Table Model Role-Based Permission ====================

  private void executeTableRoleBasedPermissionHA(
      final Statement rootStmt, final DataNodeWrapper liveDN1) throws Exception {

    final String testTable = DATABASE_NAME + "." + TABLE_NAME;

    // Step: GRANT SELECT ON TABLE TO ROLE → user inherits via role
    LOGGER.info("start to test high availability of GRANT SELECT ON TABLE TO ROLE (table model)");
    assertStatementEffect(
        rootStmt,
        "GRANT SELECT ON TABLE " + testTable + " TO ROLE " + TEST_ROLE,
        () -> userHasPrivilege(rootStmt, RENAMED_USER, "SELECT"),
        "GRANT SELECT ON TABLE TO ROLE must succeed");
    // renamed_user inherits SELECT via test_role
    tableUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, true);

    // Step: REVOKE SELECT ON TABLE FROM ROLE → user loses inherited privilege
    LOGGER.info(
        "start to test high availability of REVOKE SELECT ON TABLE FROM ROLE (table model)");
    assertStatementEffect(
        rootStmt,
        "REVOKE SELECT ON TABLE " + testTable + " FROM ROLE " + TEST_ROLE,
        () -> !userHasPrivilege(rootStmt, RENAMED_USER, "SELECT"),
        "REVOKE SELECT ON TABLE FROM ROLE must succeed");
    tableUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, DATABASE_NAME, TABLE_NAME, false);
  }

  // ==================== Tree Model Permission Management ====================

  private void executeTreePermissionHA(
      final Statement rootStmt, final DataNodeWrapper liveDN0, final DataNodeWrapper liveDN1)
      throws Exception {

    final String treePath = "root.test_auth_tree.**";

    // Step 15: GRANT READ_DATA ON tree path
    LOGGER.info("15. start to test high availability of GRANT READ_DATA (tree model)");
    rootStmt.execute("SET SQL_DIALECT=tree");
    assertStatementEffect(
        rootStmt,
        "GRANT READ_DATA ON " + treePath + " TO USER " + RENAMED_USER,
        () -> userHasPrivilege(rootStmt, RENAMED_USER, "READ_DATA"),
        "GRANT READ_DATA must succeed");
    // Verify: SELECT succeeds on DN-1
    treeUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, true);

    // Step 16: Verify NO WRITE permission
    LOGGER.info("16. start to test NO WRITE permission enforcement (tree model)");
    treeUserInsert(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, false);

    // Step 17: GRANT WRITE_DATA ON tree path
    LOGGER.info("17. start to test high availability of GRANT WRITE_DATA (tree model)");
    assertStatementEffect(
        rootStmt,
        "GRANT WRITE_DATA ON " + treePath + " TO USER " + RENAMED_USER,
        () -> userHasPrivilege(rootStmt, RENAMED_USER, "WRITE_DATA"),
        "GRANT WRITE_DATA must succeed");
    treeUserInsert(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, true);

    // Step 18: REVOKE WRITE_DATA ON tree path
    LOGGER.info("18. start to test high availability of REVOKE WRITE_DATA (tree model)");
    assertStatementEffect(
        rootStmt,
        "REVOKE WRITE_DATA ON " + treePath + " FROM USER " + RENAMED_USER,
        () -> !userHasPrivilege(rootStmt, RENAMED_USER, "WRITE_DATA"),
        "REVOKE WRITE_DATA must succeed");
    treeUserInsert(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, false);

    // Step 19: REVOKE READ_DATA ON tree path
    LOGGER.info("19. start to test high availability of REVOKE READ_DATA (tree model)");
    assertStatementEffect(
        rootStmt,
        "REVOKE READ_DATA ON " + treePath + " FROM USER " + RENAMED_USER,
        () -> !userHasPrivilege(rootStmt, RENAMED_USER, "READ_DATA"),
        "REVOKE READ_DATA must succeed");
    treeUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, false);

    rootStmt.execute("SET SQL_DIALECT=table");
  }

  // ==================== Tree Model Role-Based Permission ====================

  private void executeTreeRoleBasedPermissionHA(
      final Statement rootStmt, final DataNodeWrapper liveDN1) throws Exception {

    final String treePath = "root.test_auth_tree.**";

    // Grant READ_DATA to test_role, renamed_user inherits via role
    LOGGER.info("start to test high availability of GRANT READ_DATA TO ROLE (tree model)");
    rootStmt.execute("SET SQL_DIALECT=tree");
    assertStatementEffect(
        rootStmt,
        "GRANT READ_DATA ON " + treePath + " TO ROLE " + TEST_ROLE,
        () -> roleHasPrivilege(rootStmt, TEST_ROLE, "READ_DATA"),
        "GRANT READ_DATA ON TO ROLE must succeed");
    // renamed_user inherits READ_DATA via test_role
    assertTrue(
        "renamed_user should inherit READ_DATA via test_role",
        userHasPrivilege(rootStmt, RENAMED_USER, "READ_DATA"));
    treeUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, true);

    // Revoke READ_DATA from test_role → renamed_user loses inherited privilege
    LOGGER.info("start to test high availability of REVOKE READ_DATA FROM ROLE (tree model)");
    assertStatementEffect(
        rootStmt,
        "REVOKE READ_DATA ON " + treePath + " FROM ROLE " + TEST_ROLE,
        () -> !roleHasPrivilege(rootStmt, TEST_ROLE, "READ_DATA"),
        "REVOKE READ_DATA ON FROM ROLE must succeed");
    assertTrue(
        "renamed_user should lose READ_DATA after revoke from test_role",
        !userHasPrivilege(rootStmt, RENAMED_USER, "READ_DATA"));
    treeUserSelect(liveDN1, RENAMED_USER, TEST_USER_NEW_PWD, false);

    rootStmt.execute("SET SQL_DIALECT=table");
  }

  // ==================== Cleanup ====================

  private void executeCleanup(final Statement rootStmt, final DataNodeWrapper liveDN1)
      throws Exception {

    // Step 21: REVOKE ROLE
    LOGGER.info("20. start to test high availability of REVOKE ROLE");
    assertStatementEffect(
        rootStmt,
        "REVOKE ROLE " + TEST_ROLE + " FROM " + RENAMED_USER,
        () -> !userHasRole(rootStmt, RENAMED_USER, TEST_ROLE),
        "REVOKE ROLE must succeed");

    // Step 22: DROP ROLE
    LOGGER.info("21. start to test high availability of DROP ROLE");
    assertStatementEffect(
        rootStmt,
        "DROP ROLE " + HA_ROLE,
        () -> !roleExists(rootStmt, HA_ROLE),
        "DROP ROLE must succeed");

    // Step 23: DROP USER renamed_user
    LOGGER.info("22. start to test high availability of DROP USER renamed_user");
    assertStatementEffect(
        rootStmt,
        "DROP USER " + RENAMED_USER,
        () -> !userExists(rootStmt, RENAMED_USER),
        "DROP USER renamed_user must succeed");

    // Step 24: DROP USER ha_user
    LOGGER.info("23. start to test high availability of DROP USER ha_user");
    assertStatementEffect(
        rootStmt,
        "DROP USER " + HA_USER,
        () -> !userExists(rootStmt, HA_USER),
        "DROP USER ha_user must succeed");
  }

  // ==================== Verification Helpers ====================

  private void assertStatementEffect(
      final Statement statement,
      final String sql,
      final Callable<Boolean> effect,
      final String message)
      throws Exception {
    statement.execute(sql);
    assertTrue(message, effect.call());
  }

  private boolean userExists(final Statement stmt, final String userName) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery("LIST USER")) {
      while (rs.next()) {
        if (userName.equalsIgnoreCase(rs.getString(2))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean roleExists(final Statement stmt, final String roleName) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery("LIST ROLE")) {
      while (rs.next()) {
        if (roleName.equalsIgnoreCase(rs.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean listHasRows(final Statement stmt, final String sql) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next();
    }
  }

  private boolean userHasRole(final Statement stmt, final String userName, final String roleName)
      throws SQLException {
    try (final ResultSet rs = stmt.executeQuery("LIST ROLE OF USER " + userName)) {
      while (rs.next()) {
        if (roleName.equalsIgnoreCase(rs.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean userHasPrivilege(
      final Statement stmt, final String userName, final String privilege) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery("LIST PRIVILEGES OF USER " + userName)) {
      while (rs.next()) {
        // LIST PRIVILEGES columns: Role, Scope, Privileges, GrantOption
        if (privilege.equalsIgnoreCase(rs.getString(3))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean roleHasPrivilege(
      final Statement stmt, final String roleName, final String privilege) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery("LIST PRIVILEGES OF ROLE " + roleName)) {
      while (rs.next()) {
        // LIST PRIVILEGES columns: Role, Scope, Privileges, GrantOption
        if (privilege.equalsIgnoreCase(rs.getString(3))) {
          return true;
        }
      }
    }
    return false;
  }

  private void assertConnectionFails(
      final DataNodeWrapper dn,
      final String user,
      final String password,
      final String sqlDialect,
      final String message) {
    try {
      final Connection conn = EnvFactory.getEnv().getConnection(dn, user, password, sqlDialect);
      conn.close();
      Assert.fail(message + " — expected connection failure but succeeded");
    } catch (final SQLException e) {
      // Expected
    }
  }

  // ==================== Table Model User Operation Helpers ====================

  private void tableUserSelect(
      final DataNodeWrapper dn,
      final String user,
      final String password,
      final String databaseName,
      final String tableName,
      final boolean expectSuccess)
      throws SQLException {
    final String sql = "SELECT * FROM " + tableName;
    if (expectSuccess) {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TABLE_SQL_DIALECT);
          final Statement stmt = conn.createStatement()) {
        stmt.execute("USE " + databaseName);
        try (final ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue("SELECT should succeed", rs.next());
        }
      }
    } else {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TABLE_SQL_DIALECT);
          final Statement stmt = conn.createStatement()) {
        stmt.execute("USE " + databaseName);
        stmt.executeQuery(sql);
        Assert.fail("SELECT should fail");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("No permissions") || e.getMessage().contains("Access Denied"));
      }
    }
  }

  private void tableUserInsert(
      final DataNodeWrapper dn,
      final String user,
      final String password,
      final String databaseName,
      final String tableName,
      final boolean expectSuccess)
      throws SQLException {
    final String sql = "INSERT INTO " + tableName + "(time, dev, s1) VALUES(2, 'dev2', 200)";
    if (expectSuccess) {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TABLE_SQL_DIALECT);
          final Statement stmt = conn.createStatement()) {
        stmt.execute("USE " + databaseName);
        stmt.execute(sql);
      }
    } else {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TABLE_SQL_DIALECT);
          final Statement stmt = conn.createStatement()) {
        stmt.execute("USE " + databaseName);
        stmt.execute(sql);
        Assert.fail("INSERT should fail");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("No permissions") || e.getMessage().contains("Access Denied"));
      }
    }
  }

  // ==================== Tree Model User Operation Helpers ====================

  private void treeUserSelect(
      final DataNodeWrapper dn,
      final String user,
      final String password,
      final boolean expectSuccess)
      throws SQLException {
    final String sql = "SELECT * FROM " + TREE_DB_NAME + ".dev1";
    if (expectSuccess) {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TREE_SQL_DIALECT);
          final Statement stmt = conn.createStatement();
          final ResultSet rs = stmt.executeQuery(sql)) {
        assertTrue("Tree SELECT should succeed", rs.next());
      }
    } else {
      // Tree model returns empty result set on permission denial, no exception thrown
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TREE_SQL_DIALECT);
          final Statement stmt = conn.createStatement();
          final ResultSet rs = stmt.executeQuery(sql)) {
        Assert.assertFalse("Tree SELECT should return empty result set", rs.next());
      }
    }
  }

  private void treeUserInsert(
      final DataNodeWrapper dn,
      final String user,
      final String password,
      final boolean expectSuccess)
      throws SQLException {
    final String sql = "INSERT INTO " + TREE_DB_NAME + ".dev1(time, s1) VALUES(2, 200)";
    if (expectSuccess) {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TREE_SQL_DIALECT);
          final Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }
    } else {
      try (final Connection conn =
              EnvFactory.getEnv().getConnection(dn, user, password, BaseEnv.TREE_SQL_DIALECT);
          final Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
        Assert.fail("Tree INSERT should fail");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("No permissions") || e.getMessage().contains("Access Denied"));
      }
    }
  }
}
