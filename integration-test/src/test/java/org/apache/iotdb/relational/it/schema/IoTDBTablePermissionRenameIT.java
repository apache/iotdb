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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTablePermissionRenameIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRenameTablePermissionDenied() throws Exception {
    // admin setup
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("DROP DATABASE IF EXISTS permdb");
      adminStmt.execute("CREATE DATABASE IF NOT EXISTS permdb");
      adminStmt.execute("CREATE USER permUser 'permPass123456@'");
      adminStmt.execute("USE permdb");
      adminStmt.execute("CREATE TABLE IF NOT EXISTS t1 (s1 int32)");
    }

    // grant a non-ALTER privilege so the user can USE the database
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("USE permdb");
      adminStmt.execute("GRANT SELECT ON TABLE t1 TO USER permUser");
    }

    // user attempts rename without ALTER -> expect access denied
    try (final Connection userCon =
            EnvFactory.getEnv()
                .getConnection("permUser", "permPass123456@", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      // ensure user is using the target database
      userStmt.execute("USE permdb");
      try {
        userStmt.execute("ALTER TABLE t1 RENAME TO t1_new");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "803: Access Denied: No permissions for this operation, please add privilege ALTER ON permdb.t1",
            e.getMessage());
      }
    }

    // grant ALTER and retry
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      // ensure we're operating in the target database
      adminStmt.execute("USE permdb");
      adminStmt.execute("GRANT ALTER,INSERT,SELECT ON TABLE t1 TO USER permUser");
    }

    try (final Connection userCon =
            EnvFactory.getEnv()
                .getConnection("permUser", "permPass123456@", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      // ensure user is using the target database
      userStmt.execute("USE permdb");
      userStmt.execute("ALTER TABLE t1 RENAME TO t1_new");
      userStmt.execute("INSERT INTO t1_new (time, s1) VALUES (1, 1)");
      try (final ResultSet rs = userStmt.executeQuery("SELECT * FROM t1_new")) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
      }
    }

    // cleanup
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      // operate in the database for cleanup
      try {
        adminStmt.execute("USE permdb");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("REVOKE ALTER ON TABLE t1_new FROM USER permUser");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("REVOKE SELECT ON TABLE t1 FROM USER permUser");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("DROP TABLE IF EXISTS t1_new");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("DROP DATABASE IF EXISTS permdb");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("DROP USER permUser");
      } catch (SQLException ignore) {
      }
    }
  }

  @Test
  public void testRenameColumnPermissionDenied() throws Exception {
    // admin setup
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("DROP DATABASE IF EXISTS col_db");
      adminStmt.execute("CREATE DATABASE IF NOT EXISTS col_db");
      adminStmt.execute("CREATE USER colUser 'colPass123456@'");
      adminStmt.execute("USE col_db");
      adminStmt.execute("CREATE TABLE IF NOT EXISTS tc (c1 int32)");
      adminStmt.execute("INSERT INTO tc (time, c1) VALUES (1, 1)");
    }

    // grant a non-ALTER privilege so the user can USE the database
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("USE col_db");
      adminStmt.execute("GRANT SELECT ON TABLE tc TO USER colUser");
    }

    // user attempts rename column without ALTER -> expect access denied
    try (final Connection userCon =
            EnvFactory.getEnv()
                .getConnection("colUser", "colPass123456@", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      // ensure user is using the target database
      userStmt.execute("USE col_db");
      try {
        userStmt.execute("ALTER TABLE tc RENAME COLUMN c1 TO c2");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "803: Access Denied: No permissions for this operation, please add privilege ALTER ON col_db.tc",
            e.getMessage());
      }
    }

    // grant ALTER and retry
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      // ensure we're operating in the target database
      adminStmt.execute("USE col_db");
      adminStmt.execute("GRANT ALTER,INSERT,SELECT ON TABLE tc TO USER colUser");
    }

    try (final Connection userCon =
            EnvFactory.getEnv()
                .getConnection("colUser", "colPass123456@", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      // ensure user is using the target database
      userStmt.execute("USE col_db");
      userStmt.execute("ALTER TABLE tc RENAME COLUMN c1 TO c2");
      userStmt.execute("INSERT INTO tc (time, c2) VALUES (2, 2)");
      try (final ResultSet rs = userStmt.executeQuery("SELECT * FROM tc ORDER BY time")) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals(1, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
      }

      // verify metadata contains c2
      try (final ResultSet rs2 = userStmt.executeQuery("DESC tc")) {
        boolean found = false;
        while (rs2.next()) {
          String colName = rs2.getString(1);
          if ("c2".equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
    }

    // cleanup
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      try {
        adminStmt.execute("USE col_db");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("REVOKE ALTER ON TABLE tc FROM USER colUser");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("REVOKE SELECT ON TABLE tc FROM USER colUser");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("DROP TABLE IF EXISTS tc");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("DROP DATABASE IF EXISTS col_db");
      } catch (SQLException ignore) {
      }
      try {
        adminStmt.execute("DROP USER colUser");
      } catch (SQLException ignore) {
      }
    }
  }
}
