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

package org.apache.iotdb.db.it.auth;

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

import static org.apache.iotdb.db.it.auth.IoTDBAuthIT.validateResultSet;

@RunWith(IoTDBTestRunner.class)
@Category(LocalStandaloneIT.class)
public class IoTDBUserRenameIT {

  @Before
  public void setUp() throws Exception {
    // Init 1C1D for testing
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void userRenameTestInTreeModel() throws SQLException {
    userRenameTest(BaseEnv.TREE_SQL_DIALECT);
  }

  @Test
  public void userRenameTestInTableModel() throws SQLException {
    userRenameTest(BaseEnv.TABLE_SQL_DIALECT);
  }

  private void userRenameTest(String dialect) throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(dialect);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER user1 'IoTDB@2021abc'");
      adminStmt.execute("CREATE USER user2 'IoTDB@2023abc'");
      if (BaseEnv.TABLE_SQL_DIALECT.equals(dialect)) {
        adminStmt.execute("GRANT SECURITY TO USER user2");
      } else {
        adminStmt.execute("GRANT SECURITY ON root.** TO USER user2");
      }
      try (Connection userCon =
              EnvFactory.getEnv().getConnection("user1", "IoTDB@2021abc", dialect);
          Statement userStmt = userCon.createStatement()) {
        // A normal user cannot rename other users
        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("ALTER USER user2 RENAME TO user3"));
        // A normal user can only rename himself
        userStmt.execute("ALTER USER user1 RENAME TO user3");
      }
      try (Connection userCon =
              EnvFactory.getEnv().getConnection("user2", "IoTDB@2023abc", dialect);
          Statement userStmt = userCon.createStatement()) {
        // User with SECURITY privilege can rename other users
        userStmt.execute("ALTER USER user3 RENAME TO user1");
        // Nobody can rename superuser
        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("ALTER USER root RENAME TO admin"));
      }
      // Cannot rename an unexisting user
      Assert.assertThrows(
          SQLException.class, () -> adminStmt.execute("ALTER USER user4 RENAME TO user5"));
      // Cannot rename to an already existed user
      Assert.assertThrows(
          SQLException.class, () -> adminStmt.execute("ALTER USER user2 RENAME TO user1"));
      // Cannot rename to an illegal name
      Assert.assertThrows(
          SQLException.class, () -> adminStmt.execute("ALTER USER user2 RENAME TO p00"));
      // Only the superuser can rename him/herself
      adminStmt.execute("ALTER USER root RENAME TO admin");
    }
    try (Connection adminCon = EnvFactory.getEnv().getConnection("admin", "root", dialect);
        Statement adminStmt = adminCon.createStatement()) {
      // We can rename other user to root
      adminStmt.execute("ALTER USER user1 RENAME TO root");
      adminStmt.execute("ALTER USER root RENAME TO user4");
      // We can create another root
      adminStmt.execute("CREATE USER root 'IoTDB@2025abc'");
      // We can grant and revoke privilege to the new root
      if (BaseEnv.TABLE_SQL_DIALECT.equals(dialect)) {
        adminStmt.execute("GRANT SYSTEM TO USER root");
        adminStmt.execute("REVOKE SYSTEM FROM USER root");
      } else {
        adminStmt.execute("GRANT SYSTEM ON root.** TO USER root");
        adminStmt.execute("REVOKE SYSTEM ON root.** FROM USER root");
      }
      // Ensure everything works
      String ans = "0,admin,\n" + "10000,user4,\n" + "10001,user2,\n" + "10002,root,\n";
      ResultSet resultSet = adminStmt.executeQuery("LIST USER");
      validateResultSet(resultSet, ans);
      // Finally, the other root can be deleted
      adminStmt.execute("DROP USER root");
      ans = "0,admin,\n" + "10000,user4,\n" + "10001,user2,\n";
      resultSet = adminStmt.executeQuery("LIST USER");
      validateResultSet(resultSet, ans);
    }
  }
}
