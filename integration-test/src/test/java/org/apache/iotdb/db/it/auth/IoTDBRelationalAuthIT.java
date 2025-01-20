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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRelationalAuthIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void validateResultSet(ResultSet set, String ans) throws SQLException {
    try {
      StringBuilder builder = new StringBuilder();
      ResultSetMetaData metaData = set.getMetaData();
      int colNum = metaData.getColumnCount();
      while (set.next()) {
        for (int i = 1; i <= colNum; i++) {
          builder.append(set.getString(i)).append(",");
        }
        builder.append("\n");
      }
      String result = builder.toString();
      assertEquals(ans.length(), result.length());
      List<String> ansLines = Arrays.asList(ans.split("\n"));
      List<String> resultLines = Arrays.asList(result.split("\n"));
      assertEquals(ansLines.size(), resultLines.size());
      for (String resultLine : resultLines) {
        assertTrue(ansLines.contains(resultLine));
      }
    } finally {
      set.close();
    }
  }

  @Test
  public void listUserPrivileges() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {

      adminStmt.execute("create user testuser 'password'");
      adminStmt.execute("create database testdb");
      adminStmt.execute("GRANT MANAGE_USER to user testuser");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT MANAGE_DATABASE to user testuser");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT read_data on database db1 to user testuser");
          });

      adminStmt.execute("GRANT MANAGE_ROLE TO USER testuser");
      adminStmt.execute("GRANT SELECT ON ANY TO USER testuser");
      adminStmt.execute("GRANT INSERT ON ANY TO USER testuser");
      adminStmt.execute("GRANT DELETE ON ANY TO USER testuser");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT SELECT ON TABLE TB to user testuser");
          });
      adminStmt.execute("GRANT SELECT ON DATABASE TESTDB TO USER testuser");
      adminStmt.execute("GRANT SELECT ON DATABASE TESTDB TO USER testuser WITH GRANT OPTION");

      adminStmt.execute("use testdb");
      adminStmt.execute("GRANT SELECT ON TABLE TB to user testuser");
      adminStmt.execute("GRANT INSERT ON TABLE TB to user testuser with grant option");
      adminStmt.execute("GRANT DROP ON testdb.tb to user testuser with grant option");

      ResultSet rs = adminStmt.executeQuery("LIST PRIVILEGES OF USER testuser");
      String ans =
          ",,MANAGE_USER,false,\n"
              + ",,MANAGE_ROLE,false,\n"
              + ",*.*,SELECT,false,\n"
              + ",*.*,INSERT,false,\n"
              + ",*.*,DELETE,false,\n"
              + ",testdb.*,SELECT,true,\n"
              + ",testdb.tb,SELECT,false,\n"
              + ",testdb.tb,INSERT,true,\n"
              + ",testdb.tb,DROP,true,\n";
      validateResultSet(rs, ans);
    }
  }

  @Test
  public void checkAuthorStatementPrivilegeCheck() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user testuser 'password'");
      adminStmt.execute("create user testuser2 'password'");
      adminStmt.execute("create role testrole");
      adminStmt.execute("create database testdb");

      // cannot create admin user
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("CREATE USER root 'password'");
          });
      // cannot create admin role
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("CREATE role root");
          });

      // cannot grant role to admin user
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT ROLE testrole to root");
          });

      // cannot grant privileg to admin user
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT MANAGE_USER to root");
          });

      // admin can do all things below.
      adminStmt.execute("GRANT MANAGE_USER to user testuser2 with grant option");
      adminStmt.execute("GRANT MANAGE_ROLE to user testuser");
      adminStmt.execute("GRANT MAINTAIN to ROLE testrole with grant option");

      adminStmt.execute("use testdb");
      adminStmt.execute("GRANT SELECT ON TABLE TB to user testuser");
      adminStmt.execute("GRANT INSERT ON TABLE TB to user testuser");
      adminStmt.execute("GRANT INSERT ON DATABASE testdb to user testuser");
      adminStmt.execute("GRANT ALTER ON ANY to user testuser");

      adminStmt.execute("GRANT DROP ON TABLE TB to user testuser2 with grant option");
      adminStmt.execute("GRANT CREATE ON TABLE TB to user testuser2 with grant option");
      adminStmt.execute("GRANT DROP ON DATABASE testdb to user testuser2 with grant option");
      adminStmt.execute("GRANT SELECT ON ANY to user testuser2 with grant option");

      adminStmt.execute("GRANT ROLE testrole to testuser");
    }

    try (Connection userCon1 =
            EnvFactory.getEnv().getConnection("testuser", "password", BaseEnv.TABLE_SQL_DIALECT);
        Statement userStmt = userCon1.createStatement()) {
      // 1. user1's privileges
      // testdb.TB select
      // testdb.TB insert
      // testdb.* insert
      // any alter
      // manage_role
      // MAINTAIN with grant option

      // cannot create user
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("CREATE USER testuser3 'password'");
          });
      // can create role
      userStmt.execute("CREATE ROLE testrole2");
      // can grant role to user
      userStmt.execute("GRANT ROLE testrole2 to testuser");
      // cannot grant privileges to other
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT SELECT ON testdb.TB to role testrole2");
          });

      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT ALTER ON ANY to role testrole2");
          });

      // cannot grant manage_role to other
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT manage_role to role testrole2");
          });
      userStmt.execute("GRANT MAINTAIN to ROLE testrole2");

      // can list itself privileges and the all roles privileges
      ResultSet rs = userStmt.executeQuery("List privileges of user testuser");
      String ans =
          ",,MANAGE_ROLE,false,\n"
              + ",*.*,ALTER,false,\n"
              + ",testdb.*,INSERT,false,\n"
              + ",testdb.tb,SELECT,false,\n"
              + ",testdb.tb,INSERT,false,\n"
              + "testrole2,,MAINTAIN,false,\n"
              + "testrole,,MAINTAIN,true,\n";
      validateResultSet(rs, ans);
      rs = userStmt.executeQuery("List privileges of role testrole");
      ans = "testrole,,MAINTAIN,true,\n";
      validateResultSet(rs, ans);
      rs = userStmt.executeQuery("List privileges of role testrole2");
      ans = "testrole2,,MAINTAIN,false,\n";
      validateResultSet(rs, ans);
      // testdb.TB's privilege is not grant option.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT insert on testdb.TB to role testrole2");
          });

      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT ALTER on any to role testrole2");
          });
    }

    try (Connection userCon1 =
            EnvFactory.getEnv().getConnection("testuser2", "password", BaseEnv.TABLE_SQL_DIALECT);
        Statement userStmt = userCon1.createStatement()) {
      // 2. user2's privileges
      // MANAGE_USER with grant option
      // testdb.tb drop with grant option
      // testdb.tb create with grant option
      // testdb.* drop with grant option
      // any select with grant option

      // can create user.
      userStmt.execute("CREATE USER testuser3 'password'");

      // can not create role
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("CREATE ROLE testrole3");
          });

      // cannot list role's privileges
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.executeQuery("List privileges of role testrole");
          });

      userStmt.execute("GRANT drop on database testdb to user testuser3");
      userStmt.execute("GRANT SELECT ON database testdb to user testuser3");
      ResultSet rs = userStmt.executeQuery("List privileges of user testuser3");
      String ans = ",testdb.*,SELECT,false,\n" + ",testdb.*,DROP,false,\n";
      validateResultSet(rs, ans);
      userStmt.execute("REVOKE SELECT ON DATABASE testdb from user testuser3");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT CREATE ON DATABASE testdb to user testuser3");
          });

      rs = userStmt.executeQuery("List privileges of user testuser3");
      ans = ",testdb.*,DROP,false,\n";
      validateResultSet(rs, ans);
    }
  }
}
