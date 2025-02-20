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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.it.utils.TestUtils;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
      Set<String> ans =
          new HashSet<>(
              Arrays.asList(
                  ",,MANAGE_USER,false,",
                  ",,MANAGE_ROLE,false,",
                  ",*.*,SELECT,false,",
                  ",*.*,INSERT,false,",
                  ",*.*,DELETE,false,",
                  ",testdb.*,SELECT,true,",
                  ",testdb.tb,SELECT,false,",
                  ",testdb.tb,INSERT,true,",
                  ",testdb.tb,DROP,true,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
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

      // cannot grant privilege to admin user
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
      Set<String> ans =
          new HashSet<>(
              Arrays.asList(
                  ",,MANAGE_ROLE,false,",
                  ",*.*,ALTER,false,",
                  ",testdb.*,INSERT,false,",
                  ",testdb.tb,SELECT,false,",
                  ",testdb.tb,INSERT,false,",
                  "testrole2,,MAINTAIN,false,",
                  "testrole,,MAINTAIN,true,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
      rs = userStmt.executeQuery("List privileges of role testrole");
      ans = new HashSet<>(Collections.singletonList("testrole,,MAINTAIN,true,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
      rs = userStmt.executeQuery("List privileges of role testrole2");
      ans = new HashSet<>(Collections.singletonList("testrole2,,MAINTAIN,false,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
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
      Set<String> ans =
          new HashSet<>(Arrays.asList(",testdb.*,SELECT,false,", ",testdb.*,DROP,false,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
      userStmt.execute("REVOKE SELECT ON DATABASE testdb from user testuser3");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("GRANT CREATE ON DATABASE testdb to user testuser3");
          });

      rs = userStmt.executeQuery("List privileges of user testuser3");
      TestUtils.assertResultSetEqual(
          rs, "Role,Scope,Privileges,GrantOption,", Collections.singleton(",testdb.*,DROP,false,"));
    }
  }

  @Test
  public void checkGrantRevokeAllPrivileges() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password'");
      adminStmt.execute("grant all to user test");
      adminStmt.execute("revoke SELECT ON ANY from user test");
      adminStmt.execute("create role role1");
      adminStmt.execute("grant all to role role1 with grant option");
    }

    Set<String> listUserPrivilegeResult = new HashSet<>();
    for (PrivilegeType privilegeType : PrivilegeType.values()) {
      if (privilegeType == PrivilegeType.SELECT) {
        continue;
      }
      if (privilegeType.isRelationalPrivilege()) {
        listUserPrivilegeResult.add(",*.*," + privilegeType + ",false,");
      }
      if (privilegeType.forRelationalSys()) {
        listUserPrivilegeResult.add(",," + privilegeType + ",false,");
      }
    }

    Set<String> listRolePrivilegeResult = new HashSet<>();
    for (PrivilegeType privilegeType : PrivilegeType.values()) {
      if (privilegeType.isRelationalPrivilege()) {
        listRolePrivilegeResult.add("role1,*.*," + privilegeType + ",true,");
      }
      if (privilegeType.forRelationalSys()) {
        listRolePrivilegeResult.add("role1,," + privilegeType + ",true,");
      }
    }

    try (Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
        Statement userConStatement = userCon.createStatement()) {
      ResultSet resultSet = userConStatement.executeQuery("List privileges of user test");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", listUserPrivilegeResult);

      // Have manage_role privilege
      resultSet = userConStatement.executeQuery("List privileges of role role1");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", listRolePrivilegeResult);

      // Do not have grant option
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userConStatement.execute("GRANT SELECT ON DATABASE TEST to role role1");
          });
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("REVOKE ALL FROM USER test");
      ResultSet resultSet = adminStmt.executeQuery("List privileges of user test");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", Collections.emptySet());
    }
  }
}
