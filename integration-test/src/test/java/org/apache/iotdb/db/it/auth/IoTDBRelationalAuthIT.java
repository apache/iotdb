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
import org.apache.iotdb.jdbc.IoTDBSQLException;

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

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRelationalAuthIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
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

      adminStmt.execute("create user testuser 'password123456'");
      try (Connection userCon =
              EnvFactory.getEnv()
                  .getConnection("testuser", "password123456", BaseEnv.TABLE_SQL_DIALECT);
          Statement userStmt = userCon.createStatement()) {
        ResultSet resultSet = userStmt.executeQuery("LIST USER");
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("10000", resultSet.getString(1));
        Assert.assertEquals("testuser", resultSet.getString(2));
        Assert.assertFalse(resultSet.next());
      }
      adminStmt.execute("create database testdb");
      adminStmt.execute("GRANT SECURITY to user testuser");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT MANAGE_USER to user testuser");
          });
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
      // No Maintain privilege in table model.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT MAINTAIN to user testuser");
          });

      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT MANAGE_ROLE to user testuser");
          });
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
                  ",,SECURITY,false,",
                  ",*.*,SELECT,false,",
                  ",*.*,INSERT,false,",
                  ",*.*,DELETE,false,",
                  ",testdb.*,SELECT,true,",
                  ",testdb.tb,SELECT,false,",
                  ",testdb.tb,INSERT,true,",
                  ",testdb.tb,DROP,true,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
      adminStmt.execute("create role testrole");
      adminStmt.execute("GRANT ROLE testrole to testuser");
      rs = adminStmt.executeQuery("LIST USER OF ROLE testrole");
      TestUtils.assertResultSetEqual(rs, "UserId,User,", Collections.singleton("10000,testuser,"));
      rs = adminStmt.executeQuery("LIST ROLE OF USER testuser");
      TestUtils.assertResultSetEqual(rs, "Role,", Collections.singleton("testrole,"));
    }
  }

  @Test
  public void checkAuthorStatementPrivilegeCheck() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user testuser 'password123456'");
      adminStmt.execute("create user testuser2 'password123456'");
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
      adminStmt.execute("GRANT SECURITY to user testuser with grant option");

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
            EnvFactory.getEnv()
                .getConnection("testuser", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        Statement userStmt = userCon1.createStatement()) {

      // can create role
      userStmt.execute("CREATE ROLE testrole2");
      // can grant role to user
      userStmt.execute("GRANT ROLE testrole2 to testuser");

      // can list itself privileges and the all roles privileges
      ResultSet rs = userStmt.executeQuery("List privileges of user testuser");
      Set<String> ans =
          new HashSet<>(
              Arrays.asList(
                  ",,SECURITY,true,",
                  ",*.*,ALTER,false,",
                  ",testdb.*,INSERT,false,",
                  ",testdb.tb,SELECT,false,",
                  ",testdb.tb,INSERT,false,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
      rs = userStmt.executeQuery("List privileges of role testrole");
      TestUtils.assertResultSetEqual(
          rs, "Role,Scope,Privileges,GrantOption,", Collections.emptySet());
      rs = userStmt.executeQuery("List privileges of role testrole2");
      TestUtils.assertResultSetEqual(
          rs, "Role,Scope,Privileges,GrantOption,", Collections.emptySet());
      userStmt.execute("CREATE USER testuser3 'password123456'");

      userStmt.execute("GRANT drop on database testdb to user testuser3");
      userStmt.execute("GRANT SELECT ON database testdb to user testuser3");
      rs = userStmt.executeQuery("List privileges of user testuser3");
      ans = new HashSet<>(Arrays.asList(",testdb.*,SELECT,false,", ",testdb.*,DROP,false,"));
      TestUtils.assertResultSetEqual(rs, "Role,Scope,Privileges,GrantOption,", ans);
      userStmt.execute("REVOKE SELECT ON DATABASE testdb from user testuser3");

      rs = userStmt.executeQuery("List privileges of user testuser3");
      TestUtils.assertResultSetEqual(
          rs, "Role,Scope,Privileges,GrantOption,", Collections.singleton(",testdb.*,DROP,false,"));
    }
  }

  @Test
  public void checkGrantRevokeAllPrivileges() throws SQLException {
    // In this IT:
    // grant
    // 1. grant all on table tb1 with grant option
    // 2. grant all on database testdb
    // 3. grant all on any
    // revoke
    // 1. revoke grant option for all on table tb1
    // 2. revoke all on table tb1
    // 3. revoke all on database testdb
    // 4. revoke all on any
    // grant and revoke
    // 1. grant all on user/role
    // 2. revoke all on any
    // 3. revoke all on user/role

    for (boolean isUser : new boolean[] {true, false}) {
      try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement adminStmt = adminCon.createStatement()) {
        adminStmt.execute("create database testdb");
        adminStmt.execute(isUser ? "create user test 'password123456'" : "create role test");
        adminStmt.execute("use testdb");

        // 1. grant all on table tb1 with grant option
        adminStmt.execute(
            "grant all on table tb1 to "
                + (isUser ? "user test" : "role test")
                + " with grant option");
        Set<String> listPrivilegeResult = new HashSet<>();
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege() && !privilegeType.isDeprecated()) {
            listPrivilegeResult.add(
                (isUser ? "," : "test,") + "testdb.tb1," + privilegeType + ",true,");
          }
        }
        ResultSet resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 2. grant all on database testdb
        adminStmt.execute(
            "grant all on database testdb to " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.add(
                (isUser ? "," : "test,") + "testdb.*," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 3. grant all on any
        adminStmt.execute("grant all on any to " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.add((isUser ? "," : "test,") + "*.*," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 1. revoke grant option for all on table tb1
        adminStmt.execute(
            "revoke grant option for all on table tb1 from "
                + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.remove(
                (isUser ? "," : "test,") + "testdb.tb1," + privilegeType + ",true,");
            listPrivilegeResult.add(
                (isUser ? "," : "test,") + "testdb.tb1," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 2. revoke all on table tb1
        adminStmt.execute("revoke all on table tb1 from " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.remove(
                (isUser ? "," : "test,") + "testdb.tb1," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 3. revoke all on database testdb
        adminStmt.execute(
            "revoke all on database testdb from " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.remove(
                (isUser ? "," : "test,") + "testdb.*," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 4. revoke all on any
        adminStmt.execute("revoke all on any from " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.remove(
                (isUser ? "," : "test,") + "*.*," + privilegeType + ",false,");
          }
        }
        Assert.assertTrue(listPrivilegeResult.isEmpty());
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 1. grant all on user/role
        adminStmt.execute("grant all to " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isDeprecated() || privilegeType.isHided()) {
            continue;
          }
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.add((isUser ? "," : "test,") + "*.*," + privilegeType + ",false,");
          } else if (privilegeType.forRelationalSys()) {
            listPrivilegeResult.add((isUser ? "," : "test,") + "," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 2. revoke all on any
        adminStmt.execute("revoke all on any from " + (isUser ? "user test" : "role test"));
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            listPrivilegeResult.remove(
                (isUser ? "," : "test,") + "*.*," + privilegeType + ",false,");
          }
        }
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);

        // 3. revoke all on user/role
        adminStmt.execute("revoke all from " + (isUser ? "user test" : "role test"));
        listPrivilegeResult.clear();
        resultSet =
            adminStmt.executeQuery("List privileges of " + (isUser ? "user test" : "role test"));
        TestUtils.assertResultSetEqual(
            resultSet, "Role,Scope,Privileges,GrantOption,", listPrivilegeResult);
        adminStmt.execute("drop database testdb");
        adminStmt.execute(isUser ? "drop user test" : "drop role test");
      }
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password123456'");
      adminStmt.execute("create user test2 'password123456'");
      adminStmt.execute("grant all to user test");
      adminStmt.execute("grant all to user test2 with grant option");
      adminStmt.execute("revoke SELECT ON ANY from user test");
      adminStmt.execute("create role role1");
      adminStmt.execute("grant all to role role1 with grant option");
    }

    Set<String> listUserPrivilegeResult = new HashSet<>();
    for (PrivilegeType privilegeType : PrivilegeType.values()) {
      if (privilegeType == PrivilegeType.SELECT
          || privilegeType.isDeprecated()
          || privilegeType.isHided()) {
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
      if (privilegeType.isDeprecated() || privilegeType.isHided()) {
        continue;
      }
      if (privilegeType.isRelationalPrivilege()) {
        listRolePrivilegeResult.add("role1,*.*," + privilegeType + ",true,");
      }
      if (privilegeType.forRelationalSys()) {
        listRolePrivilegeResult.add("role1,," + privilegeType + ",true,");
      }
    }

    try (Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        Statement userConStatement = userCon.createStatement()) {
      ResultSet resultSet = userConStatement.executeQuery("List privileges of user test");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", listUserPrivilegeResult);

      // Have manage_role privilege
      resultSet = userConStatement.executeQuery("List privileges of role role1");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", listRolePrivilegeResult);
    }

    try (Connection userCon =
            EnvFactory.getEnv()
                .getConnection("test2", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        Statement userConStatement = userCon.createStatement()) {
      // user2 can grant all to user test
      userConStatement.execute("GRANT ALL to user test");
      // user2 can revoke all from user test
      userConStatement.execute("REVOKE ALL from user test");

      userConStatement.execute("GRANT ALL to user test");
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("revoke SECURITY from user test2");
    }

    try (Connection userCon =
            EnvFactory.getEnv()
                .getConnection("test2", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        Statement userConStatement = userCon.createStatement()) {
      // user2 can not grant all to user test
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userConStatement.execute("GRANT ALL to user test2");
          });

      // user2 can not revoke all from user test because does not hava all privileges
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userConStatement.execute("REVOKE ALL to user test2");
          });
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("REVOKE ALL FROM USER test");
      ResultSet resultSet = adminStmt.executeQuery("List privileges of user test");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", Collections.emptySet());
      adminStmt.execute("GRANT ALL ON db1.test TO USER test");
      adminStmt.execute("GRANT ALL ON DATABASE db2 TO USER test with grant option");
      resultSet = adminStmt.executeQuery("List privileges of user test");
      Set<String> resultSetALL = new HashSet<>();
      for (PrivilegeType privilegeType : PrivilegeType.values()) {
        if (privilegeType.isRelationalPrivilege()) {
          resultSetALL.add(",db2.*," + privilegeType + ",true,");
          resultSetALL.add(",db1.test," + privilegeType + ",false,");
        }
      }
      TestUtils.assertResultSetEqual(resultSet, "Role,Scope,Privileges,GrantOption,", resultSetALL);
      adminStmt.execute("REVOKE ALL FROM USER test");
      resultSet = adminStmt.executeQuery("List privileges of user test");
      TestUtils.assertResultSetEqual(
          resultSet, "Role,Scope,Privileges,GrantOption,", Collections.emptySet());
    }
  }

  @Test
  public void testCreateUserAndRole() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      // normal case
      adminStmt.execute("create user testuser 'password123456'");
      // username abnormal
      adminStmt.execute("create user \"!@#$%^*()_+-=1\" 'password123456'");

      // username and password abnormal
      adminStmt.execute("create user \"!@#$%^*()_+-=2\" '!@#$%^*()_+-='");

      // rolename abnormal
      adminStmt.execute("create role \"!@#$%^*()_+-=3\" ");

      ResultSet resultSet = adminStmt.executeQuery("List user");
      Set<String> resultSetList = new HashSet<>();
      resultSetList.add("0,root,");
      resultSetList.add("10000,testuser,");
      resultSetList.add("10001,!@#$%^*()_+-=1,");
      resultSetList.add("10002,!@#$%^*()_+-=2,");
      TestUtils.assertResultSetEqual(resultSet, "UserId,User,", resultSetList);
      resultSet = adminStmt.executeQuery("List role");
      TestUtils.assertResultSetEqual(resultSet, "Role,", Collections.singleton("!@#$%^*()_+-=3,"));
      adminStmt.execute("GRANT role \"!@#$%^*()_+-=3\" to  \"!@#$%^*()_+-=1\"");
      adminStmt.execute("ALTER user \"!@#$%^*()_+-=1\" set password '!@#$%^*()_+-=\'");
    }

    try (Connection userCon =
            EnvFactory.getEnv()
                .getConnection("!@#$%^*()_+-=1", "!@#$%^*()_+-=", BaseEnv.TABLE_SQL_DIALECT);
        Statement userConStatement = userCon.createStatement()) {
      // List his role.
      ResultSet set = userConStatement.executeQuery("List role of user \"!@#$%^*()_+-=1\"");
      TestUtils.assertResultSetEqual(set, "Role,", Collections.singleton("!@#$%^*()_+-=3,"));
    } catch (IoTDBSQLException e) {
      Assert.fail();
    }
  }

  @Test
  public void testAlterNonExistingUser() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      try {
        adminStmt.execute("ALTER USER nonExist SET PASSWORD 'asdfer1124566'");
      } catch (SQLException e) {
        assertEquals("701: User nonExist not found", e.getMessage());
      }
    }
  }

  @Test
  public void testAudit() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      try {
        adminStmt.execute("grant select on database __audit to user user2");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to __audit", e.getMessage());
      }
      try {
        adminStmt.execute("grant select on table __audit.t1 to user user2");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to __audit", e.getMessage());
      }
      try {
        adminStmt.execute("revoke select on table __audit.t1 from user user2");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to __audit", e.getMessage());
      }
      try {
        adminStmt.execute("grant select on table __audit.t1 to role role1");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to __audit", e.getMessage());
      }
      try {
        adminStmt.execute("revoke select on table __audit.t1 from role role1");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to __audit", e.getMessage());
      }
    }
  }
}
