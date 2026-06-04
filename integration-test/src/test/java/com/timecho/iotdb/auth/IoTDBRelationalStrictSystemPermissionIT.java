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

package com.timecho.iotdb.auth;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.executeTableNonQuery;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRelationalStrictSystemPermissionIT {

  private Set<String> createdUsers;
  private Set<String> createdRoles;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    executeTableNonQuery(
        "set configuration enable_separation_of_powers='true'", "root", "TimechoDB@2021");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() {
    createdUsers = new HashSet<>();
    createdRoles = new HashSet<>();
  }

  @After
  public void tearDown() {
    for (String user : createdUsers) {
      dropUser(user);
    }
    for (String role : createdRoles) {
      dropRole(role);
    }
  }

  @Test
  public void testRootLogin() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection("root", "TimechoDB@2021")) {
      Assert.fail();
    } catch (IoTDBConnectionException e) {
      Assert.assertEquals(
          "803: SUPER USER is not allowed to login when separation of admin powers is enabled.",
          e.getMessage());
    }
  }

  @Test
  public void listUsers() throws IoTDBConnectionException, StatementExecutionException {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");
    Assert.assertEquals(Collections.singleton("user1"), executeListUserWithSpecifiedUser("user1"));
    Assert.assertEquals(
        new HashSet<>(Collections.singletonList("sys_admin")),
        executeListUserWithSpecifiedUser("sys_admin"));
    Assert.assertEquals(
        new HashSet<>(Collections.singletonList("audit_admin")),
        executeListUserWithSpecifiedUser("audit_admin"));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList("root", "sys_admin", "security_admin", "audit_admin", "user1", "user2")),
        executeListUserWithSpecifiedUser("security_admin"));

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    Assert.assertEquals(Collections.singleton("user1"), executeListUserWithSpecifiedUser("user1"));
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("sys_admin", "user1")),
        executeListUserWithSpecifiedUser("sys_admin"));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList("root", "sys_admin", "security_admin", "audit_admin", "user1", "user2")),
        executeListUserWithSpecifiedUser("security_admin"));
    Assert.assertEquals(
        new HashSet<>(Collections.singletonList("audit_admin")),
        executeListUserWithSpecifiedUser("audit_admin"));
    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");

    executeTableNonQuery("grant audit to user user1", "audit_admin", "TimechoDB@2021");
    Assert.assertEquals(Collections.singleton("user1"), executeListUserWithSpecifiedUser("user1"));
    Assert.assertEquals(
        new HashSet<>(Collections.singletonList("sys_admin")),
        executeListUserWithSpecifiedUser("sys_admin"));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList("root", "sys_admin", "security_admin", "audit_admin", "user1", "user2")),
        executeListUserWithSpecifiedUser("security_admin"));
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("audit_admin", "user1")),
        executeListUserWithSpecifiedUser("audit_admin"));
    executeTableNonQuery("revoke audit from user user1", "audit_admin", "TimechoDB@2021");

    executeTableNonQuery("grant security to user user1", "security_admin", "TimechoDB@2021");
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList("root", "sys_admin", "security_admin", "audit_admin", "user1", "user2")),
        executeListUserWithSpecifiedUser("user1"));
    Assert.assertEquals(
        new HashSet<>(Collections.singletonList("sys_admin")),
        executeListUserWithSpecifiedUser("sys_admin"));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList("root", "sys_admin", "security_admin", "audit_admin", "user1", "user2")),
        executeListUserWithSpecifiedUser("security_admin"));
    Assert.assertEquals(
        new HashSet<>(Collections.singletonList("audit_admin")),
        executeListUserWithSpecifiedUser("audit_admin"));
    executeTableNonQuery("revoke security from user user1", "security_admin", "TimechoDB@2021");
  }

  @Test
  public void listUserOfRole() throws IoTDBConnectionException, StatementExecutionException {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");
    createUser("user3", "TimechoDB@2021");
    createUser("user4", "TimechoDB@2021");
    createRole("role1");

    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "sys_admin"));
    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "security_admin"));
    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "audit_admin"));
    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "user1"));

    executeTableNonQuery("grant role role1 to user1", "security_admin", "TimechoDB@2021");
    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "sys_admin"));
    Assert.assertEquals(
        Collections.singleton("user1"),
        executeListUserOfRoleWithSpecifiedUser("role1", "security_admin"));
    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "audit_admin"));
    Assert.assertEquals(
        Collections.singleton("user1"), executeListUserOfRoleWithSpecifiedUser("role1", "user1"));
    Assert.assertEquals(
        Collections.emptySet(), executeListUserOfRoleWithSpecifiedUser("role1", "user2"));

    executeTableNonQuery("grant role role1 to user2", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant role role1 to user3", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant system to user user2", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user3", "audit_admin", "TimechoDB@2021");
    executeTableNonQuery("grant security to user user4", "security_admin", "TimechoDB@2021");
    Assert.assertEquals(
        Collections.singleton("user2"),
        executeListUserOfRoleWithSpecifiedUser("role1", "sys_admin"));
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("user1", "user2", "user3")),
        executeListUserOfRoleWithSpecifiedUser("role1", "security_admin"));
    Assert.assertEquals(
        Collections.singleton("user3"),
        executeListUserOfRoleWithSpecifiedUser("role1", "audit_admin"));
    Assert.assertEquals(
        Collections.singleton("user1"), executeListUserOfRoleWithSpecifiedUser("role1", "user1"));
    Assert.assertEquals(
        Collections.singleton("user2"), executeListUserOfRoleWithSpecifiedUser("role1", "user2"));
    Assert.assertEquals(
        Collections.singleton("user3"), executeListUserOfRoleWithSpecifiedUser("role1", "user3"));
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("user1", "user2", "user3")),
        executeListUserOfRoleWithSpecifiedUser("role1", "user4"));
    executeTableNonQuery("revoke system from user user2", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("revoke audit from user user3", "audit_admin", "TimechoDB@2021");
    executeTableNonQuery("revoke security from user user4", "security_admin", "TimechoDB@2021");
  }

  @Test
  public void updateUser() {
    createUser("user1", "TimechoDB@2021");

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "alter user user1 set password 'TimechoDB@2022'",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "alter user user1 set password 'TimechoDB@2022'",
        "803: No permission to update system admin.",
        "security_admin",
        "TimechoDB@2021");
    executeTableNonQuery(
        "alter user user1 set password 'TimechoDB@2022'", "user1", "TimechoDB@2021");
    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "alter user user1 set password 'TimechoDB@2022'",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    executeTableNonQuery(
        "alter user user1 set password 'TimechoDB@2022'", "security_admin", "TimechoDB@2021");
    executeTableNonQuery(
        "alter user user1 set password 'TimechoDB@2021'", "user1", "TimechoDB@2022");
  }

  @Test
  public void renameUser() {
    createUser("user1", "TimechoDB@2021");

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "alter user user1 rename to user2",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "alter user user1 rename to user2",
        "803: No permission to update system admin.",
        "security_admin",
        "TimechoDB@2021");
    executeTableNonQuery("alter user user1 rename to user2", "user1", "TimechoDB@2021");
    executeTableNonQuery(
        "alter user security_admin rename to security_admin1", "security_admin", "TimechoDB@2021");
    executeTableNonQuery(
        "alter user security_admin1 rename to security_admin", "security_admin1", "TimechoDB@2021");
    executeTableNonQuery("revoke system from user user2", "sys_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "alter user user2 rename to user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    executeTableNonQuery("alter user user2 rename to user1", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("alter user user1 rename to user2", "user1", "TimechoDB@2021");
    executeTableNonQuery("alter user user2 rename to user1", "user2", "TimechoDB@2021");
  }

  @Test
  public void dropUser() throws IoTDBConnectionException, StatementExecutionException {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "drop user user1",
        "803: Access Denied: Cannot drop admin user or yourself",
        "user1",
        "TimechoDB@2021");
    executeTableNonQuery("grant security to user user1", "security_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user1", "Cannot drop admin user or yourself", "user1", "TimechoDB@2021");
    executeTableNonQuery("drop user user2", "user1", "TimechoDB@2021");

    createUser("user2", "TimechoDB@2021");
    executeTableNonQuery("grant system to user user2", "sys_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user2", "803: No permission to drop system admin.", "user1", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user2",
        "803: No permission to drop system admin.",
        "security_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user2",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");

    executeTableNonQuery("revoke system from user user2", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("drop user user2", "user1", "TimechoDB@2021");

    createUser("user2", "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user2", "audit_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user2", "803: No permission to drop audit admin.", "user1", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user2",
        "803: No permission to drop audit admin.",
        "security_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "drop user user2",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "audit_admin",
        "TimechoDB@2021");

    executeTableNonQuery("revoke audit from user user2", "audit_admin", "TimechoDB@2021");
    executeTableNonQuery("drop user user2", "user1", "TimechoDB@2021");

    executeTableNonQuery("drop user user1", "security_admin", "TimechoDB@2021");

    createdUsers.clear();
  }

  @Test
  public void listRole() throws IoTDBConnectionException, StatementExecutionException {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");
    createRole("role1");
    createRole("role2");

    executeTableNonQuery("grant role role1 to user1", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant role role2 to user2", "security_admin", "TimechoDB@2021");
    Assert.assertEquals(Collections.singleton("role1"), executeListRoleWithSpecifiedUser("user1"));
    Assert.assertEquals(Collections.singleton("role2"), executeListRoleWithSpecifiedUser("user2"));
    Assert.assertEquals(Collections.emptySet(), executeListRoleWithSpecifiedUser("sys_admin"));
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("role1", "role2")),
        executeListRoleWithSpecifiedUser("security_admin"));
    Assert.assertEquals(Collections.emptySet(), executeListRoleWithSpecifiedUser("audit_admin"));

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user2", "audit_admin", "TimechoDB@2021");
    Assert.assertEquals(Collections.singleton("role1"), executeListRoleWithSpecifiedUser("user1"));
    Assert.assertEquals(Collections.singleton("role2"), executeListRoleWithSpecifiedUser("user2"));
    Assert.assertEquals(
        Collections.singleton("role1"), executeListRoleWithSpecifiedUser("sys_admin"));
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("role1", "role2")),
        executeListRoleWithSpecifiedUser("security_admin"));
    Assert.assertEquals(
        Collections.singleton("role2"), executeListRoleWithSpecifiedUser("audit_admin"));

    executeTableNonQuery("revoke audit from user user2", "audit_admin", "TimechoDB@2021");
    executeTableNonQuery("grant system to user user2", "sys_admin", "TimechoDB@2021");

    Assert.assertEquals(Collections.singleton("role1"), executeListRoleWithSpecifiedUser("user1"));
    Assert.assertEquals(Collections.singleton("role2"), executeListRoleWithSpecifiedUser("user2"));

    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("revoke system from user user2", "sys_admin", "TimechoDB@2021");
  }

  @Test
  public void listRoleOfUser() throws IoTDBConnectionException, StatementExecutionException {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");
    createRole("role1");
    createRole("role2");

    executeTableNonQuery("grant role role1 to user1", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant role role2 to user2", "security_admin", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "list role of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    Assert.assertEquals(
        Collections.singleton("role1"),
        executeListRoleOfUserWithSpecifiedUser("user1", "security_admin"));
    Assert.assertEquals(
        Collections.singleton("role2"),
        executeListRoleOfUserWithSpecifiedUser("user2", "security_admin"));
    assertTableNonQueryTestFail(
        "list role of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "audit_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "list role of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user2",
        "TimechoDB@2021");

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user2", "audit_admin", "TimechoDB@2021");
    Assert.assertEquals(
        Collections.singleton("role1"),
        executeListRoleOfUserWithSpecifiedUser("user1", "sys_admin"));
    Assert.assertEquals(
        Collections.singleton("role2"),
        executeListRoleOfUserWithSpecifiedUser("user2", "audit_admin"));
    Assert.assertEquals(
        Collections.singleton("role2"),
        executeListRoleOfUserWithSpecifiedUser("user2", "security_admin"));
    assertTableNonQueryTestFail(
        "list role of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user2",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "list role of user user2",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user1",
        "TimechoDB@2021");
    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("revoke audit from user user2", "audit_admin", "TimechoDB@2021");
  }

  @Test
  public void grantAndRevokeAdminPowerToUser() {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");
    createUser("user3", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "grant system to user user1 with grant option",
        "803: Access Denied: Admin privileges do not support grant options when separation of admin power is enabled.",
        "sys_admin",
        "TimechoDB@2021");
    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant security to user user1",
        "803: When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.",
        "security_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant audit to user user1",
        "803: When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.",
        "audit_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "revoke system from user user1",
        "803: Access Denied: Only the builtin admin can grant/revoke admin permissions",
        "audit_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "revoke system from user user1",
        "803: Access Denied: Only the builtin admin can grant/revoke admin permissions",
        "security_admin",
        "TimechoDB@2021");
    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "grant security to user user2 with grant option",
        "803: Access Denied: Admin privileges do not support grant options when separation of admin power is enabled.",
        "security_admin",
        "TimechoDB@2021");
    executeTableNonQuery("grant security to user user2", "security_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant system to user user2",
        "803: When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant audit to user user2",
        "803: When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.",
        "audit_admin",
        "TimechoDB@2021");
    executeTableNonQuery("revoke security from user user2", "security_admin", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "grant audit to user user3 with grant option",
        "803: Access Denied: Admin privileges do not support grant options when separation of admin power is enabled.",
        "audit_admin",
        "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user3", "audit_admin", "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant system to user user3",
        "803: When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant security to user user3",
        "803: When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.",
        "security_admin",
        "TimechoDB@2021");
    executeTableNonQuery("revoke audit from user user3", "audit_admin", "TimechoDB@2021");
  }

  @Test
  public void grantAndRevokeNormalPowerToUser() {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");

    executeTableNonQuery(
        "grant select on any to user user1 with grant option", "security_admin", "TimechoDB@2021");
    executeTableNonQuery(
        "grant select on any to user user2 with grant option", "user1", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "revoke select on any from user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user2",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "revoke select on any from user user2",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user1",
        "TimechoDB@2021");
    executeTableNonQuery(
        "revoke select on any from user user1", "security_admin", "TimechoDB@2021");
  }

  @Test
  public void grantAdminPowerToRole() {
    createRole("role1");
    assertTableNonQueryTestFail(
        "grant system to role role1",
        "803: Access Denied: Grant admin privileges to roles is not allowed when separation of admin powers is enabled",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant security to role role1",
        "803: Access Denied: Grant admin privileges to roles is not allowed when separation of admin powers is enabled",
        "security_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "grant audit to role role1",
        "803: Access Denied: Grant admin privileges to roles is not allowed when separation of admin powers is enabled",
        "audit_admin",
        "TimechoDB@2021");
  }

  @Test
  public void sysAdminCanOperateWritableView()
      throws IoTDBConnectionException, StatementExecutionException {
    final String database = "strict_system_writable_view_db";
    final String setupUser = "writable_view_setup_user";
    createUser(setupUser, "TimechoDB@2021");
    executeTableNonQuery(
        "grant all on any to user " + setupUser, "security_admin", "TimechoDB@2021");
    try (ITableSession setupSession =
            EnvFactory.getEnv().getTableSessionConnection(setupUser, "TimechoDB@2021");
        ITableSession sysAdminSession =
            EnvFactory.getEnv().getTableSessionConnection("sys_admin", "TimechoDB@2021")) {
      try {
        setupSession.executeNonQueryStatement("drop database if exists " + database);
        setupSession.executeNonQueryStatement("create database " + database);
        setupSession.executeNonQueryStatement("use " + database);
        setupSession.executeNonQueryStatement(
            "create table source_table(" + "device_id string tag, " + "temperature int32 field)");
        setupSession.executeNonQueryStatement(
            "create writable view writable_view as select "
                + "device_id as dev, temperature as temp from source_table");

        sysAdminSession.executeNonQueryStatement("use " + database);
        sysAdminSession.executeNonQueryStatement(
            "insert into writable_view(time, dev, temp) values (1, 'd1', 10), (2, 'd1', 11)");

        try (SessionDataSet dataSet =
            sysAdminSession.executeQueryStatement("select temp from writable_view order by time")) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          Assert.assertTrue(iterator.next());
          Assert.assertEquals(10, iterator.getInt("temp"));
          Assert.assertTrue(iterator.next());
          Assert.assertEquals(11, iterator.getInt("temp"));
          Assert.assertFalse(iterator.next());
        }

        sysAdminSession.executeNonQueryStatement("delete from writable_view where time = 1");

        try (SessionDataSet dataSet =
            sysAdminSession.executeQueryStatement("select temp from writable_view order by time")) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          Assert.assertTrue(iterator.next());
          Assert.assertEquals(11, iterator.getInt("temp"));
          Assert.assertFalse(iterator.next());
        }
      } finally {
        setupSession.executeNonQueryStatement("drop database if exists " + database);
      }
    }
  }

  @Test
  public void listPrivilegesOfUser() throws IoTDBConnectionException, StatementExecutionException {
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "list privileges of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "list privileges of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user2",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "list privileges of user user1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "audit_admin",
        "TimechoDB@2021");
    Assert.assertEquals(
        Collections.emptySet(), executeListPrivilegesOfUserWithSpecifiedUser("user1", "user1"));
    Assert.assertEquals(
        Collections.emptySet(),
        executeListPrivilegesOfUserWithSpecifiedUser("user1", "security_admin"));

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user2", "audit_admin", "TimechoDB@2021");
    Assert.assertEquals(
        Collections.singleton("SYSTEM"),
        executeListPrivilegesOfUserWithSpecifiedUser("user1", "user1"));
    Assert.assertEquals(
        Collections.singleton("AUDIT"),
        executeListPrivilegesOfUserWithSpecifiedUser("user2", "user2"));
    Assert.assertEquals(
        Collections.singleton("SYSTEM"),
        executeListPrivilegesOfUserWithSpecifiedUser("user1", "sys_admin"));
    Assert.assertEquals(
        Collections.singleton("AUDIT"),
        executeListPrivilegesOfUserWithSpecifiedUser("user2", "audit_admin"));
    Assert.assertEquals(
        Collections.singleton("SYSTEM"),
        executeListPrivilegesOfUserWithSpecifiedUser("user1", "security_admin"));
    Assert.assertEquals(
        Collections.singleton("AUDIT"),
        executeListPrivilegesOfUserWithSpecifiedUser("user2", "security_admin"));
    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("revoke audit from user user2", "audit_admin", "TimechoDB@2021");
  }

  @Test
  public void listPrivilegesOfRole() throws IoTDBConnectionException, StatementExecutionException {
    createRole("role1");
    createRole("role2");
    createUser("user1", "TimechoDB@2021");
    createUser("user2", "TimechoDB@2021");

    assertTableNonQueryTestFail(
        "list privileges of role role1",
        "803: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "list privileges of role role1",
        "803: Access Denied: No permissions for this operation, please add privilege SECURITY",
        "user1",
        "TimechoDB@2021");
    assertTableNonQueryTestFail(
        "list privileges of role role1",
        "803: No permissions for this operation, please add privilege SECURITY",
        "audit_admin",
        "TimechoDB@2021");

    executeTableNonQuery("grant system to user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("grant audit to user user2", "audit_admin", "TimechoDB@2021");
    executeTableNonQuery("grant role role1 to user1", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant role role2 to user2", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant select on any to role role1", "security_admin", "TimechoDB@2021");
    executeTableNonQuery("grant select on any to role role2", "security_admin", "TimechoDB@2021");

    Assert.assertEquals(
        Collections.singleton("SELECT"),
        executeListPrivilegesOfRoleWithSpecifiedUser("role1", "user1"));
    Assert.assertEquals(
        Collections.singleton("SELECT"),
        executeListPrivilegesOfRoleWithSpecifiedUser("role1", "sys_admin"));
    assertTableNonQueryTestFail(
        "list privileges of role role1",
        "803: No permissions for this operation, please add privilege SECURITY",
        "audit_admin",
        "TimechoDB@2021");
    Assert.assertEquals(
        Collections.singleton("SELECT"),
        executeListPrivilegesOfRoleWithSpecifiedUser("role1", "security_admin"));
    Assert.assertEquals(
        Collections.singleton("SELECT"),
        executeListPrivilegesOfRoleWithSpecifiedUser("role2", "user2"));
    Assert.assertEquals(
        Collections.singleton("SELECT"),
        executeListPrivilegesOfRoleWithSpecifiedUser("role2", "audit_admin"));
    Assert.assertEquals(
        Collections.singleton("SELECT"),
        executeListPrivilegesOfRoleWithSpecifiedUser("role2", "security_admin"));
    assertTableNonQueryTestFail(
        "list privileges of role role2",
        "803: No permissions for this operation, please add privilege SECURITY",
        "sys_admin",
        "TimechoDB@2021");

    executeTableNonQuery("revoke system from user user1", "sys_admin", "TimechoDB@2021");
    executeTableNonQuery("revoke audit from user user2", "audit_admin", "TimechoDB@2021");
  }

  private Set<String> executeListUserWithSpecifiedUser(String executedByUsername)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> users = new HashSet<>();
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection(executedByUsername, "TimechoDB@2021")) {
      SessionDataSet sessionDataSet = session.executeQueryStatement("list user");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString("User");
        users.add(name);
      }
    }
    return users;
  }

  private Set<String> executeListUserOfRoleWithSpecifiedUser(String role, String executedByUsername)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> users = new HashSet<>();
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection(executedByUsername, "TimechoDB@2021")) {
      SessionDataSet sessionDataSet = session.executeQueryStatement("list user of role " + role);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString("User");
        users.add(name);
      }
    }
    return users;
  }

  private Set<String> executeListRoleWithSpecifiedUser(String executedByUsername)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> roles = new HashSet<>();
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection(executedByUsername, "TimechoDB@2021")) {
      SessionDataSet sessionDataSet = session.executeQueryStatement("list role");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString("Role");
        roles.add(name);
      }
    }
    return roles;
  }

  private Set<String> executeListRoleOfUserWithSpecifiedUser(
      String username, String executedByUsername)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> roles = new HashSet<>();
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection(executedByUsername, "TimechoDB@2021")) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("list role of user " + username);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString("Role");
        roles.add(name);
      }
    }
    return roles;
  }

  private Set<String> executeListPrivilegesOfUserWithSpecifiedUser(
      String username, String executedByUsername)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> privileges = new HashSet<>();
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection(executedByUsername, "TimechoDB@2021")) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("list privileges of user " + username);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString("Privileges");
        privileges.add(name);
      }
    }
    return privileges;
  }

  private Set<String> executeListPrivilegesOfRoleWithSpecifiedUser(
      String role, String executedByUsername)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> privileges = new HashSet<>();
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection(executedByUsername, "TimechoDB@2021")) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("list privileges of role " + role);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString("Privileges");
        privileges.add(name);
      }
    }
    return privileges;
  }

  private void createUser(String userName, String password) {
    executeTableNonQuery(
        "create user " + userName + " '" + password + "'", "security_admin", "TimechoDB@2021");
    createdUsers.add(userName);
  }

  private void createRole(String roleName) {
    executeTableNonQuery("create role " + roleName, "security_admin", "TimechoDB@2021");
    createdRoles.add(roleName);
  }

  private void dropUser(String userName) {
    try {
      executeTableNonQuery("drop user " + userName, "security_admin", "TimechoDB@2021");
    } catch (Throwable ignored) {
    }
  }

  private void dropRole(String roleName) {
    try {
      executeTableNonQuery("drop role " + roleName, "security_admin", "TimechoDB@2021");
    } catch (Throwable ignored) {
    }
  }
}
