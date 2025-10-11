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
package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalFileAuthorizerTest {

  IAuthorizer authorizer;
  PartialPath nodeName;
  static final String roleName = "role";
  static final String userName = "user";
  static final String password = "password123456";
  static final String database = "testDB";
  static final String table = "testTB";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    authorizer = BasicAuthorizer.getInstance();
    authorizer.reset();
    nodeName = new PartialPath("root.laptop.d1");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testLogin() throws AuthException {
    Assert.assertTrue(authorizer.login("root", "root"));
    Assert.assertThrows(AuthException.class, () -> authorizer.login("root", "error"));
  }

  @Test
  public void createAndDeleteUser() throws AuthException {
    authorizer.createUser(userName, password);
    try {
      authorizer.createUser(userName, password);
    } catch (AuthException e) {
      assertEquals("User user already exists", e.getMessage());
    }
    Assert.assertTrue(authorizer.login(userName, password));
    authorizer.deleteUser(userName);
    try {
      authorizer.deleteUser(userName);
    } catch (AuthException e) {
      assertEquals("User user does not exist", e.getMessage());
    }

    try {
      authorizer.deleteUser("root");
    } catch (AuthException e) {
      assertEquals("Default administrator cannot be deleted", e.getMessage());
    }
    try {
      authorizer.deleteUser("nouser");
    } catch (AuthException e) {
      assertEquals("User nouser does not exist", e.getMessage());
    }
  }

  @Test
  public void createAndDeleteRole() throws AuthException {
    authorizer.createRole(roleName);
    try {
      authorizer.createRole(roleName);
    } catch (AuthException e) {
      assertEquals("Role role already exists", e.getMessage());
    }
    authorizer.createUser(userName, "password123456");
    authorizer.grantRoleToUser(roleName, userName);
    authorizer.deleteRole(roleName);
    try {
      authorizer.deleteRole(roleName);
    } catch (AuthException e) {
      assertEquals("Role role does not exist", e.getMessage());
    }
    assertEquals(0, authorizer.getUser(userName).getRoleSet().size());
  }

  @Test
  public void testTreePermission() throws AuthException {
    authorizer.createUser(userName, password);
    authorizer.grantPrivilegeToUser(
        userName, new PrivilegeUnion(nodeName, PrivilegeType.READ_DATA, false));
    try {
      authorizer.grantPrivilegeToUser(
          userName, new PrivilegeUnion(nodeName, PrivilegeType.READ_DATA, false));
    } catch (AuthException e) {
      assertEquals("User user already has READ_DATA on root.laptop.d1", e.getMessage());
    }
    try {
      authorizer.grantPrivilegeToUser(
          "error", new PrivilegeUnion(nodeName, PrivilegeType.READ_DATA, false));
    } catch (AuthException e) {
      assertEquals("User error does not exist", e.getMessage());
    }

    try {
      authorizer.grantPrivilegeToUser(
          "root", new PrivilegeUnion(nodeName, PrivilegeType.READ_DATA, false));
    } catch (AuthException e) {
      Assert.assertEquals(
          "Invalid operation, administrator already has all privileges", e.getMessage());
    }

    authorizer.revokePrivilegeFromUser(
        userName, new PrivilegeUnion(nodeName, PrivilegeType.READ_DATA));
    authorizer.revokePrivilegeFromUser(
        userName, new PrivilegeUnion(nodeName, PrivilegeType.READ_DATA));
  }

  @Test
  public void testRelationalPermission() throws AuthException {
    authorizer.createUser(userName, password);
    authorizer.grantPrivilegeToUser(
        userName, new PrivilegeUnion(database, PrivilegeType.SELECT, true));
    authorizer.grantPrivilegeToUser(
        userName, new PrivilegeUnion(database, PrivilegeType.ALTER, false));
    authorizer.grantPrivilegeToUser(
        userName, new PrivilegeUnion(database, table, PrivilegeType.INSERT, true));
    authorizer.grantPrivilegeToUser(
        userName, new PrivilegeUnion(database, table, PrivilegeType.DELETE, true));
    assertEquals(1, authorizer.getUser(userName).getDBScopePrivilegeMap().size());
    assertEquals(
        1,
        authorizer
            .getUser(userName)
            .getDBScopePrivilegeMap()
            .get(database)
            .getTablePrivilegeMap()
            .size());

    assertTrue(
        authorizer.checkUserPrivileges(
            userName, new PrivilegeUnion(database, PrivilegeType.SELECT)));
    assertTrue(
        authorizer.checkUserPrivileges(
            userName, new PrivilegeUnion(database, table, PrivilegeType.SELECT))); // db privilege
    assertTrue(
        authorizer.checkUserPrivileges(
            userName, new PrivilegeUnion(database, table, PrivilegeType.DELETE)));
    assertTrue(
        authorizer.checkUserPrivileges(
            userName, new PrivilegeUnion(database, table, PrivilegeType.INSERT, true)));
  }

  @Test
  public void testUserRole() throws AuthException {
    authorizer.createUser(userName, password);
    authorizer.createRole(roleName);
    authorizer.grantRoleToUser(roleName, userName);
    authorizer.grantPrivilegeToUser(
        userName, new PrivilegeUnion(nodeName, PrivilegeType.WRITE_DATA, false));
    authorizer.grantPrivilegeToRole(
        roleName, new PrivilegeUnion(nodeName, PrivilegeType.WRITE_SCHEMA, false));

    // a user can get all role permissions.
    Set<PrivilegeType> permissions = authorizer.getPrivileges(userName, nodeName);
    assertEquals(2, permissions.size());
    assertTrue(permissions.contains(PrivilegeType.WRITE_DATA));
    assertTrue(permissions.contains(PrivilegeType.WRITE_SCHEMA));
    assertFalse(permissions.contains(PrivilegeType.READ_DATA));

    try {
      authorizer.grantRoleToUser(roleName, userName);
    } catch (AuthException e) {
      Assert.assertEquals("User user already has role role", e.getMessage());
    }
    // revoke a role from a user, the user will lose all role's permission
    authorizer.revokeRoleFromUser(roleName, userName);
    Set<PrivilegeType> revokeRolePermissions = authorizer.getPrivileges(userName, nodeName);
    assertEquals(1, revokeRolePermissions.size());
    assertTrue(revokeRolePermissions.contains(PrivilegeType.WRITE_DATA));
    assertFalse(revokeRolePermissions.contains(PrivilegeType.READ_SCHEMA));

    // check the users' permission again
    Assert.assertTrue(
        authorizer.checkUserPrivileges(
            userName, new PrivilegeUnion(nodeName, PrivilegeType.WRITE_DATA)));
    Assert.assertFalse(
        authorizer.checkUserPrivileges(
            userName, new PrivilegeUnion(nodeName, PrivilegeType.READ_SCHEMA)));

    try {
      authorizer.grantRoleToUser("role1", userName);
    } catch (AuthException e) {
      Assert.assertEquals("No such role : role1", e.getMessage());
    }
  }

  @Test
  public void testUpdatePassword() throws AuthException {
    authorizer.createUser(userName, password);
    authorizer.updateUserPassword(userName, "newPassword123456");
    Assert.assertTrue(authorizer.login(userName, "newPassword123456"));
  }

  @Test
  public void testGetAllUsersAndRoles() throws AuthException {
    authorizer.createUser("user0", "user123456789");
    authorizer.createUser("user1", "user1123456789");
    authorizer.createUser("user2", "user2123456789");
    authorizer.createRole("role0");
    authorizer.createRole("role1");
    authorizer.createRole("role2");
    Assert.assertEquals(4, authorizer.getAllUsers().size());
    Assert.assertEquals(3, authorizer.getAllRoles().size());
  }

  @Test
  public void testListUser() throws AuthException {
    IAuthorizer authorizer = BasicAuthorizer.getInstance();
    List<String> userList = authorizer.listAllUsers();
    assertEquals(1, userList.size());
    assertEquals(CommonDescriptor.getInstance().getConfig().getDefaultAdminName(), userList.get(0));

    int userCnt = 10;
    for (int i = 0; i < userCnt; i++) {
      authorizer.createUser("newUser" + i, "password123456" + i);
    }
    userList = authorizer.listAllUsers();
    assertEquals(11, userList.size());
    for (int i = 0; i < userCnt; i++) {
      assertEquals("newUser" + i, userList.get(i));
    }

    for (int i = 0; i < userCnt; i++) {
      if (i % 2 == 0) {
        authorizer.deleteUser("newUser" + i);
      }
    }
    userList = authorizer.listAllUsers();
    assertEquals(6, userList.size());
    for (int i = 0; i < userCnt; i++) {
      if (i % 2 == 1) {
        assertEquals("newUser" + i, userList.get(i / 2));
      }
    }
  }

  @Test
  public void testListRole() throws AuthException {
    IAuthorizer authorizer = BasicAuthorizer.getInstance();
    List<String> roleList = authorizer.listAllRoles();
    assertEquals(0, roleList.size());

    int roleCnt = 10;
    for (int i = 0; i < roleCnt; i++) {
      authorizer.createRole("newRole" + i);
    }
    roleList = authorizer.listAllRoles();
    assertEquals(10, roleList.size());
    for (int i = 0; i < roleCnt; i++) {
      assertEquals("newRole" + i, roleList.get(i));
    }

    for (int i = 0; i < roleCnt; i++) {
      if (i % 2 == 0) {
        authorizer.deleteRole("newRole" + i);
      }
    }
    roleList = authorizer.listAllRoles();
    assertEquals(5, roleList.size());
    for (int i = 0; i < roleCnt; i++) {
      if (i % 2 == 1) {
        assertEquals("newRole" + i, roleList.get(i / 2));
      }
    }
  }
}
