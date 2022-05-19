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
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalFileAuthorizerTest {

  IAuthorizer authorizer;
  User user;
  String nodeName = "root.laptop.d1";
  String roleName = "role";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    authorizer = AuthorizerManager.getInstance();
    user = new User("user", "password");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testLogin() throws AuthException {
    Assert.assertTrue(authorizer.login("root", "root"));
    Assert.assertFalse(authorizer.login("root", "error"));
  }

  @Test
  public void createAndDeleteUser() throws AuthException {
    authorizer.createUser(user.getName(), user.getPassword());
    try {
      authorizer.createUser(user.getName(), user.getPassword());
    } catch (AuthException e) {
      assertEquals("User user already exists", e.getMessage());
    }
    Assert.assertTrue(authorizer.login(user.getName(), user.getPassword()));
    authorizer.deleteUser(user.getName());
    try {
      authorizer.deleteUser(user.getName());
    } catch (AuthException e) {
      assertEquals("User user does not exist", e.getMessage());
    }

    try {
      authorizer.deleteUser("root");
    } catch (AuthException e) {
      assertEquals("Default administrator cannot be deleted", e.getMessage());
    }
  }

  @Test
  public void testUserPermission() throws AuthException {
    authorizer.createUser(user.getName(), user.getPassword());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, 1);
    try {
      authorizer.grantPrivilegeToUser(user.getName(), nodeName, 1);
    } catch (AuthException e) {
      assertEquals("User user already has INSERT_TIMESERIES on root.laptop.d1", e.getMessage());
    }
    try {
      authorizer.grantPrivilegeToUser("error", nodeName, 1);
    } catch (AuthException e) {
      assertEquals("No such user error", e.getMessage());
    }

    try {
      authorizer.grantPrivilegeToUser("root", nodeName, 1);
    } catch (AuthException e) {
      Assert.assertEquals(
          "Invalid operation, administrator already has all privileges", e.getMessage());
    }

    try {
      authorizer.grantPrivilegeToUser(user.getName(), nodeName, 100);
    } catch (AuthException e) {
      assertEquals("Invalid privilegeId 100", e.getMessage());
    }

    authorizer.revokePrivilegeFromUser(user.getName(), nodeName, 1);
    try {
      authorizer.revokePrivilegeFromUser(user.getName(), nodeName, 1);
    } catch (AuthException e) {
      assertEquals("User user does not have INSERT_TIMESERIES on root.laptop.d1", e.getMessage());
    }

    try {
      authorizer.revokePrivilegeFromUser(user.getName(), nodeName, 100);
    } catch (AuthException e) {
      assertEquals("Invalid privilegeId 100", e.getMessage());
    }

    try {
      authorizer.deleteUser(user.getName());
      authorizer.revokePrivilegeFromUser(user.getName(), nodeName, 1);
    } catch (AuthException e) {
      assertEquals("No such user user", e.getMessage());
    }

    try {
      authorizer.revokePrivilegeFromUser("root", "root", 1);
    } catch (AuthException e) {
      Assert.assertEquals(
          "Invalid operation, administrator must have all privileges", e.getMessage());
    }
  }

  @Test
  public void testCreateAndDeleteRole() throws AuthException {
    authorizer.createRole(roleName);
    try {
      authorizer.createRole(roleName);
    } catch (AuthException e) {
      assertEquals("Role role already exists", e.getMessage());
    }
    authorizer.deleteRole(roleName);
    try {
      authorizer.deleteRole(roleName);
    } catch (AuthException e) {
      assertEquals("Role role does not exist", e.getMessage());
    }
  }

  @Test
  public void testRolePermission() throws AuthException {
    authorizer.createRole(roleName);
    authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
    try {
      authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("Role role already has INSERT_TIMESERIES on root.laptop.d1", e.getMessage());
    }
    authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
    try {
      authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("Role role does not have INSERT_TIMESERIES on root.laptop.d1", e.getMessage());
    }
    authorizer.deleteRole(roleName);
    try {
      authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("No such role role", e.getMessage());
    }
    try {
      authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("No such role role", e.getMessage());
    }
  }

  @Test
  public void testUserRole() throws AuthException {
    authorizer.createUser(user.getName(), user.getPassword());
    authorizer.createRole(roleName);
    authorizer.grantRoleToUser(roleName, user.getName());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, 1);
    authorizer.grantPrivilegeToRole(roleName, nodeName, 3);

    // a user can get all role permissions.
    Set<Integer> permissions = authorizer.getPrivileges(user.getName(), nodeName);
    assertEquals(2, permissions.size());
    assertTrue(permissions.contains(1));
    assertTrue(permissions.contains(3));
    assertFalse(permissions.contains(2));

    try {
      authorizer.grantRoleToUser(roleName, user.getName());
    } catch (AuthException e) {
      Assert.assertEquals("User user already has role role", e.getMessage());
    }
    // revoke a role from a user, the user will lose all role's permission
    authorizer.revokeRoleFromUser(roleName, user.getName());
    Set<Integer> revokeRolePermissions = authorizer.getPrivileges(user.getName(), nodeName);
    assertEquals(1, revokeRolePermissions.size());
    assertTrue(revokeRolePermissions.contains(1));
    assertFalse(revokeRolePermissions.contains(2));

    // check the users' permission again
    Assert.assertTrue(authorizer.checkUserPrivileges(user.getName(), nodeName, 1));
    Assert.assertFalse(authorizer.checkUserPrivileges(user.getName(), nodeName, 2));

    try {
      authorizer.grantRoleToUser("role1", user.getName());
    } catch (AuthException e) {
      Assert.assertEquals("No such role : role1", e.getMessage());
    }
  }

  @Test
  public void testUpdatePassword() throws AuthException {
    authorizer.createUser(user.getName(), user.getPassword());
    authorizer.updateUserPassword(user.getName(), "newPassword");
    Assert.assertTrue(authorizer.login(user.getName(), "newPassword"));
  }

  @Test
  public void testUserWaterMark() throws AuthException {
    authorizer.setUserUseWaterMark("root", true);
    assertTrue(authorizer.getAllUserWaterMarkStatus().get("root"));
    Assert.assertTrue(authorizer.isUserUseWaterMark("root"));
  }

  @Test
  public void testGetAllUsersAndRoles() throws AuthException {
    authorizer.createUser("user0", "user");
    authorizer.createUser("user1", "user1");
    authorizer.createUser("user2", "user2");
    authorizer.createRole("role0");
    authorizer.createRole("role1");
    authorizer.createRole("role2");
    Assert.assertEquals(4, authorizer.getAllUsers().size());
    Assert.assertEquals(3, authorizer.getAllRoles().size());
  }

  @Test
  public void testListUser() throws AuthException {
    IAuthorizer authorizer = AuthorizerManager.getInstance();
    List<String> userList = authorizer.listAllUsers();
    assertEquals(1, userList.size());
    assertEquals(CommonDescriptor.getInstance().getConfig().getAdminName(), userList.get(0));

    int userCnt = 10;
    for (int i = 0; i < userCnt; i++) {
      authorizer.createUser("newUser" + i, "password" + i);
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
    IAuthorizer authorizer = AuthorizerManager.getInstance();
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

  @Test
  public void testReplaceAllUsers() throws AuthException {
    IAuthorizer authorizer = AuthorizerManager.getInstance();
    Assert.assertEquals("root", authorizer.listAllUsers().get(0));
    User user = new User("user", "user");
    HashMap<String, User> users = new HashMap<>();
    users.put("user", user);
    authorizer.replaceAllUsers(users);
    Assert.assertEquals("user", authorizer.listAllUsers().get(1));
  }

  @Test
  public void testReplaceAllRole() throws AuthException {
    IAuthorizer authorizer = AuthorizerManager.getInstance();
    Role role = new Role("role");
    HashMap<String, Role> roles = new HashMap<>();
    roles.put("role", role);
    authorizer.replaceAllRoles(roles);
    Assert.assertEquals("role", authorizer.listAllRoles().get(0));
  }
}
