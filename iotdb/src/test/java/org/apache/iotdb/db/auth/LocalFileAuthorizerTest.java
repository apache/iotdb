/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalFileAuthorizerTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAuthorizer() throws AuthException {

    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
    /*
     * login
     */
    boolean status = false;
    try {
      status = authorizer.login("root", "root");
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      status = authorizer.login("root", "error");
    } catch (AuthException e) {
      assertEquals("The username or the password is not correct", e.getMessage());
    }
    /*
     * create user,delete user
     */
    User user = new User("user", "password");
    try {
      status = authorizer.createUser(user.name, user.password);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.createUser(user.name, user.password);
    } catch (AuthException e) {
      assertEquals("The user is exist", e.getMessage());
    }
    try {
      status = authorizer.login(user.name, user.password);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.deleteUser(user.name);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.deleteUser(user.name);
    } catch (AuthException e) {
      assertEquals("The user is not exist", e.getMessage());
    }

    /*
     * permission for user
     */
    String nodeName = "root.laptop.d1";
    try {
      authorizer.createUser(user.name, user.password);
      status = authorizer.grantPrivilegeToUser(user.name, nodeName, 1);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      authorizer.grantPrivilegeToUser(user.name, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("The permission is exist", e.getMessage());
    }
    try {
      authorizer.grantPrivilegeToUser("error", nodeName, 1);
    } catch (AuthException e) {
      assertEquals("No such user error", e.getMessage());
    }
    try {
      status = authorizer.revokePrivilegeFromUser(user.name, nodeName, 1);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.revokePrivilegeFromUser(user.name, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("The permission is not exist", e.getMessage());
    }
    try {
      authorizer.deleteUser(user.name);
      authorizer.revokePrivilegeFromUser(user.name, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("No such user user", e.getMessage());
    }
    /*
     * role
     */
    String roleName = "role";
    try {
      status = authorizer.createRole(roleName);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.createRole(roleName);
    } catch (AuthException e) {
      assertEquals("The role is exist", e.getMessage());
    }

    try {
      status = authorizer.deleteRole(roleName);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.deleteRole(roleName);
    } catch (AuthException e) {
      assertEquals("The role is not exist", e.getMessage());
    }
    /*
     * role permission
     */
    try {
      status = authorizer.createRole(roleName);
      status = authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      status = authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("The permission is exist", e.getMessage());
    }

    try {
      status = authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
      assertEquals(true, status);
    } catch (AuthException e1) {
      fail(e1.getMessage());
    }
    try {
      authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
    } catch (AuthException e) {
      assertEquals("The permission is not exist", e.getMessage());
    }

    try {
      authorizer.deleteRole(roleName);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

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

    /*
     * user role
     */
    try {
      authorizer.createUser(user.name, user.password);
      authorizer.createRole(roleName);
      status = authorizer.grantRoleToUser(roleName, user.name);
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      authorizer.grantPrivilegeToUser(user.name, nodeName, 1);
      authorizer.grantPrivilegeToRole(roleName, nodeName, 2);
      authorizer.grantPrivilegeToRole(roleName, nodeName, 3);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      Set<Integer> permisssions = authorizer.getPrivileges(user.name, nodeName);
      assertEquals(3, permisssions.size());
      assertEquals(true, permisssions.contains(1));
      assertEquals(true, permisssions.contains(2));
      assertEquals(true, permisssions.contains(3));
      assertEquals(false, permisssions.contains(4));
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.revokeRoleFromUser(roleName, user.name);
      assertEquals(true, status);
      Set<Integer> permisssions = authorizer.getPrivileges(user.name, nodeName);
      assertEquals(1, permisssions.size());
      assertEquals(true, permisssions.contains(1));
      assertEquals(false, permisssions.contains(2));
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      status = authorizer.checkUserPrivileges(user.name, nodeName, 1);
    } catch (AuthException e) {
      fail(e.getMessage());
    }
    assertEquals(true, status);
    try {
      status = authorizer.checkUserPrivileges(user.name, nodeName, 2);
    } catch (AuthException e) {
      fail(e.getMessage());
    }
    assertEquals(false, status);
    try {
      status = authorizer.updateUserPassword(user.name, "newPassword");
      assertEquals(true, status);
      status = authorizer.login(user.name, "newPassword");
      assertEquals(true, status);
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      authorizer.deleteUser(user.name);
      authorizer.deleteRole(roleName);
    } catch (AuthException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testListUser() throws AuthException {
    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
    List<String> userList = authorizer.listAllUsers();
    assertEquals(1, userList.size());
    assertEquals(IoTDBConstant.ADMIN_NAME, userList.get(0));

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
    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
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
