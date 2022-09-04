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
package org.apache.iotdb.db.auth.user;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.AuthUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LocalFileUserManagerTest {

  private File testFolder;
  private LocalFileUserManager manager;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    manager = new LocalFileUserManager(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testIllegalInput() throws AuthException {
    // Password contains space
    try {
      manager.createUser("username1", "password_ ");
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
    // Username contains space
    try {
      assertFalse(manager.createUser("username 2", "password_"));
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
  }

  @Test
  public void test() throws AuthException {
    User[] users = new User[5];
    for (int i = 0; i < users.length; i++) {
      users[i] = new User("user" + i, "password" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege("root.a.b.c" + j);
        pathPrivilege.getPrivileges().add(j);
        users[i].getPrivilegeList().add(pathPrivilege);
        users[i].getRoleList().add("role" + j);
      }
    }

    // create
    User user = manager.getUser(users[0].getName());
    assertNull(user);
    for (User user1 : users) {
      assertTrue(manager.createUser(user1.getName(), user1.getPassword()));
    }
    for (User user1 : users) {
      user = manager.getUser(user1.getName());
      assertEquals(user1.getName(), user.getName());
      assertEquals(AuthUtils.encryptPassword(user1.getPassword()), user.getPassword());
    }

    assertFalse(manager.createUser(users[0].getName(), users[0].getPassword()));
    boolean caught = false;
    try {
      manager.createUser("too", "short");
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);
    caught = false;
    try {
      manager.createUser("short", "too");
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // delete
    assertFalse(manager.deleteUser("not a user"));
    assertTrue(manager.deleteUser(users[users.length - 1].getName()));
    assertNull(manager.getUser(users[users.length - 1].getName()));
    assertFalse(manager.deleteUser(users[users.length - 1].getName()));

    // grant privilege
    user = manager.getUser(users[0].getName());
    String path = "root.a.b.c";
    int privilegeId = 0;
    assertFalse(user.hasPrivilege(path, privilegeId));
    assertTrue(manager.grantPrivilegeToUser(user.getName(), path, privilegeId));
    assertTrue(manager.grantPrivilegeToUser(user.getName(), path, privilegeId + 1));
    assertFalse(manager.grantPrivilegeToUser(user.getName(), path, privilegeId));
    user = manager.getUser(users[0].getName());
    assertTrue(user.hasPrivilege(path, privilegeId));
    caught = false;
    try {
      manager.grantPrivilegeToUser("not a user", path, privilegeId);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);
    caught = false;
    try {
      manager.grantPrivilegeToUser(user.getName(), path, -1);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // revoke privilege
    user = manager.getUser(users[0].getName());
    assertTrue(manager.revokePrivilegeFromUser(user.getName(), path, privilegeId));
    assertFalse(manager.revokePrivilegeFromUser(user.getName(), path, privilegeId));
    caught = false;
    try {
      manager.revokePrivilegeFromUser("not a user", path, privilegeId);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);
    caught = false;
    try {
      manager.revokePrivilegeFromUser(user.getName(), path, -1);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // update password
    String newPassword = "newPassword";
    String illegalPW = "new";
    assertTrue(manager.updateUserPassword(user.getName(), newPassword));
    assertFalse(manager.updateUserPassword(user.getName(), illegalPW));
    user = manager.getUser(user.getName());
    assertEquals(AuthUtils.encryptPassword(newPassword), user.getPassword());
    caught = false;
    try {
      manager.updateUserPassword("not a user", newPassword);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // grant role
    String roleName = "newrole";
    assertTrue(manager.grantRoleToUser(roleName, user.getName()));
    assertFalse(manager.grantRoleToUser(roleName, user.getName()));
    user = manager.getUser(user.getName());
    assertTrue(user.hasRole(roleName));
    caught = false;
    try {
      manager.grantRoleToUser("not a user", roleName);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // revoke role
    assertTrue(manager.revokeRoleFromUser(roleName, user.getName()));
    assertFalse(manager.revokeRoleFromUser(roleName, user.getName()));
    user = manager.getUser(user.getName());
    assertFalse(user.hasRole(roleName));
    caught = false;
    try {
      manager.revokeRoleFromUser("not a user", roleName);
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // list users
    List<String> usernames = manager.listAllUsers();
    usernames.sort(null);
    assertEquals(IoTDBDescriptor.getInstance().getConfig().getAdminName(), usernames.get(0));
    for (int i = 0; i < users.length - 1; i++) {
      assertEquals(users[i].getName(), usernames.get(i + 1));
    }
  }
}
