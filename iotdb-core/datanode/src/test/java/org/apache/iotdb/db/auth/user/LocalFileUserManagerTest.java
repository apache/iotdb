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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PriPrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.user.LocalFileUserManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
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
      manager.createUser("username1", "password_ ", true);
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
    // Username contains space
    try {
      assertFalse(manager.createUser("username 2", "password_", true));
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
  }

  @Test
  public void test() throws AuthException, IllegalPathException {
    User[] users = new User[5];
    for (int i = 0; i < users.length; i++) {
      users[i] = new User("user" + i, "password" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.a.b.c" + j));
        pathPrivilege.getPrivileges().add(j);
        users[i].getPathPrivilegeList().add(pathPrivilege);
        users[i].getRoleList().add("role" + j);
      }
    }

    // create
    User user = manager.getUser(users[0].getName());
    assertNull(user);
    for (User user1 : users) {
      assertTrue(manager.createUser(user1.getName(), user1.getPassword(), false));
    }
    for (User user1 : users) {
      user = manager.getUser(user1.getName());
      assertEquals(user1.getName(), user.getName());
      assertTrue(AuthUtils.validatePassword(user1.getPassword(), user.getPassword()));
    }

    assertFalse(manager.createUser(users[0].getName(), users[0].getPassword(), false));

    Assert.assertThrows(AuthException.class, () -> manager.createUser("too", "short", true));
    Assert.assertThrows(AuthException.class, () -> manager.createUser("short", "too", true));

    // delete
    assertFalse(manager.deleteUser("not a user"));
    assertTrue(manager.deleteUser(users[users.length - 1].getName()));
    assertNull(manager.getUser(users[users.length - 1].getName()));
    assertFalse(manager.deleteUser(users[users.length - 1].getName()));

    // grant privilege
    user = manager.getUser(users[0].getName());
    PartialPath path = new PartialPath("root.a.b.c");
    int privilegeId = 0;

    assertFalse(user.hasPrivilegeToRevoke(path, privilegeId));
    assertTrue(manager.grantPrivilegeToUser(user.getName(), path, privilegeId, false));
    assertTrue(manager.grantPrivilegeToUser(user.getName(), path, privilegeId + 1, false));
    // grant again will success
    assertTrue(manager.grantPrivilegeToUser(user.getName(), path, privilegeId, false));
    user = manager.getUser(users[0].getName());
    assertTrue(user.hasPrivilegeToRevoke(path, privilegeId));

    Assert.assertThrows(
        AuthException.class,
        () -> manager.grantPrivilegeToUser("not a user", path, privilegeId, false));
    // We will check the privilegeid before we process it.
    //    Assert.assertThrows(
    //        AuthException.class,
    //        () -> manager.grantPrivilegeToUser(users[0].getName(), path, -1, false));

    // revoke privilege
    user = manager.getUser(users[0].getName());
    assertTrue(manager.revokePrivilegeFromUser(user.getName(), path, privilegeId));
    assertFalse(manager.revokePrivilegeFromUser(user.getName(), path, privilegeId));

    Assert.assertThrows(
        AuthException.class,
        () -> manager.revokePrivilegeFromUser("not a user", path, privilegeId));

    // update password
    String newPassword = "newPassword";
    String illegalPW = "new";
    assertTrue(manager.updateUserPassword(user.getName(), newPassword));
    assertFalse(manager.updateUserPassword(user.getName(), illegalPW));
    user = manager.getUser(user.getName());
    assertTrue(AuthUtils.validatePassword(newPassword, user.getPassword()));

    Assert.assertThrows(
        AuthException.class, () -> manager.updateUserPassword("not a user", newPassword));

    // grant role
    String roleName = "newrole";
    assertTrue(manager.grantRoleToUser(roleName, user.getName()));
    assertFalse(manager.grantRoleToUser(roleName, user.getName()));
    user = manager.getUser(user.getName());
    assertTrue(user.hasRole(roleName));

    Assert.assertThrows(AuthException.class, () -> manager.grantRoleToUser("not a user", roleName));

    boolean caught = false;

    // revoke role
    assertTrue(manager.revokeRoleFromUser(roleName, user.getName()));
    assertFalse(manager.revokeRoleFromUser(roleName, user.getName()));
    user = manager.getUser(user.getName());
    assertFalse(user.hasRole(roleName));

    Assert.assertThrows(
        AuthException.class, () -> manager.revokeRoleFromUser("not a user", roleName));

    // list users
    List<String> usernames = manager.listAllUsers();
    usernames.sort(null);
    assertEquals(CommonDescriptor.getInstance().getConfig().getAdminName(), usernames.get(0));
    for (int i = 0; i < users.length - 1; i++) {
      assertEquals(users[i].getName(), usernames.get(i + 1));
    }
  }

  @Test
  public void testPathCheckForUpgrade() throws AuthException, IllegalPathException {
    manager.createUser("test", "pwssord", false);
    manager.setPreVersion(true);

    // turn to root.d.a
    manager.grantPrivilegeToUser(
        "test", new PartialPath("root.d.a"), PriPrivilegeType.READ_SCHEMA.ordinal(), false);
    // turn to root.**
    manager.grantPrivilegeToUser(
        "test", new PartialPath("root.d*.a"), PriPrivilegeType.READ_DATA.ordinal(), false);
    // turn to root.**
    manager.grantPrivilegeToUser(
        "test", new PartialPath("root.d*.a"), PriPrivilegeType.READ_SCHEMA.ordinal(), false);
    // turn to root.**
    manager.grantPrivilegeToUser(
        "test", new PartialPath("root.*.a.b"), PriPrivilegeType.READ_SCHEMA.ordinal(), false);
    // turn to root.ds.a.**
    manager.grantPrivilegeToUser(
        "test", new PartialPath("root.ds.a.b*"), PriPrivilegeType.READ_SCHEMA.ordinal(), false);
    // turn to root.ds.a.b
    manager.grantPrivilegeToUser(
        "test", new PartialPath("root.ds.a.b"), PriPrivilegeType.READ_SCHEMA.ordinal(), false);
    assertFalse(manager.getUser("test").getServiceReady());
    // after this operation, the user has these privileges:
    // root.d.a : read_schema
    // root.** : read_data, read_schema
    // root.ds.a.** :read_schema
    // root.ds.a.b : read_schema
    manager.checkAndRefreshPathPri();
    Role role = manager.getUser("test");
    assertTrue(role.getServiceReady());
    assertEquals(4, role.getPathPrivilegeList().size());
    manager.revokePrivilegeFromUser(
        "test", new PartialPath("root.**"), PriPrivilegeType.READ_SCHEMA.ordinal());
    manager.revokePrivilegeFromUser(
        "test", new PartialPath("root.**"), PriPrivilegeType.READ_DATA.ordinal());
    assertEquals(3, role.getPathPrivilegeList().size());
    assertTrue(
        role.checkPathPrivilege(
            new PartialPath("root.ds.a.**"), PriPrivilegeType.READ_SCHEMA.ordinal()));
    assertFalse(
        role.checkPathPrivilege(
            new PartialPath("root.ds.a.**"), PriPrivilegeType.READ_DATA.ordinal()));
  }
}
