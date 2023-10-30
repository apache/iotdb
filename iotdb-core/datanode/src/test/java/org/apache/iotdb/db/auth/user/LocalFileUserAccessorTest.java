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

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PriPrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.user.LocalFileUserAccessor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LocalFileUserAccessorTest {

  private File testFolder;
  private LocalFileUserAccessor accessor;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    accessor = new LocalFileUserAccessor(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws IOException, IllegalPathException {
    User[] users = new User[4];
    for (int i = 0; i < users.length; i++) {
      users[i] = new User("user" + i, "password" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.a.b.c" + j));
        pathPrivilege.getPrivileges().add(j);
        users[i].getPathPrivilegeList().add(pathPrivilege);
        users[i].getSysPrivilege().add(j + 5);
        users[i].getRoleList().add("role" + j);
      }
    }

    // save
    for (User user : users) {
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }

    // load
    for (User user : users) {
      try {
        User loadedUser = accessor.loadUser(user.getName());
        assertEquals(user, loadedUser);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }
    assertNull(accessor.loadUser("not a user"));

    // list
    List<String> usernames = accessor.listAllUsers();
    usernames.sort(null);
    for (int i = 0; i < users.length; i++) {
      assertEquals(users[i].getName(), usernames.get(i));
    }

    // delete
    assertTrue(accessor.deleteUser("not a user"));
    assertTrue(accessor.deleteUser(users[users.length - 1].getName()));
    usernames = accessor.listAllUsers();
    assertEquals(users.length - 1, usernames.size());
    usernames.sort(null);
    for (int i = 0; i < users.length - 1; i++) {
      assertEquals(users[i].getName(), usernames.get(i));
    }
    User nullUser = accessor.loadUser(users[users.length - 1].getName());
    assertNull(nullUser);
  }

  @Test
  public void testLoadOldVersion() throws IOException, IllegalPathException {
    User user = new User();
    user.setName("root");
    user.setPassword("password1");
    user.setRoleList(Collections.emptyList());

    List<PathPrivilege> pathPriList = new ArrayList<>();
    PathPrivilege rootPathPriv = new PathPrivilege(new PartialPath("root.**"));
    PathPrivilege normalPathPriv = new PathPrivilege(new PartialPath("root.b.c.**"));
    PathPrivilege wroPathPriv = new PathPrivilege(new PartialPath("root.c.*.d"));
    PathPrivilege wroPathPriv2 = new PathPrivilege(new PartialPath("root.c.*.**"));
    for (PriPrivilegeType item : PriPrivilegeType.values()) {
      if (item.isPreIsPathRelevant()) {
        normalPathPriv.grantPrivilege(item.ordinal(), false);
        wroPathPriv.grantPrivilege(item.ordinal(), false);
        wroPathPriv2.grantPrivilege(item.ordinal(), false);
      }
      rootPathPriv.grantPrivilege(item.ordinal(), false);
    }

    // In this case, we use four path to store some privileges.
    // path1: root.** will store all privileges
    // path2: root.b.c.** will store relevant privileges
    // path3: root.c.*.d will store relevant privileges but the path will be transformed to
    // root.c.**
    // path4: root.c.*.** will store relevant privileges but the path will be transformed like path3

    // 1. for path 1:
    pathPriList.add(rootPathPriv);
    user.setPrivilegeList(pathPriList);
    user.setSysPriGrantOpt(new HashSet<>());
    user.setSysPrivilegeSet(new HashSet<>());
    accessor.saveUserOldVersion(user);
    Role newRole = accessor.loadUser("root");
    assertEquals("root", newRole.getName());
    assertTrue(newRole.getServiceReady());
    assertEquals(1, newRole.getPathPrivilegeList().size());
    assertEquals(
        PrivilegeType.getPathPriCount(),
        newRole.getPathPrivilegeList().get(0).getPrivileges().size());
    assertEquals(PrivilegeType.getSysPriCount(), newRole.getSysPrivilege().size());
    accessor.deleteUser("root");

    // 2. for path2:
    pathPriList.clear();
    pathPriList.add(normalPathPriv);
    user.setPrivilegeList(pathPriList);
    accessor.saveUserOldVersion(user);
    newRole = accessor.loadUser("root");
    assertTrue(newRole.getServiceReady());
    assertEquals(3, newRole.getPathPrivilegeList().get(0).getPrivileges().size());
    assertEquals(2, newRole.getSysPrivilege().size());
    accessor.deleteUser("root");

    // 3. for path3 and path4
    pathPriList.clear();
    pathPriList.add(wroPathPriv2);
    pathPriList.add(wroPathPriv);
    user.setPrivilegeList(pathPriList);
    accessor.saveUserOldVersion(user);
    newRole = accessor.loadUser("root");
    assertFalse(newRole.getServiceReady());
    assertEquals(3, newRole.getPathPrivilegeList().get(0).getPrivileges().size());
    assertEquals(3, newRole.getPathPrivilegeList().get(1).getPrivileges().size());
    assertEquals(2, newRole.getSysPrivilege().size());
  }
}
