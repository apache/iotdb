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
    // In this test, we will store role with old func and role might have illegal path.
    User role = new User();
    role.setName("root");
    List<PathPrivilege> pathPriList = new ArrayList<>();
    PathPrivilege rootPathPriv = new PathPrivilege(new PartialPath("root.**"));
    PathPrivilege normalPathPriv = new PathPrivilege(new PartialPath("root.b.c.**"));
    PathPrivilege wroPathPriv = new PathPrivilege(new PartialPath("root.c.*.d"));
    PathPrivilege wroPathPriv2 = new PathPrivilege(new PartialPath("root.c.*.**"));
    for (PriPrivilegeType item : PriPrivilegeType.values()) {
      // ALL will never appear in file.
      if (item.ordinal() == PriPrivilegeType.ALL.ordinal()) {
        continue;
      }
      if (item.isPrePathRelevant()) {
        normalPathPriv.grantPrivilege(item.ordinal(), false);
        wroPathPriv.grantPrivilege(item.ordinal(), false);
        wroPathPriv2.grantPrivilege(item.ordinal(), false);
      }
      rootPathPriv.grantPrivilege(item.ordinal(), false);
    }

    pathPriList.add(rootPathPriv);
    pathPriList.add(normalPathPriv);
    pathPriList.add(wroPathPriv);
    pathPriList.add(wroPathPriv2);
    role.setPrivilegeList(pathPriList);
    role.setSysPriGrantOpt(new HashSet<>());
    role.setSysPrivilegeSet(new HashSet<>());
    role.setRoleList(new ArrayList<>());
    accessor.saveUserOldVersion(role);
    User newRole = accessor.loadUser("root");
    assertEquals("root", newRole.getName());
    assertFalse(newRole.getServiceReady());
    assertEquals(4, newRole.getPathPrivilegeList().size());
    for (PathPrivilege path : newRole.getPathPrivilegeList()) {
      if (!path.getPath().equals(new PartialPath("root.**"))) {
        assertEquals(17, path.getPrivileges().size());
      } else {
        assertEquals(33, path.getPrivileges().size());
      }
    }
  }
}
