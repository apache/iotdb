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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    List<PathPrivilege> pathPriList = new ArrayList<>();

    // root.a.b.c -- read_data, wirte_shcema.
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.a.b.c"));
    pathPrivilege.grantPrivilege(PriPrivilegeType.READ_DATA.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.WRITE_SCHEMA.ordinal(), false);
    pathPriList.add(pathPrivilege);

    // root.a.*.b -- read_schema, write_data
    pathPrivilege = new PathPrivilege(new PartialPath("root.a.*.b"));
    pathPrivilege.grantPrivilege(PriPrivilegeType.READ_SCHEMA.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.WRITE_DATA.ordinal(), false);
    pathPriList.add(pathPrivilege);

    // root.a.* -- manage_database -- will ignore the path.
    pathPrivilege = new PathPrivilege(new PartialPath("root.a.*"));
    pathPrivilege.grantPrivilege(PriPrivilegeType.MANAGE_DATABASE.ordinal(), false);
    pathPriList.add(pathPrivilege);

    // root.** -- for some systems.
    pathPrivilege = new PathPrivilege(new PartialPath("root.**"));
    pathPrivilege.grantPrivilege(PriPrivilegeType.MAINTAIN.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.MANAGE_ROLE.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.MANAGE_USER.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.ALTER_PASSWORD.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.GRANT_PRIVILEGE.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.USE_CQ.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.USE_PIPE.ordinal(), false);
    pathPrivilege.grantPrivilege(PriPrivilegeType.USE_TRIGGER.ordinal(), false);
    pathPriList.add(pathPrivilege);

    user.setPrivilegeList(pathPriList);
    ArrayList<String> roleList = new ArrayList<>();
    roleList.add("role1");
    roleList.add("role2");
    user.setRoleList(roleList);

    accessor.saveUserOldVersion(user);
    User newUser = accessor.loadUser("root");
    assertEquals("root", newUser.getName());
    assertEquals("password1", newUser.getPassword());

    Assert.assertFalse(newUser.getServiceReady());

    assertEquals(2, newUser.getPathPrivilegeList().size());
    assertEquals(7, newUser.getSysPrivilege().size());
    assertNotNull(newUser.getSysPriGrantOpt());

    accessor.deleteUser("root");
    accessor.saveUser(newUser);
    User newUser2 = accessor.loadUser("root");
    assertEquals(newUser2, newUser);
  }
}
