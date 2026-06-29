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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.user.LocalFileUserAccessor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.external.commons.io.FileUtils;
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
    User user = new User("test", "password123456");
    user.setUserId(5);
    user.grantSysPrivilege(PrivilegeType.EXTEND_TEMPLATE, false);
    user.grantSysPrivilege(PrivilegeType.MANAGE_USER, false);
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.test"));
    pathPrivilege.grantPrivilege(PrivilegeType.READ_DATA, true);
    pathPrivilege.grantPrivilege(PrivilegeType.WRITE_DATA, false);
    user.getPathPrivilegeList().add(pathPrivilege);
    user.grantAnyScopePrivilege(PrivilegeType.SELECT, false);
    user.grantAnyScopePrivilege(PrivilegeType.ALTER, true);
    user.grantDBPrivilege("testdb", PrivilegeType.SELECT, false);
    user.grantTBPrivilege("testdb", "testtb", PrivilegeType.ALTER, true);
    user.addRole("testRole1");
    user.addRole("testRole2");
    accessor.saveEntity(user);
    accessor.reset();
    User loadUser = accessor.loadEntity("5");
    assertEquals(user, loadUser);
    user.setName("test1");
    user.setUserId(6);
    accessor.saveEntity(user);

    // list
    List<String> usernames = accessor.listAllEntities();
    usernames.sort(null);
    assertTrue(usernames.contains("5"));
    assertTrue(usernames.contains("6"));

    // delete
    assertFalse(accessor.deleteEntity("not a user"));
    assertTrue(accessor.deleteEntity(String.valueOf(user.getUserId())));
    usernames = accessor.listAllEntities();
    assertEquals(1, usernames.size());
    assertTrue(usernames.contains("5"));
    User nullUser = accessor.loadEntity(user.getName());
    assertNull(nullUser);
  }

  @Test
  public void testLoadOldVersion() throws IOException, IllegalPathException {
    User role = new User();
    role.setName("root");
    role.setPassword("password123456");
    List<PathPrivilege> pathPriList = new ArrayList<>();
    PathPrivilege rootPathPriv = new PathPrivilege(new PartialPath("root.**"));
    PathPrivilege normalPathPriv = new PathPrivilege(new PartialPath("root.b.c.**"));
    for (PrivilegeType privilegeType : PrivilegeType.values()) {
      if (privilegeType.isRelationalPrivilege()) continue;
      if (privilegeType.isSystemPrivilege()) {
        role.grantSysPrivilege(privilegeType, true);
      } else if (privilegeType.isPathPrivilege()) {
        rootPathPriv.grantPrivilege(privilegeType, true);
        normalPathPriv.grantPrivilege(privilegeType, true);
      }
    }
    pathPriList.add(rootPathPriv);
    pathPriList.add(normalPathPriv);
    role.setPrivilegeList(pathPriList);
    role.setSysPriGrantOpt(new HashSet<>());
    role.setSysPrivilegeSet(new HashSet<>());
    role.setRoleSet(new HashSet<>());
    accessor.saveUserOldVersion(role);
    User newRole = accessor.loadEntity("root");
    assertEquals(role, newRole);
    newRole.setName("root1");
    accessor.saveUserOldVersion1(newRole);
    User newRole1 = accessor.loadEntity("root1");
    assertEquals(newRole, newRole1);
    newRole.setName("root2");
    newRole.setUserId(10000);
    accessor.saveEntity(newRole);
    User newRole2 = accessor.loadEntity("10000");
    assertEquals(newRole, newRole2);
  }
}
