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
package org.apache.iotdb.db.auth.role;

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PriPrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.role.LocalFileRoleAccessor;
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

public class LocalFileRoleAccessorTest {

  private File testFolder;
  private LocalFileRoleAccessor accessor;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    accessor = new LocalFileRoleAccessor(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws IOException, IllegalPathException {
    Role[] roles = new Role[4];
    for (int i = 0; i < roles.length; i++) {
      roles[i] = new Role("role" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.a.b.c" + j));
        pathPrivilege.getPrivileges().add(j);
        roles[i].getPathPrivilegeList().add(pathPrivilege);
        roles[i].getSysPrivilege().add(i + 4);
        if (i % 2 != 0) {
          roles[i].getSysPriGrantOpt().add(i + 4);
        }
      }
    }

    // save
    for (Role role : roles) {
      accessor.saveRole(role);
    }

    // load
    for (Role role : roles) {
      Role loadedRole = accessor.loadRole(role.getName());
      assertEquals(role, loadedRole);
    }
    assertNull(accessor.loadRole("not a role"));

    // delete
    assertTrue(accessor.deleteRole(roles[roles.length - 1].getName()));
    assertFalse(accessor.deleteRole(roles[roles.length - 1].getName()));
    assertNull(accessor.loadRole(roles[roles.length - 1].getName()));

    // list
    List<String> roleNames = accessor.listAllRoles();
    roleNames.sort(null);
    for (int i = 0; i < roleNames.size(); i++) {
      assertEquals(roles[i].getName(), roleNames.get(i));
    }
  }

  @Test
  public void testLoadOldVersion() throws IOException, IllegalPathException {
    // In this test, we will store role with old func and role might have illegal path.
    Role role = new Role();
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
    accessor.saveRoleOldVer(role);
    Role newRole = accessor.loadRole("root");
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
