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
import static org.junit.Assert.assertNotNull;
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

    role.setPrivilegeList(pathPriList);
    role.setSysPriGrantOpt(new HashSet<>());
    role.setSysPrivilegeSet(new HashSet<>());
    accessor.saveRoleOldVer(role);
    Role newRole = accessor.loadRole("root");
    assertEquals("root", newRole.getName());

    // because newRole has illegal path, its not ready to service.
    assertFalse(newRole.getServiceReady());

    // ignore manage_database.
    assertEquals(2, newRole.getPathPrivilegeList().size());

    // ignore alterpassword and grant_privilege
    assertEquals(7, newRole.getSysPrivilege().size());
    assertNotNull(newRole.getSysPriGrantOpt());
    accessor.deleteRole("root");
    accessor.saveRole(newRole);
    Role newRole2 = accessor.loadRole("root");
    assertEquals(newRole, newRole2);
  }
}
