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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.role.LocalFileRoleManager;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LocalFileRoleManagerTest {

  private File testFolder;
  private LocalFileRoleManager manager;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    manager = new LocalFileRoleManager(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws AuthException, IllegalPathException {
    Role[] roles = new Role[4];
    for (int i = 0; i < roles.length; i++) {
      roles[i] = new Role("role" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.a.b.c" + j));
        pathPrivilege.grantPrivilege(PrivilegeType.values()[j], false);
        roles[i].getPathPrivilegeList().add(pathPrivilege);
        roles[i].getSysPrivilege().add(PrivilegeType.values()[j + 4]);
      }
    }

    // create
    Role role = manager.getEntity(roles[0].getName());
    assertNull(role);
    for (Role role1 : roles) {
      assertTrue(manager.createRole(role1.getName()));
    }
    for (Role role1 : roles) {
      role = manager.getEntity(role1.getName());
      assertEquals(role1.getName(), role.getName());
    }

    assertFalse(manager.createRole(roles[0].getName()));

    // delete
    assertFalse(manager.deleteEntity("not a role"));
    assertTrue(manager.deleteEntity(roles[roles.length - 1].getName()));
    assertNull(manager.getEntity(roles[roles.length - 1].getName()));
    assertFalse(manager.deleteEntity(roles[roles.length - 1].getName()));

    // grant privilege
    role = manager.getEntity(roles[0].getName());
    PartialPath path = new PartialPath("root.a.b.c");
    assertFalse(role.hasPrivilegeToRevoke(path, PrivilegeType.READ_DATA));
    manager.grantPrivilegeToEntity(
        role.getName(), new PrivilegeUnion(path, PrivilegeType.READ_DATA));
    manager.grantPrivilegeToEntity(
        role.getName(), new PrivilegeUnion(path, PrivilegeType.WRITE_DATA));

    // grant again will success
    manager.grantPrivilegeToEntity(
        role.getName(), new PrivilegeUnion(path, PrivilegeType.WRITE_DATA));
    role = manager.getEntity(roles[0].getName());
    assertTrue(role.hasPrivilegeToRevoke(path, PrivilegeType.READ_DATA));
    manager.grantPrivilegeToEntity(role.getName(), new PrivilegeUnion(PrivilegeType.MAINTAIN));
    manager.grantPrivilegeToEntity(
        role.getName(), new PrivilegeUnion(PrivilegeType.MANAGE_ROLE, true));
    boolean caught = false;
    try {
      manager.grantPrivilegeToEntity("not a role", new PrivilegeUnion(PrivilegeType.MANAGE_ROLE));
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // revoke privilege
    role = manager.getEntity(roles[0].getName());
    manager.revokePrivilegeFromEntity(role.getName(), new PrivilegeUnion(PrivilegeType.MAINTAIN));
    manager.revokePrivilegeFromEntity(
        role.getName(), new PrivilegeUnion(PrivilegeType.MANAGE_USER));
    manager.revokePrivilegeFromEntity(
        role.getName(),
        new PrivilegeUnion(new PartialPath("root.test"), PrivilegeType.WRITE_SCHEMA));
    assertEquals(1, manager.getEntity(role.getName()).getSysPriGrantOpt().size());
    caught = false;
    try {
      manager.revokePrivilegeFromEntity("not a role", new PrivilegeUnion(PrivilegeType.MAINTAIN));
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // list roles
    List<String> roleNames = manager.listAllEntities();
    roleNames.sort(null);
    for (int i = 0; i < roles.length - 1; i++) {
      assertEquals(roles[i].getName(), roleNames.get(i));
    }
  }
}
