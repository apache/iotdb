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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.role.LocalFileRoleAccessor;
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
        pathPrivilege.grantPrivilege(PrivilegeType.values()[j], true);
        roles[i].getPathPrivilegeList().add(pathPrivilege);
      }
      roles[i].grantSysPrivilege(PrivilegeType.values()[i + 4], false);
      roles[i].grantDBPrivilege("testdb", PrivilegeType.CREATE, false);
      roles[i].grantTBPrivilege("testdb", "table", PrivilegeType.ALTER, true);
      roles[i].grantAnyScopePrivilege(PrivilegeType.INSERT, true);
      if (i % 2 != 0) {
        roles[i].grantSysPrivilegeGrantOption(PrivilegeType.values()[i + 4]);
      }
    }

    for (Role role : roles) {
      for (PrivilegeType item : PrivilegeType.values()) {
        if (item.isRelationalPrivilege()) {
          role.grantDBPrivilege("testdb", item, true);
          if (item.ordinal() % 2 == 0) {
            role.grantTBPrivilege("testdb", "testtb", item, false);
          }
        }
      }
    }

    // save
    for (Role role : roles) {
      accessor.saveEntity(role);
    }

    // load
    for (Role role : roles) {
      Role loadedRole = accessor.loadEntity(role.getName());
      assertEquals(role, loadedRole);
    }
    assertNull(accessor.loadEntity("not a role"));

    // delete
    assertTrue(accessor.deleteEntity(roles[roles.length - 1].getName()));
    assertFalse(accessor.deleteEntity(roles[roles.length - 1].getName()));
    assertNull(accessor.loadEntity(roles[roles.length - 1].getName()));

    // list
    List<String> roleNames = accessor.listAllEntities();
    roleNames.sort(null);
    for (int i = 0; i < roleNames.size(); i++) {
      assertEquals(roles[i].getName(), roleNames.get(i));
      accessor.deleteEntity(roleNames.get(i));
    }
    String[] files = testFolder.list();
    if (files != null) {
      assertEquals(0, files.length);
    }
  }
}
