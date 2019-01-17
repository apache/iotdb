/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.auth;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.role.LocalFileRoleAccessor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalFIleRoleAccessorTest {

  private File testFolder;
  private LocalFileRoleAccessor accessor;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    testFolder = new File("test/");
    testFolder.mkdirs();
    accessor = new LocalFileRoleAccessor(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws IOException {
    Role[] roles = new Role[5];
    for (int i = 0; i < roles.length; i++) {
      roles[i] = new Role("role" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege("root.a.b.c" + j);
        pathPrivilege.privileges.add(j);
        roles[i].privilegeList.add(pathPrivilege);
      }
    }

    // save
    for (Role role : roles) {
      accessor.saveRole(role);
    }

    // load
    for (Role role : roles) {
      Role loadedRole = accessor.loadRole(role.name);
      assertEquals(role, loadedRole);
    }
    assertEquals(null, accessor.loadRole("not a role"));

    // delete
    assertEquals(true, accessor.deleteRole(roles[roles.length - 1].name));
    assertEquals(false, accessor.deleteRole(roles[roles.length - 1].name));
    assertEquals(null, accessor.loadRole(roles[roles.length - 1].name));

    // list
    List<String> roleNames = accessor.listAllRoles();
    roleNames.sort(null);
    for (int i = 0; i < roleNames.size(); i++) {
      assertEquals(roles[i].name, roleNames.get(i));
    }
  }
}
