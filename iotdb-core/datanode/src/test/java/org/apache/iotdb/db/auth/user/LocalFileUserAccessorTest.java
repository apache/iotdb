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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

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
    User user = new User("test", "password");
    user.grantSysPrivilege(PrivilegeType.EXTEND_TEMPLATE, false);
    user.grantSysPrivilege(PrivilegeType.MANAGE_USER, false);
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.test"));
    pathPrivilege.grantPrivilege(PrivilegeType.READ_DATA, true);
    pathPrivilege.grantPrivilege(PrivilegeType.WRITE_DATA, false);
    user.getPathPrivilegeList().add(pathPrivilege);
    user.grantDBPrivilege("testdb", PrivilegeType.SELECT, false);
    user.grantTBPrivilege("testdb", "testtb", PrivilegeType.ALTER, true);
    user.addRole("testRole1");
    user.addRole("testRole2");
    accessor.saveEntry(user);
    accessor.reset();
    User loadUser = accessor.loadEntry("test");
    assertEquals(user, loadUser);
  }
}
