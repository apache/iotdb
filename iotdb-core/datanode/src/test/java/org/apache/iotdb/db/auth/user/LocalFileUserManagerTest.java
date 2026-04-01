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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.user.LocalFileUserManager;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalFileUserManagerTest {

  private File testFolder;
  private LocalFileUserManager manager;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    manager = new LocalFileUserManager(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testIllegalInput() {
    // Password contains space
    try {
      manager.createUser("username1", "password_ 123456", true);
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
    // Username contains space
    try {
      assertFalse(manager.createUser("username 2", "password_", true));
    } catch (AuthException e) {
      assertTrue(e.getMessage().contains("cannot contain spaces"));
    }
  }

  @Test
  public void testCreateUserRawPassword() throws AuthException {
    Assert.assertTrue(
        manager.createUser("testRaw", AuthUtils.encryptPassword("password1"), true, false));
    User user = manager.getEntity("testRaw");
    Assert.assertEquals(user.getPassword(), AuthUtils.encryptPassword("password1"));
  }
}
