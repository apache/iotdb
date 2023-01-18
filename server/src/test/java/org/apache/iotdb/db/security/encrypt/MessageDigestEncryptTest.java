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
package org.apache.iotdb.db.security.encrypt;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.user.LocalFileUserManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessageDigestEncryptTest {
  private static final String providerClass =
      "org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt";

  private File testFolder;
  private LocalFileUserManager manager;
  private MessageDigestEncrypt messageDigestEncrypt = new MessageDigestEncrypt();

  @Before
  public void setUp() throws Exception {
    CommonDescriptor.getInstance().getConfig().setEncryptDecryptProvider(providerClass);
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
  public void testMessageDigestEncrypt() throws AuthException {
    User[] users = new User[5];
    for (int i = 0; i < users.length; i++) {
      users[i] = new User("user" + i, "password" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege("root.a.b.c" + j);
        pathPrivilege.getPrivileges().add(j);
        users[i].getPrivilegeList().add(pathPrivilege);
        users[i].getRoleList().add("role" + j);
      }
    }

    // create
    User user = manager.getUser(users[0].getName());
    assertNull(user);
    for (User user1 : users) {
      assertTrue(manager.createUser(user1.getName(), user1.getPassword()));
    }
    for (User user1 : users) {
      user = manager.getUser(user1.getName());
      assertEquals(user1.getName(), user.getName());
      assertEquals(messageDigestEncrypt.encrypt(user1.getPassword()), user.getPassword());
    }
  }

  @Test
  public void testMessageDigestValidatePassword() {
    String password = "root";
    assertTrue(messageDigestEncrypt.validate(password, messageDigestEncrypt.encrypt(password)));
  }
}
