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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.user.LocalFileUserAccessor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalFileUserAccessorTest {

  private File testFolder;
  private LocalFileUserAccessor accessor;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    testFolder = new File("test/");
    testFolder.mkdirs();
    accessor = new LocalFileUserAccessor(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws IOException {
    User[] users = new User[5];
    for (int i = 0; i < users.length; i++) {
      users[i] = new User("user" + i, "password" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege("root.a.b.c" + j);
        pathPrivilege.privileges.add(j);
        users[i].privilegeList.add(pathPrivilege);
        users[i].roleList.add("role" + j);
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
        User loadedUser = accessor.loadUser(user.name);
        assertEquals(user, loadedUser);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }
    assertEquals(null, accessor.loadUser("not a user"));

    // list
    List<String> usernames = accessor.listAllUsers();
    usernames.sort(null);
    for (int i = 0; i < users.length; i++) {
      assertEquals(users[i].name, usernames.get(i));
    }

    // delete
    assertEquals(false, accessor.deleteUser("not a user"));
    assertEquals(true, accessor.deleteUser(users[users.length - 1].name));
    usernames = accessor.listAllUsers();
    assertEquals(users.length - 1, usernames.size());
    usernames.sort(null);
    for (int i = 0; i < users.length - 1; i++) {
      assertEquals(users[i].name, usernames.get(i));
    }
    User nullUser = accessor.loadUser(users[users.length - 1].name);
    assertEquals(null, nullUser);
  }
}
