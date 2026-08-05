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
package org.apache.iotdb.db.auth.entity;

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class UserTest {

  @Test
  public void testUser() throws Exception {
    User user = new User("user", "password123456");
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.ln"));
    user.setPrivilegeList(Collections.singletonList(pathPrivilege));
    user.setPathPrivileges(
        new PartialPath("root.ln"), Collections.singleton(PrivilegeType.WRITE_DATA));
    Assert.assertEquals(
        "User{id=-1, name='user', pathPrivilegeList=[root.ln : WRITE_DATA], "
            + "sysPrivilegeSet=[], AnyScopePrivilegeMap=[], objectPrivilegeMap={}, roleList=[], isOpenIdUser=false, maxSessionPerUser=-1, minSessionPerUser=-1}",
        user.toString());
    User user1 = new User("user1", "password1");
    user1.deserialize(user.serialize());
    Assert.assertEquals(
        "User{id=-1, name='user', pathPrivilegeList=[root.ln : WRITE_DATA], "
            + "sysPrivilegeSet=[], AnyScopePrivilegeMap=[], objectPrivilegeMap={}, roleList=[], isOpenIdUser=false, maxSessionPerUser=-1, minSessionPerUser=-1}",
        user1.toString());
    Assert.assertEquals(user1, user);
    Assert.assertArrayEquals(
        "password123456".getBytes(StandardCharsets.UTF_8), user1.getPassword());
  }

  @Test
  public void testPasswordBufferIsErasedWhenPasswordChanges() throws Exception {
    final User user = new User("user", "old-password");
    final byte[] oldPassword = user.getPassword();

    user.setPassword("new-password");

    Assert.assertArrayEquals(new byte[oldPassword.length], oldPassword);
    Assert.assertArrayEquals("new-password".getBytes(StandardCharsets.UTF_8), user.getPassword());

    final byte[] newPassword = user.getPassword();
    user.erasePassword();

    Assert.assertArrayEquals(new byte[newPassword.length], newPassword);
    Assert.assertNull(user.getPassword());
  }

  @Test
  public void testPasswordSerializationRemainsCompatible() throws Exception {
    final User source = new User("user", "password");
    final User target = new User("target", "password-to-erase");
    final byte[] oldTargetPassword = target.getPassword();

    target.deserialize(source.serialize());

    Assert.assertArrayEquals(new byte[oldTargetPassword.length], oldTargetPassword);
    Assert.assertArrayEquals("password".getBytes(StandardCharsets.UTF_8), target.getPassword());
    Assert.assertEquals(source, target);
  }
}
