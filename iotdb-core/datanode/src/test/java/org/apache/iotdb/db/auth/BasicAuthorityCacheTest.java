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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class BasicAuthorityCacheTest {

  @Test
  public void testInvalidAllCacheErasesCachedPasswords() throws Exception {
    final BasicAuthorityCache authorityCache = new BasicAuthorityCache();
    final User user = new User("user", "password-digest");
    final Role role = new Role("role");
    final byte[] password = user.getPassword();

    authorityCache.putUserCache(user.getName(), user);
    authorityCache.putRoleCache(role.getName(), role);

    authorityCache.invalidAllCache();

    Assert.assertArrayEquals(new byte[password.length], password);
    Assert.assertNull(user.getPassword());
    Assert.assertNull(authorityCache.getUserCache(user.getName()));
    Assert.assertNull(authorityCache.getRoleCache(role.getName()));
  }

  @Test
  public void testInvalidateUserCacheErasesPassword() throws Exception {
    final BasicAuthorityCache authorityCache = new BasicAuthorityCache();
    final User user = new User("user", "password-digest");
    final byte[] password = user.getPassword();
    authorityCache.putUserCache(user.getName(), user);

    Assert.assertTrue(authorityCache.invalidateCache(user.getName(), null));

    Assert.assertArrayEquals(new byte[password.length], password);
    Assert.assertNull(authorityCache.getUserCache(user.getName()));
  }

  @Test
  public void testReplacingCachedUserErasesOldPassword() throws Exception {
    final BasicAuthorityCache authorityCache = new BasicAuthorityCache();
    final User oldUser = new User("user", "old-password");
    final User newUser = new User("user", "new-password");
    final byte[] oldPassword = oldUser.getPassword();
    authorityCache.putUserCache(oldUser.getName(), oldUser);

    authorityCache.putUserCache(newUser.getName(), newUser);

    Assert.assertArrayEquals(new byte[oldPassword.length], oldPassword);
    Assert.assertSame(newUser, authorityCache.getUserCache(newUser.getName()));
    Assert.assertArrayEquals(
        "new-password".getBytes(StandardCharsets.UTF_8), newUser.getPassword());
    authorityCache.invalidAllCache();
  }
}
