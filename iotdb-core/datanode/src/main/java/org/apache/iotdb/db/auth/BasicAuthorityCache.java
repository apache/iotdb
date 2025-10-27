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
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class BasicAuthorityCache implements IAuthorCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthorityCache.class);

  private final IoTDBDescriptor conf = IoTDBDescriptor.getInstance();

  private final Cache<String, User> userCache =
      Caffeine.newBuilder()
          .maximumSize(conf.getConfig().getAuthorCacheSize())
          .expireAfterAccess(conf.getConfig().getAuthorCacheExpireTime(), TimeUnit.MINUTES)
          .build();

  private final Cache<String, Role> roleCache =
      Caffeine.newBuilder()
          .maximumSize(conf.getConfig().getAuthorCacheSize())
          .expireAfterAccess(conf.getConfig().getAuthorCacheExpireTime(), TimeUnit.MINUTES)
          .build();

  @Override
  public User getUserCache(String userName) {
    return userCache.getIfPresent(userName);
  }

  @Override
  public Role getRoleCache(String roleName) {
    return roleCache.getIfPresent(roleName);
  }

  @Override
  public void putUserCache(String userName, User user) {
    if (user.getUserId() == AuthorityChecker.SUPER_USER_ID) {
      AuthorityChecker.setSuperUser(user.getName());
    }
    userCache.put(userName, user);
  }

  @Override
  public void putRoleCache(String roleName, Role role) {
    roleCache.put(roleName, role);
  }

  /**
   * Initialize user and role cache information.
   *
   * <p>If the permission information of the role changes, only the role cache information is
   * cleared. During permission checking, if the role belongs to a user, the user will be
   * initialized.
   */
  @Override
  public boolean invalidateCache(final String userName, final String roleName) {
    if (userName != null) {
      if (userCache.getIfPresent(userName) != null) {
        Set<String> roleSet = userCache.getIfPresent(userName).getRoleSet();
        if (!roleSet.isEmpty()) {
          roleCache.invalidateAll(roleSet);
        }
        userCache.invalidate(userName);
      }
      if (userCache.getIfPresent(userName) != null) {
        LOGGER.error("datanode cache initialization failed");
        return false;
      }
    }
    if (roleName != null) {
      if (roleCache.getIfPresent(roleName) != null) {
        roleCache.invalidate(roleName);
      }
      if (roleCache.getIfPresent(roleName) != null) {
        LOGGER.error("datanode cache initialization failed");
        return false;
      }
    }
    return true;
  }

  @Override
  public void invalidAllCache() {
    userCache.invalidateAll();
    roleCache.invalidateAll();
  }
}
