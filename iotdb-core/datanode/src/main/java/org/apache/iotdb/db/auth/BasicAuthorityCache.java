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
    userCache.put(userName, user);
  }

  @Override
  public void putRoleCache(String roleName, Role role) {
    roleCache.put(roleName, role);
  }

  /**
   * Invalidate user and role caches This method clears cached user and role information to ensure
   * that policy changes are immediately reflected in permission checks
   *
   * @param userName The username whose cache should be invalidated
   * @param roleName The role name whose cache should be invalidated
   * @return true if invalidation was successful, false otherwise
   */
  @Override
  public boolean invalidateCache(final String userName, final String roleName) {
    LOGGER.info("Invalidating permission cache - User: {}, Role: {}", userName, roleName);

    if (userName != null) {
      User cachedUser = userCache.getIfPresent(userName);
      if (cachedUser != null) {
        LOGGER.info("Found cached user {} with roles: {}", userName, cachedUser.getRoleSet());
        Set<String> roleSet = cachedUser.getRoleSet();
        if (!roleSet.isEmpty()) {
          LOGGER.info("Invalidating role cache for roles: {}", roleSet);
          roleCache.invalidateAll(roleSet);
        }
        userCache.invalidate(userName);
        LOGGER.info("Successfully invalidated user cache for: {}", userName);
      } else {
        LOGGER.debug("No cached user found for: {}", userName);
      }

      // Verify that cache was actually cleared
      if (userCache.getIfPresent(userName) != null) {
        LOGGER.error("Failed to invalidate user cache for: {}", userName);
        return false;
      }
    }

    if (roleName != null) {
      Role cachedRole = roleCache.getIfPresent(roleName);
      if (cachedRole != null) {
        LOGGER.info("Found cached role: {}", roleName);
        roleCache.invalidate(roleName);
        LOGGER.info("Successfully invalidated role cache for: {}", roleName);
      } else {
        LOGGER.debug("No cached role found for: {}", roleName);
      }

      // Verify that cache was actually cleared
      if (roleCache.getIfPresent(roleName) != null) {
        LOGGER.error("Failed to invalidate role cache for: {}", roleName);
        return false;
      }
    }

    LOGGER.info("Permission cache invalidation completed successfully");
    return true;
  }

  @Override
  public void invalidAllCache() {
    userCache.invalidateAll();
    roleCache.invalidateAll();
  }
}
