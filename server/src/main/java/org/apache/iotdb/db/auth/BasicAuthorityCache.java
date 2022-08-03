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

import java.util.List;
import java.util.concurrent.TimeUnit;

public class BasicAuthorityCache implements IAuthorCache {
  private static final Logger logger = LoggerFactory.getLogger(BasicAuthorityCache.class);

  private IoTDBDescriptor conf = IoTDBDescriptor.getInstance();

  private Cache<String, User> userCache =
      Caffeine.newBuilder()
          .maximumSize(conf.getConfig().getAuthorCacheSize())
          .expireAfterAccess(conf.getConfig().getAuthorCacheExpireTime(), TimeUnit.MINUTES)
          .build();

  private Cache<String, Role> roleCache =
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
   * Initialize user and role cache information.
   *
   * <p>If the permission information of the role changes, only the role cache information is
   * cleared. During permission checking, if the role belongs to a user, the user will be
   * initialized.
   */
  @Override
  public boolean invalidateCache(String userName, String roleName) {
    if (userName != null) {
      if (userCache.getIfPresent(userName) != null) {
        List<String> roleList = userCache.getIfPresent(userName).getRoleList();
        if (!roleList.isEmpty()) {
          roleCache.invalidateAll(roleList);
        }
        userCache.invalidate(userName);
      }
      if (userCache.getIfPresent(userName) != null) {
        logger.error("datanode cache initialization failed");
        return false;
      }
    }
    if (roleName != null) {
      if (roleCache.getIfPresent(roleName) != null) {
        roleCache.invalidate(roleName);
      }
      if (roleCache.getIfPresent(roleName) != null) {
        logger.error("datanode cache initialization failed");
        return false;
      }
    }
    return true;
  }
}
