/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.protocol.rest.filter;

import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.TimeUnit;

public class UserCache {
  private static final IoTDBRestServiceConfig CONFIG =
      IoTDBRestServiceDescriptor.getInstance().getConfig();
  private final Cache<String, User> cache;

  private UserCache() {
    cache =
        Caffeine.newBuilder()
            .initialCapacity(CONFIG.getCacheInitNum())
            .maximumSize(CONFIG.getCacheMaxNum())
            .expireAfterWrite(CONFIG.getCacheExpireInSeconds(), TimeUnit.SECONDS)
            .build();
  }

  public static UserCache getInstance() {
    return UserCacheHolder.INSTANCE;
  }

  public User getUser(String key) {
    return cache.getIfPresent(key);
  }

  public void setUser(String key, User user) {
    cache.put(key, user);
  }

  private static class UserCacheHolder {
    private static final UserCache INSTANCE = new UserCache();
  }
}
