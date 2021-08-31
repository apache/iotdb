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
package org.apache.iotdb.db.rescon;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachedStringPool {

  private Map<String, String> cachedPool = new ConcurrentHashMap<>();

  public Map<String, String> getCachedPool() {
    return cachedPool;
  }

  public static CachedStringPool getInstance() {
    return CachedStringPool.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private static final CachedStringPool INSTANCE = new CachedStringPool();

    private InstanceHolder() {}
  }
}
