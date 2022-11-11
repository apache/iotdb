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

package org.apache.iotdb.db.mpp.common.object;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MPPObjectPool {

  private final Map<String, QueryObjectPool> queryPool = new ConcurrentHashMap<>();

  private MPPObjectPool() {}

  private static class MPPObjectPoolHolder {

    private static final MPPObjectPool INSTANCE = new MPPObjectPool();

    private MPPObjectPoolHolder() {}
  }

  public static MPPObjectPool getInstance() {
    return MPPObjectPoolHolder.INSTANCE;
  }

  public <T extends ObjectEntry> T put(String queryId, T objectEntry) {
    return queryPool.computeIfAbsent(queryId, k -> new QueryObjectPool()).put(objectEntry);
  }

  public <T extends ObjectEntry> T get(String queryId, int objectId) {
    QueryObjectPool queryObjectPool = queryPool.get(queryId);
    if (queryObjectPool == null) {
      return null;
    } else {
      return queryObjectPool.get(objectId);
    }
  }

  public void clear(String queryId) {
    queryPool.remove(queryId);
  }

  public void clear() {
    queryPool.clear();
  }

  private static class QueryObjectPool {

    private final AtomicInteger indexGenerator = new AtomicInteger(0);

    private final Map<Integer, ObjectEntry> objectPool = new ConcurrentHashMap<>();

    public <T extends ObjectEntry> T put(T objectEntry) {
      int id = indexGenerator.getAndIncrement();
      objectEntry.setId(id);
      objectPool.put(id, objectEntry);
      return objectEntry;
    }

    @SuppressWarnings("unchecked")
    public <T extends ObjectEntry> T get(int objectId) {
      return (T) objectPool.remove(objectId);
    }
  }
}
