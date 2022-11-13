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

import org.apache.iotdb.commons.utils.TestOnly;

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

  public QueryObjectPool getQueryObjectPool(String queryId) {
    return queryPool.compute(
        queryId,
        (k, v) -> {
          QueryObjectPool queryObjectPool;
          if (v == null) {
            queryObjectPool = new QueryObjectPool(queryId);
          } else {
            queryObjectPool = v;
          }
          queryObjectPool.incReferenceCount();
          return queryObjectPool;
        });
  }

  public void clearQueryObjectPool(String queryId) {
    queryPool.compute(
        queryId,
        (k, v) -> {
          if (v == null) {
            return null;
          }
          v.decReferenceCount();
          if (v.getReferenceCount() == 0) {
            return null;
          } else {
            return v;
          }
        });
  }

  @TestOnly
  boolean hasQueryObjectPool(String queryId) {
    return queryPool.containsKey(queryId);
  }

  public void clear() {
    queryPool.clear();
  }

  public static class QueryObjectPool {

    private final String queryId;

    private final AtomicInteger referenceCount = new AtomicInteger(0);

    private final AtomicInteger indexGenerator = new AtomicInteger(0);

    private final Map<Integer, ObjectEntry> objectPool = new ConcurrentHashMap<>();

    private QueryObjectPool(String queryId) {
      this.queryId = queryId;
    }

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

    public String getQueryId() {
      return queryId;
    }

    private int getReferenceCount() {
      return referenceCount.get();
    }

    private void incReferenceCount() {
      referenceCount.getAndIncrement();
    }

    private void decReferenceCount() {
      referenceCount.decrementAndGet();
    }
  }
}
