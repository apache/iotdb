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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class MPPObjectPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(MPPObjectPool.class);

  private final Map<String, QueryObjectPoolReference> queryPool = new ConcurrentHashMap<>();
  private final ReferenceQueue<QueryObjectPool> referenceQueue = new ReferenceQueue<>();

  private final ExecutorService executorService =
      IoTDBThreadPoolFactory.newSingleThreadExecutor("MPP-Object-Release-Task");
  private volatile boolean hasReleaseTask = false;

  private MPPObjectPool() {}

  private static class MPPObjectPoolHolder {

    private static final MPPObjectPool INSTANCE = new MPPObjectPool();

    private MPPObjectPoolHolder() {}
  }

  public static MPPObjectPool getInstance() {
    return MPPObjectPoolHolder.INSTANCE;
  }

  public QueryObjectPool getQueryObjectPool(String queryId) {
    QueryObjectPoolReference reference = queryPool.get(queryId);
    QueryObjectPool queryObjectPool = null;
    if (reference != null) {
      queryObjectPool = reference.get();
    }

    if (queryObjectPool == null) {
      queryObjectPool = new QueryObjectPool(queryId);
      QueryObjectPool finalQueryObjectPool = queryObjectPool;
      return queryPool
          .compute(
              queryId,
              (k, v) ->
                  v == null || v.get() == null
                      ? new QueryObjectPoolReference(queryId, finalQueryObjectPool, referenceQueue)
                      : v)
          .get();
    } else {
      return queryObjectPool;
    }
  }

  public void clearQueryObjectPool(String queryId) {
    if (hasReleaseTask) {
      return;
    }
    QueryObjectPoolReference reference = queryPool.get(queryId);
    if (reference == null || reference.get() != null) {
      return;
    }
    synchronized (this) {
      if (hasReleaseTask) {
        return;
      }
      reference = queryPool.get(queryId);
      if (reference == null || reference.get() != null) {
        return;
      }
      hasReleaseTask = true;
      executorService.submit(this::executeClearQueryObjectPool);
    }
  }

  public void executeClearQueryObjectPool() {
    try {
      QueryObjectPoolReference reference = (QueryObjectPoolReference) referenceQueue.poll();
      while (reference != null) {
        queryPool.compute(
            reference.getQueryId(), (k, v) -> v == null || v.get() == null ? null : v);
        reference = (QueryObjectPoolReference) referenceQueue.poll();
      }
    } catch (Throwable e) {
      LOGGER.error(e.getMessage(), e);
    }
    hasReleaseTask = false;
  }

  @TestOnly
  boolean isHasReleaseTask() {
    return hasReleaseTask;
  }

  @TestOnly
  boolean hasQueryObjectPool(String queryId) {
    QueryObjectPoolReference reference = queryPool.get(queryId);
    return reference != null && reference.get() != null;
  }

  @TestOnly
  boolean hasQueryObjectPoolReference(String queryId) {
    return queryPool.containsKey(queryId);
  }

  public void clear() {
    queryPool.clear();
  }

  public static class QueryObjectPool {

    private final String queryId;

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
  }

  private static class QueryObjectPoolReference extends WeakReference<QueryObjectPool> {

    private final String queryId;

    private QueryObjectPoolReference(
        String queryId, QueryObjectPool referent, ReferenceQueue<? super QueryObjectPool> q) {
      super(referent, q);
      this.queryId = queryId;
    }

    private String getQueryId() {
      return queryId;
    }
  }
}
