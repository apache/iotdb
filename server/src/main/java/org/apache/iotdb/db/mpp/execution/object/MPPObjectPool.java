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

package org.apache.iotdb.db.mpp.execution.object;

import org.apache.iotdb.db.mpp.common.QueryId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MPPObjectPool {

  private final Map<QueryId, List<ObjectEntry>> objectPool = new ConcurrentHashMap<>();

  private MPPObjectPool() {}

  private static class MPPObjectPoolHolder {

    private static final MPPObjectPool INSTANCE = new MPPObjectPool();

    private MPPObjectPoolHolder() {}
  }

  public static MPPObjectPool getInstance() {
    return MPPObjectPoolHolder.INSTANCE;
  }

  public synchronized <T extends ObjectEntry> T put(QueryId queryId, T objectEntry) {
    List<ObjectEntry> queryObjectList =
        objectPool.computeIfAbsent(queryId, k -> Collections.synchronizedList(new ArrayList<>()));
    queryObjectList.add(objectEntry);
    objectEntry.setId(queryObjectList.size() - 1);
    return objectEntry;
  }

  @SuppressWarnings("unchecked")
  public <T extends ObjectEntry> T get(QueryId queryId, int objectId) {
    List<ObjectEntry> queryObjectList = objectPool.get(queryId);
    if (queryObjectList == null) {
      return null;
    }
    return (T) queryObjectList.get(objectId);
  }

  public void clear(QueryId queryId) {
    objectPool.remove(queryId);
  }

  public void clear() {
    objectPool.clear();
  }
}
