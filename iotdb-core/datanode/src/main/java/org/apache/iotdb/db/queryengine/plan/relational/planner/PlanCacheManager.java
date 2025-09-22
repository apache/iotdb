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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** LRU Cache with dual restrictions on cache quantity and memory */
public class PlanCacheManager {
  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(PlanCacheManager.class);

  private static class SingletonHolder {
    private static final PlanCacheManager INSTANCE = new PlanCacheManager();
  }

  private static final int MAX_CACHE_SIZE = 1000;
  private static final long MAX_MEMORY_BYTES = 64L * 1024 * 1024; // 64 MB

  private final AtomicLong currentMemoryBytes = new AtomicLong(INSTANCE_SIZE);

  private final Map<String, CachedValue> planCache;

  private PlanCacheManager() {
    this.planCache = new LinkedHashMap<>(16, 0.75f, true);
  }

  public static PlanCacheManager getInstance() {
    return SingletonHolder.INSTANCE;
  }

  public void cacheValue(
      String cachedKey,
      PlanNode planNode,
      List<DeviceTableScanNode> scanNodes,
      List<Literal> literalReference,
      DatasetHeader header,
      HashMap<Symbol, Type> symbolMap,
      int symbolNextId,
      List<List<Expression>> metadataExpressionLists,
      List<List<String>> attributeColumnsLists,
      List<Map<Symbol, ColumnSchema>> assignmentsLists) {
    CachedValue newValue =
        new CachedValue(
            planNode,
            scanNodes,
            literalReference,
            header,
            symbolMap,
            symbolNextId,
            metadataExpressionLists,
            attributeColumnsLists,
            assignmentsLists);

    long keySize = RamUsageEstimator.sizeOf(cachedKey);
    long newValueSize = newValue.estimateMemoryUsage();

    synchronized (planCache) {
      planCache.put(cachedKey, newValue);
      currentMemoryBytes.addAndGet(keySize + newValueSize);

      Iterator<Map.Entry<String, CachedValue>> iterator = planCache.entrySet().iterator();
      while ((currentMemoryBytes.get() > MAX_MEMORY_BYTES || planCache.size() > MAX_CACHE_SIZE)
          && iterator.hasNext()) {
        Map.Entry<String, CachedValue> eldest = iterator.next();

        CachedValue evicted = eldest.getValue();
        long evictedKeySize = RamUsageEstimator.sizeOf(eldest.getKey());
        long evictedValueSize = evicted.estimateMemoryUsage();
        iterator.remove();
        currentMemoryBytes.addAndGet(-(evictedKeySize + evictedValueSize));
      }
    }
  }

  public int size() {
    return planCache.size();
  }

  public CachedValue getCachedValue(String cacheKey) {
    synchronized (planCache) {
      return planCache.get(cacheKey);
    }
  }

  public long getCurrentMemoryBytes() {
    return currentMemoryBytes.get();
  }

  public void clear() {
    synchronized (planCache) {
      planCache.clear();
      currentMemoryBytes.set(0);
    }
  }
}
