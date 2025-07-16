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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PlanCacheManager {

  private static class SingletonHolder {
    private static final PlanCacheManager INSTANCE = new PlanCacheManager();
  }

  private final Map<String, CachedValue> planCache;

  private PlanCacheManager() {
    planCache = new ConcurrentHashMap<>();
  }

  public static PlanCacheManager getInstance() {
    return SingletonHolder.INSTANCE;
  }

  public void cacheValue(
      String cachedKey,
      PlanNode planNodeTree,
      List<Literal> literalReference,
      DatasetHeader header,
      HashMap<Symbol, Type> symbolMap,
      Map<Symbol, ColumnSchema> assignments,
      List<Expression> expressionList,
      List<String> columnAttributes) {
    planCache.put(
        cachedKey,
        new CachedValue(
            planNodeTree,
            literalReference,
            header,
            symbolMap,
            assignments,
            expressionList,
            columnAttributes));
  }

  public CachedValue getCachedValue(String cacheKey) {
    return planCache.get(cacheKey);
  }

  /*
  TODO: add LRU strategy
   缓存数量和内存大小
  */
}
