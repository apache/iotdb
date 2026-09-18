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

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.commons.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.utils.cte.CteDataStore;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Holds CTE materialization state for one top-level query execution.
 *
 * <p>A relational {@link Query} AST can be cached by a prepared statement and reused by independent
 * executions. Materialized data therefore cannot be stored on that AST. This context is owned by an
 * {@link MPPQueryContext} instead and is shared only with inner queries spawned while executing the
 * same top-level query. It is designed to be accessed only by the planner thread.
 */
public final class CteMaterializationContext {

  private final Map<NodeRef<Table>, Query> cteQueries = new HashMap<>();

  // Optional.empty() records an attempted materialization that fell back to inline planning.
  private final Map<NodeRef<Query>, Optional<CteDataStore>> materializationResults =
      new HashMap<>();

  public void addCteQuery(Table table, Query query) {
    cteQueries.put(NodeRef.of(table), query);
  }

  public Map<NodeRef<Table>, Query> getCteQueries() {
    return Collections.unmodifiableMap(cteQueries);
  }

  public boolean isMaterializationAttempted(Query query) {
    return materializationResults.containsKey(NodeRef.of(query));
  }

  public void recordMaterializationResult(Query query, @Nullable CteDataStore dataStore) {
    materializationResults.put(NodeRef.of(query), Optional.ofNullable(dataStore));
  }

  @Nullable
  public CteDataStore getCteDataStore(Query query) {
    return materializationResults.getOrDefault(NodeRef.of(query), Optional.empty()).orElse(null);
  }

  @Nullable
  public CteDataStore getCteDataStore(Table table) {
    Query query = cteQueries.get(NodeRef.of(table));
    return query == null ? null : getCteDataStore(query);
  }

  public void clear() {
    cteQueries.clear();
    materializationResults.clear();
  }
}
