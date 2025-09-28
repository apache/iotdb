1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.Iterator;
1import java.util.Map;
1import java.util.concurrent.TimeUnit;
1
1/** Internal cleaner that removes the completed procedure results after a TTL. */
1public class CompletedProcedureRecycler<Env> extends InternalProcedure<Env> {
1  private static final Logger LOG = LoggerFactory.getLogger(CompletedProcedureRecycler.class);
1  private static final int DEFAULT_BATCH_SIZE = 8;
1  private final long evictTTL;
1  private final Map<Long, CompletedProcedureContainer<Env>> completed;
1  private final IProcedureStore<Env> store;
1
1  public CompletedProcedureRecycler(
1      IProcedureStore<Env> store,
1      Map<Long, CompletedProcedureContainer<Env>> completedMap,
1      long cleanTimeInterval,
1      long evictTTL) {
1    super(TimeUnit.SECONDS.toMillis(cleanTimeInterval));
1    this.completed = completedMap;
1    this.store = store;
1    this.evictTTL = evictTTL;
1  }
1
1  @Override
1  protected void periodicExecute(final Env env) {
1    if (completed.isEmpty()) {
1      if (LOG.isTraceEnabled()) {
1        LOG.trace("No completed procedures to cleanup.");
1      }
1      return;
1    }
1
1    final long[] batchIds = new long[DEFAULT_BATCH_SIZE];
1    int batchCount = 0;
1
1    final long now = System.currentTimeMillis();
1    final Iterator<Map.Entry<Long, CompletedProcedureContainer<Env>>> it =
1        completed.entrySet().iterator();
1    while (it.hasNext() && store.isRunning()) {
1      final Map.Entry<Long, CompletedProcedureContainer<Env>> entry = it.next();
1      final CompletedProcedureContainer<Env> retainer = entry.getValue();
1      final Procedure<?> proc = retainer.getProcedure();
1      if (retainer.isExpired(now, evictTTL)) {
1        // Failed procedures aren't persisted in WAL.
1        batchIds[batchCount++] = entry.getKey();
1        if (batchCount == batchIds.length) {
1          store.delete(batchIds, 0, batchCount);
1          batchCount = 0;
1        }
1        it.remove();
1        LOG.trace("Evict completed {}", proc);
1      }
1    }
1    if (batchCount > 0) {
1      store.delete(batchIds, 0, batchCount);
1    }
1  }
1}
1