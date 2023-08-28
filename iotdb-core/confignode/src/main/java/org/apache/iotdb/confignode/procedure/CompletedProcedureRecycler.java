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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Internal cleaner that removes the completed procedure results after a TTL. */
public class CompletedProcedureRecycler<Env> extends InternalProcedure<Env> {
  private static final Logger LOG = LoggerFactory.getLogger(CompletedProcedureRecycler.class);
  private static final int DEFAULT_BATCH_SIZE = 32;
  private long evictTTL;
  private final Map<Long, CompletedProcedureContainer<Env>> completed;
  private final IProcedureStore store;

  public CompletedProcedureRecycler(
      IProcedureStore store,
      Map<Long, CompletedProcedureContainer<Env>> completedMap,
      long cleanTimeInterval,
      long evictTTL) {
    super(TimeUnit.SECONDS.toMillis(cleanTimeInterval));
    this.completed = completedMap;
    this.store = store;
    this.evictTTL = evictTTL;
  }

  @Override
  protected void periodicExecute(final Env env) {
    if (completed.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No completed procedures to cleanup.");
      }
      return;
    }

    final long[] batchIds = new long[DEFAULT_BATCH_SIZE];
    int batchCount = 0;

    final long now = System.currentTimeMillis();
    final Iterator<Map.Entry<Long, CompletedProcedureContainer<Env>>> it =
        completed.entrySet().iterator();
    while (it.hasNext() && store.isRunning()) {
      final Map.Entry<Long, CompletedProcedureContainer<Env>> entry = it.next();
      final CompletedProcedureContainer<Env> retainer = entry.getValue();
      final Procedure<?> proc = retainer.getProcedure();
      if (retainer.isExpired(now, evictTTL)) {
        // Failed procedures aren't persisted in WAL.
        batchIds[batchCount++] = entry.getKey();
        if (batchCount == batchIds.length) {
          store.delete(batchIds, 0, batchCount);
          batchCount = 0;
        }
        it.remove();
        LOG.trace("Evict completed {}", proc);
      }
    }
    if (batchCount > 0) {
      store.delete(batchIds, 0, batchCount);
    }
  }
}
