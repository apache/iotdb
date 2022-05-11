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

package org.apache.iotdb.procedure;

import org.apache.iotdb.procedure.conf.ProcedureNodeConfigDescriptor;
import org.apache.iotdb.procedure.store.IProcedureStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Internal cleaner that removes the completed procedure results after a TTL. */
public class CompletedProcedureCleaner<Env> extends InternalProcedure<Env> {
  private static final Logger LOG = LoggerFactory.getLogger(CompletedProcedureCleaner.class);

  static final long CLEANER_INTERVAL =
      ProcedureNodeConfigDescriptor.getInstance().getConf().getCompletedCleanInterval();
  private static final int DEFAULT_BATCH_SIZE = 32;

  private final Map<Long, CompletedProcedureRetainer<Env>> completed;
  private final IProcedureStore store;

  public CompletedProcedureCleaner(
      IProcedureStore store, Map<Long, CompletedProcedureRetainer<Env>> completedMap) {
    super(TimeUnit.SECONDS.toMillis(CLEANER_INTERVAL));
    this.completed = completedMap;
    this.store = store;
  }

  @Override
  protected void periodicExecute(final Env env) {
    if (completed.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No completed procedures to cleanup.");
      }
      return;
    }

    final long evictTtl = ProcedureExecutor.EVICT_TTL;
    final long[] batchIds = new long[DEFAULT_BATCH_SIZE];
    int batchCount = 0;

    final long now = System.currentTimeMillis();
    final Iterator<Map.Entry<Long, CompletedProcedureRetainer<Env>>> it =
        completed.entrySet().iterator();
    while (it.hasNext() && store.isRunning()) {
      final Map.Entry<Long, CompletedProcedureRetainer<Env>> entry = it.next();
      final CompletedProcedureRetainer<Env> retainer = entry.getValue();
      final Procedure<?> proc = retainer.getProcedure();
      if (retainer.isExpired(now, evictTtl)) {
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
    // let the store do some cleanup works, i.e, delete the place marker for preserving the max
    // procedure id.
    store.cleanup();
  }
}
