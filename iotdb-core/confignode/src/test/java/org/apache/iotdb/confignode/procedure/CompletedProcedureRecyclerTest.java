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

import org.apache.iotdb.confignode.procedure.entity.NoopProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CompletedProcedureRecyclerTest {

  @SuppressWarnings("unchecked")
  private static IProcedureStore<TestProcEnv> runningStore() {
    NoopProcedureStore store = new NoopProcedureStore();
    store.setRunning(true);
    return store;
  }

  private static CompletedProcedureContainer<TestProcEnv> completedAt(long lastUpdate) {
    NoopProcedure procedure = new NoopProcedure();
    procedure.setLastUpdate(lastUpdate);
    return new CompletedProcedureContainer<>(procedure);
  }

  /** The evict TTL is configured in seconds but compared against a millisecond delta. */
  @Test
  public void evictTtlIsConvertedFromSecondsToMillis() {
    CompletedProcedureRecycler<TestProcEnv> recycler =
        new CompletedProcedureRecycler<>(runningStore(), new ConcurrentHashMap<>(), 30, 60);
    Assert.assertEquals(TimeUnit.SECONDS.toMillis(60), recycler.getEvictTTLInMs());
  }

  @Test
  public void freshCompletedProcedureIsRetainedWhileStaleOneIsEvicted() {
    final long now = System.currentTimeMillis();
    final Map<Long, CompletedProcedureContainer<TestProcEnv>> completed = new ConcurrentHashMap<>();
    // Completed 1s ago: with a 100s evict TTL it must survive. Before the seconds->millis fix a
    // 100s TTL was treated as 100ms, so this fresh entry was evicted almost immediately.
    completed.put(1L, completedAt(now - TimeUnit.SECONDS.toMillis(1)));
    // Completed 300s ago: with a 100s evict TTL it must be evicted.
    completed.put(2L, completedAt(now - TimeUnit.SECONDS.toMillis(300)));

    CompletedProcedureRecycler<TestProcEnv> recycler =
        new CompletedProcedureRecycler<>(runningStore(), completed, 30, 100);
    recycler.periodicExecute(null);

    Assert.assertTrue(
        "A procedure completed 1s ago must survive a 100s evict TTL", completed.containsKey(1L));
    Assert.assertFalse(
        "A procedure completed 300s ago must be evicted by a 100s evict TTL",
        completed.containsKey(2L));
  }
}
