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

package org.apache.iotdb.db.schemaengine.lease;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetadataLeaseManagerTest {

  private static final long T_FENCE_MS = 20_000L;

  private MetadataLeaseManager newManager(final AtomicLong nowNanos) {
    return new MetadataLeaseManager(nowNanos::get, () -> T_FENCE_MS);
  }

  @Test
  public void notFencedWithinThresholdAfterHeartbeat() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos);
    manager.recordConfigNodeHeartbeat();
    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS - 1));
    assertFalse(manager.isFenced());
  }

  @Test
  public void fencedAfterThresholdElapsedWithoutHeartbeat() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos);
    manager.recordConfigNodeHeartbeat();
    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS + 1));
    assertTrue(manager.isFenced());
  }

  @Test
  public void recoversFromFencedAfterNewHeartbeat() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos);
    manager.recordConfigNodeHeartbeat();
    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS + 1));
    assertTrue(manager.isFenced());

    manager.recordConfigNodeHeartbeat();
    assertFalse(manager.isFenced());
  }

  @Test
  public void reportsMillisSinceLastHeartbeat() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos);
    manager.recordConfigNodeHeartbeat();
    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1234));
    assertEquals(1234L, manager.getMillisSinceLastConfigNodeHeartbeat());
  }

  @Test
  public void runsRecoveryListenerWhenHeartbeatArrivesAfterFence() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos);
    final AtomicInteger recoveries = new AtomicInteger();
    manager.addLeaseRecoveryListener(recoveries::incrementAndGet);

    manager.recordConfigNodeHeartbeat();
    assertEquals(0, recoveries.get());

    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS + 1));
    assertTrue(manager.isFenced());

    manager.recordConfigNodeHeartbeat();
    assertEquals(1, recoveries.get());
    assertFalse(manager.isFenced());
  }

  @Test
  public void doesNotRunRecoveryListenerWhenLeaseNeverExpired() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos);
    final AtomicInteger recoveries = new AtomicInteger();
    manager.addLeaseRecoveryListener(recoveries::incrementAndGet);

    for (int i = 0; i < 3; i++) {
      nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1_000));
      manager.recordConfigNodeHeartbeat();
    }
    assertEquals(0, recoveries.get());
  }
}
