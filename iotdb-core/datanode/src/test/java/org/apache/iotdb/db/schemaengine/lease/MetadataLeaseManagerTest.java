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

import static org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils.T_FENCE_MS;
import static org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils.newManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetadataLeaseManagerTest {

  @Test
  public void isNotFencedWithinLease() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos, () -> {}, () -> {});

    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1234));

    assertFalse(manager.isFenced());
    assertEquals(1234L, manager.getMillisSinceLastConfigNodeHeartbeat());
  }

  @Test
  public void recoversAfterHeartbeatWhenLeaseExpired() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final MetadataLeaseManager manager = newManager(nowNanos, () -> {}, () -> {});

    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS + 1));
    manager.checkLeaseStatus();
    assertTrue(manager.isFenced());

    manager.recoveryLeaseForTest(true);

    assertFalse(manager.isFenced());
  }

  @Test
  public void retriesCacheClearInHeartbeatWorkerAfterFailure() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final AtomicInteger clearAttempts = new AtomicInteger();
    final MetadataLeaseManager manager =
        newManager(
            nowNanos,
            () -> {
              if (clearAttempts.getAndIncrement() == 0) {
                throw new RuntimeException("mock clear cache failure");
              }
            },
            () -> {});

    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS + 1));
    manager.checkLeaseStatus();
    assertTrue(manager.isFenced());
    assertEquals(0, clearAttempts.get());

    manager.triggerCheckWithHeartBeat();
    assertTrue(manager.isFenced());
    assertEquals(1, clearAttempts.get());

    manager.triggerCheckWithHeartBeat();
    assertFalse(manager.isFenced());
    assertEquals(2, clearAttempts.get());
  }

  @Test
  public void retriesMetadataPullAfterFailure() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final AtomicInteger pullAttempts = new AtomicInteger();
    final MetadataLeaseManager manager =
        newManager(
            nowNanos,
            () -> {},
            () -> {
              if (pullAttempts.getAndIncrement() == 0) {
                throw new RuntimeException("mock pull failure");
              }
            });

    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(T_FENCE_MS + 1));
    manager.checkLeaseStatus();
    assertTrue(manager.isFenced());

    manager.triggerCheckWithHeartBeat();
    assertTrue(manager.isFenced());

    manager.triggerCheckWithHeartBeat();
    assertFalse(manager.isFenced());
    assertEquals(2, pullAttempts.get());
  }
}
