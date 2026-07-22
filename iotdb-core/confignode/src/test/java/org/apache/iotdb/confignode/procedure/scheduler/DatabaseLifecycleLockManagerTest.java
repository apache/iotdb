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

package org.apache.iotdb.confignode.procedure.scheduler;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.scheduler.DatabaseLifecycleLockManager.DatabaseLock;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DatabaseLifecycleLockManagerTest {

  @Test
  public void testRequestLocksAreScopedByDatabaseName() throws Exception {
    final DatabaseLifecycleLockManager lockManager =
        new DatabaseLifecycleLockManager(Mockito.mock(ProcedureScheduler.class));
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CountDownLatch sameDatabaseAcquired = new CountDownLatch(1);
    try (final DatabaseLock ignored = lockManager.acquireLocks(Collections.singleton("root.sg"))) {
      final Future<?> sameDatabaseFuture =
          executor.submit(
              () -> {
                try (final DatabaseLock sameDatabaseLock =
                    lockManager.acquireLocks(Collections.singleton("root.sg"))) {
                  sameDatabaseAcquired.countDown();
                }
              });
      final Future<?> otherDatabaseFuture =
          executor.submit(
              () -> {
                try (final DatabaseLock otherDatabaseLock =
                    lockManager.acquireLocks(Collections.singleton("root.other"))) {
                  // Acquiring a different database proves the lock is not cluster-global.
                }
              });

      otherDatabaseFuture.get(10, TimeUnit.SECONDS);
      Assert.assertFalse(sameDatabaseAcquired.await(200, TimeUnit.MILLISECONDS));
      Assert.assertFalse(sameDatabaseFuture.isDone());
    } finally {
      executor.shutdownNow();
    }
    Assert.assertTrue(sameDatabaseAcquired.await(10, TimeUnit.SECONDS));
  }

  @Test
  public void testWaitingProcedureCannotBeOvertakenByRequest() throws Exception {
    final ProcedureScheduler scheduler = Mockito.mock(ProcedureScheduler.class);
    final DatabaseLifecycleLockManager lockManager = new DatabaseLifecycleLockManager(scheduler);
    final Procedure<?> owner = procedure(1);
    final Procedure<?> waiter = procedure(2);
    final Set<String> databases = Collections.singleton("root.sg");

    Assert.assertNull(lockManager.tryLock(owner, databases));
    Assert.assertEquals("root.sg", lockManager.tryLock(waiter, databases));
    lockManager.waitProcedure(waiter, "root.sg");

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch requestAcquired = new CountDownLatch(1);
    try {
      final Future<?> requestFuture =
          executor.submit(
              () -> {
                try (final DatabaseLock ignored = lockManager.acquireLocks(databases)) {
                  requestAcquired.countDown();
                }
              });
      Assert.assertFalse(requestAcquired.await(200, TimeUnit.MILLISECONDS));

      lockManager.releaseLocks(owner, databases);
      Mockito.verify(scheduler).addFront(waiter);
      Assert.assertFalse(requestAcquired.await(200, TimeUnit.MILLISECONDS));

      Assert.assertNull(lockManager.tryLock(waiter, databases));
      Assert.assertFalse(requestAcquired.await(200, TimeUnit.MILLISECONDS));
      lockManager.releaseLocks(waiter, databases);
      Assert.assertTrue(requestAcquired.await(10, TimeUnit.SECONDS));
      requestFuture.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testTimedRequestLockAcquisitionHonorsTimeout() throws Exception {
    final DatabaseLifecycleLockManager lockManager =
        new DatabaseLifecycleLockManager(Mockito.mock(ProcedureScheduler.class));
    final Procedure<?> owner = procedure(1);
    final Set<String> databases = Collections.singleton("root.sg");
    Assert.assertNull(lockManager.tryLock(owner, databases));

    final long startNanos = System.nanoTime();
    Assert.assertNull(lockManager.tryAcquireLocks(databases, 100, TimeUnit.MILLISECONDS));
    Assert.assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos) < 5);

    lockManager.releaseLocks(owner, databases);
  }

  @Test
  public void testFailedMultiDatabaseAcquisitionDoesNotLeakPartialLocks() {
    final DatabaseLifecycleLockManager lockManager =
        new DatabaseLifecycleLockManager(Mockito.mock(ProcedureScheduler.class));
    final Procedure<?> firstProcedure = procedure(1);
    final Procedure<?> secondProcedure = procedure(2);

    Assert.assertNull(lockManager.tryLock(firstProcedure, Collections.singleton("root.b")));
    Assert.assertEquals("root.b", lockManager.tryLock(secondProcedure, Set.of("root.a", "root.b")));

    try (final DatabaseLock ignored = lockManager.acquireLocks(Collections.singleton("root.a"))) {
      // The partial root.a acquisition of secondProcedure must have been released.
    }
    lockManager.releaseLocks(firstProcedure, Collections.singleton("root.b"));
  }

  @Test
  public void testRequestLocksAreReentrantOnTheOwningThread() {
    final DatabaseLifecycleLockManager lockManager =
        new DatabaseLifecycleLockManager(Mockito.mock(ProcedureScheduler.class));
    final Procedure<?> procedure = procedure(1);
    final Set<String> databases = Collections.singleton("root.sg");

    try (final DatabaseLock outerLock = lockManager.acquireLocks(databases)) {
      try (final DatabaseLock innerLock = lockManager.acquireLocks(databases)) {
        Assert.assertEquals("root.sg", lockManager.tryLock(procedure, databases));
      }
      Assert.assertEquals("root.sg", lockManager.tryLock(procedure, databases));
    }
    Assert.assertNull(lockManager.tryLock(procedure, databases));
    lockManager.releaseLocks(procedure, databases);
  }

  private static Procedure<?> procedure(final long procedureId) {
    final Procedure<?> procedure = Mockito.mock(Procedure.class);
    Mockito.when(procedure.getProcId()).thenReturn(procedureId);
    return procedure;
  }
}
