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

package org.apache.iotdb.db.queryengine.execution.memory;

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.exception.runtime.MemoryLeakException;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

public class MemoryPoolTest {

  MemoryPool pool;

  private final String QUERY_ID = "q0";

  private final String FRAGMENT_INSTANCE_ID = "f0";
  private final String PLAN_NODE_ID = "p0";

  @Before
  public void before() {
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(1024L));
    pool = new MemoryPool("test", memoryManager, 512L);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
  }

  @Test
  public void testDeregisterDoesNotRemoveReregisteredQueryMemory() throws Exception {
    Field reservationsField = MemoryPool.class.getDeclaredField("queryMemoryReservations");
    reservationsField.setAccessible(true);
    Object reservationsLock = reservationsField.get(pool);
    long lockOwnerId = Thread.currentThread().getId();
    FutureTask<Void> deregistration =
        new FutureTask<>(
            () -> {
              // An empty fragment may finish without registering any memory.
              pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "empty", true);
              return null;
            });
    Thread cleanupThread = new Thread(deregistration, "memory-pool-stale-deregister");
    try {
      synchronized (reservationsLock) {
        cleanupThread.start();
        // Hold the registration lock until the empty fragment cleanup reaches it.
        await()
            .atMost(10, TimeUnit.SECONDS)
            .until(
                () -> {
                  ThreadInfo info =
                      ManagementFactory.getThreadMXBean().getThreadInfo(cleanupThread.getId());
                  return info != null
                      && info.getThreadState() == Thread.State.BLOCKED
                      && info.getLockOwnerId() == lockOwnerId
                      && info.getLockInfo().getIdentityHashCode()
                          == System.identityHashCode(reservationsLock);
                });

        // Replace the query map while the delayed cleanup still refers to the old one.
        pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
        Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
        pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "result", PLAN_NODE_ID, MemorySide.INPUT);
        Assert.assertTrue(
            pool.tryReserveForTest(QUERY_ID, "result", PLAN_NODE_ID, 256L, Long.MAX_VALUE));
      }
      deregistration.get(10, TimeUnit.SECONDS);

      Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
      Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
      Assert.assertEquals(256L, pool.getReservedBytes());
      pool.free(QUERY_ID, "result", PLAN_NODE_ID, 256L);
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "result", true);
      Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
      Assert.assertEquals(0L, pool.getReservedBytes());
    } finally {
      cleanupThread.join(TimeUnit.SECONDS.toMillis(10));
      Assert.assertFalse(cleanupThread.isAlive());
    }
  }

  @Test
  public void testDeregisterMissingFragmentPreservesReservedMemory() {
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "empty", true);

    Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testDeregisterPreservesOtherFragments() {
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "other", PLAN_NODE_ID, MemorySide.INPUT);
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, "other", PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);

    Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    pool.free(QUERY_ID, "other", PLAN_NODE_ID, 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "other", true);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testDeregisterWaitsForMemoryRelease() {
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, false);

    Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, false);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());

    // Repeated cleanup after the query has been removed remains a no-op.
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testForceDeregisterReportsMemoryLeak() {
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    Assert.assertThrows(
        MemoryLeakException.class,
        () ->
            pool.deRegisterFragmentInstanceFromQueryMemoryMap(
                QUERY_ID, FRAGMENT_INSTANCE_ID, true));

    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testDeregisterDetachesPendingReservationsBeforeUnrelatedFree() {
    pool.registerPlanNodeIdToQueryMemoryMap("healthy", "fi", "a", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap("healthy", "fi", "b", MemorySide.INPUT);
    Assert.assertTrue(pool.tryReserve("healthy", "fi", "a", 512, 512));
    Assert.assertTrue(pool.tryReserve("healthy", "fi", "b", 512, 512));
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 512, false);
    Assert.assertFalse(pending.getFuture().isDone());

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);

    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertEquals(256, pool.tryCancel(pending.getFuture()));
    pool.free("healthy", "fi", "a", 512);
    pool.free("healthy", "fi", "b", 512);
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testForceDeregisterDetachesPendingReservationsEvenWhenReportingLeak() {
    Assert.assertTrue(pool.tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512, 512));
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 512, false);
    pool.registerPlanNodeIdToQueryMemoryMap("healthy", "fi", "a", MemorySide.INPUT);
    Assert.assertTrue(pool.tryReserve("healthy", "fi", "a", 256, 512));

    Assert.assertThrows(
        MemoryLeakException.class,
        () ->
            pool.deRegisterFragmentInstanceFromQueryMemoryMap(
                QUERY_ID, FRAGMENT_INSTANCE_ID, true));

    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertEquals(256, pool.tryCancel(pending.getFuture()));
    pool.free("healthy", "fi", "a", 256);
    // Forced cleanup still reports and retains the original leaked allocation.
    Assert.assertEquals(512, pool.getReservedBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPublicationIntoDeregisteredFragmentIsRejected() {
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
    pool.markFragmentDeregisteredForTest(QUERY_ID, FRAGMENT_INSTANCE_ID);
    // The account is still in the map but marked deregistered, so publication must be refused
    // without charging any bytes.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 32, 512, false));
    Assert.assertEquals(0, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
  }

  @Test
  public void testDeregistrationDoesNotPoisonOtherQueries() throws Exception {
    // Fill the pool from an unrelated fragment so that both later waiters block on capacity.
    pool.registerPlanNodeIdToQueryMemoryMap("other", "fi", "o1", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap("other", "fi", "o2", MemorySide.INPUT);
    Assert.assertTrue(pool.tryReserve("other", "fi", "o1", 512, 512));
    Assert.assertTrue(pool.tryReserve("other", "fi", "o2", 512, 512));
    // A pending waiter in the fragment about to be deregistered, and one in another fragment.
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
    MemoryPool.MemoryReservationResult detached =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 32, 512, false);
    Assert.assertFalse(detached.getFuture().isDone());
    pool.registerPlanNodeIdToQueryMemoryMap("waiting", "fi", "later", MemorySide.INPUT);
    MemoryPool.MemoryReservationResult healthy =
        pool.reserveWithPriority("waiting", "fi", "later", 32, 512, false);
    Assert.assertFalse(healthy.getFuture().isDone());
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    // The deregistered fragment's waiter is detached without completion; the other one is
    // untouched and stays wakeable by released capacity.
    Assert.assertFalse(detached.getFuture().isDone());
    Assert.assertFalse(healthy.getFuture().isDone());
    pool.free("other", "fi", "o1", 512);
    Assert.assertTrue(healthy.getFuture().isDone());
    pool.free("other", "fi", "o2", 512);
    Assert.assertEquals(32, pool.tryCancel(detached.getFuture()));
    pool.free("waiting", "fi", "later", 32);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("waiting", "fi", true);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("other", "fi", true);
    Assert.assertEquals(0, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  @Test
  public void testDetachedWaiterDoesNotUseRecreatedFragmentAccount() {
    pool.registerPlanNodeIdToQueryMemoryMap("holder", "fi", "a", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap("holder", "fi", "b", MemorySide.INPUT);
    Assert.assertTrue(pool.tryReserve("holder", "fi", "a", 512, 512));
    Assert.assertTrue(pool.tryReserve("holder", "fi", "b", 512, 512));
    MemoryReservationResult old =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 32, 512, false);
    AtomicInteger callbacks = new AtomicInteger();
    old.getFuture().addListener(callbacks::incrementAndGet, Runnable::run);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    Assert.assertFalse(old.getFuture().isDone());
    Assert.assertEquals(0, callbacks.get());
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
    MemoryReservationResult replacement =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 32, 512, false);
    Assert.assertEquals(32, pool.tryCancel(old.getFuture()));
    Assert.assertEquals(1, pool.getMemoryReservationSize());
    pool.free("holder", "fi", "a", 512);
    Assert.assertTrue(replacement.getFuture().isDone());
    Assert.assertFalse(replacement.getFuture().isCancelled());
    pool.free("holder", "fi", "b", 512);
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 32);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("holder", "fi", true);
    Assert.assertEquals(0, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  @Test
  public void testNonForcedDeregisterKeepsPendingReservationWhileMemoryIsLive() {
    Assert.assertTrue(pool.tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512, 512));
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 512, false);

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, false);

    Assert.assertFalse(pending.getFuture().isDone());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512);
    Assert.assertTrue(pending.getFuture().isDone());
    Assert.assertFalse(pending.getFuture().isCancelled());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, false);
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testDeregisterDoesNotCancelOtherFragmentPendingReservation() {
    pool.registerPlanNodeIdToQueryMemoryMap("healthy", "fi", "a", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap("healthy", "fi", "b", MemorySide.INPUT);
    Assert.assertTrue(pool.tryReserve("healthy", "fi", "a", 512, 512));
    Assert.assertTrue(pool.tryReserve("healthy", "fi", "b", 512, 512));
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "other", PLAN_NODE_ID, MemorySide.INPUT);
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 512, false);
    MemoryReservationResult other =
        pool.reserveWithPriority(QUERY_ID, "other", PLAN_NODE_ID, 256, 512, false);

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);

    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertEquals(256, pool.tryCancel(pending.getFuture()));
    Assert.assertFalse(other.getFuture().isDone());
    pool.free("healthy", "fi", "a", 512);
    Assert.assertTrue(other.getFuture().isDone());
    Assert.assertFalse(other.getFuture().isCancelled());
    pool.free(QUERY_ID, "other", PLAN_NODE_ID, 256);
    pool.free("healthy", "fi", "b", 512);
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testOwnerCancellationAndDeregistrationDoNotScanUnrelatedWaiters() throws Exception {
    assertDeregistrationDoesNotScanUnrelatedWaiters(true);
  }

  @Test
  public void testDeregistrationDetachesOnlyIndexedWaiters() throws Exception {
    assertDeregistrationDoesNotScanUnrelatedWaiters(false);
  }

  private void assertDeregistrationDoesNotScanUnrelatedWaiters(boolean ownerCancels)
      throws Exception {
    AtomicInteger visits = countGlobalPendingVisits();
    pool.tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512, 512);
    pool.registerPlanNodeIdToQueryMemoryMap("holder", "fi", "p", MemorySide.INPUT);
    pool.tryReserve("holder", "fi", "p", 512, 512);
    int count = 1024;
    List<MemoryReservationResult> victims = new ArrayList<>();
    List<MemoryReservationResult> unrelated = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String fi = Integer.toString(i);
      pool.registerPlanNodeIdToQueryMemoryMap("victim", fi, "p", MemorySide.INPUT);
      pool.registerPlanNodeIdToQueryMemoryMap("unrelated", fi, "p", MemorySide.INPUT);
      victims.add(pool.reserveWithPriority("victim", fi, "p", 8, 512, false));
      unrelated.add(pool.reserveWithPriority("unrelated", fi, "p", 8, 512, false));
    }
    for (int i = 0; i < count; i++) {
      if (ownerCancels) {
        Assert.assertEquals(8, pool.tryCancel(victims.get(i).getFuture()));
      }
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("victim", Integer.toString(i), true);
      if (!ownerCancels) {
        Assert.assertFalse(victims.get(i).getFuture().isDone());
        Assert.assertEquals(8, pool.tryCancel(victims.get(i).getFuture()));
      }
      Assert.assertTrue(victims.get(i).getFuture().isCancelled());
      Assert.assertFalse(unrelated.get(i).getFuture().isDone());
    }
    // An operation-count assertion avoids timing noise and detects a global scan for every FI.
    Assert.assertEquals(0, visits.get());
    Assert.assertEquals(count, pool.getMemoryReservationSize());
    for (int i = 0; i < count; i++) {
      Assert.assertEquals(8, pool.tryCancel(unrelated.get(i).getFuture()));
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("unrelated", Integer.toString(i), true);
    }
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512);
    pool.free("holder", "fi", "p", 512);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("holder", "fi", true);
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @SuppressWarnings("unchecked")
  private AtomicInteger countGlobalPendingVisits() throws Exception {
    AtomicInteger visits = new AtomicInteger();
    Field field = MemoryPool.class.getDeclaredField("memoryReservationFutures");
    field.setAccessible(true);
    Object pending = field.get(pool);
    if (pending instanceof Queue) {
      // Keep this branch so the regression also runs against the former queue implementation.
      Queue<MemoryPool.MemoryReservationFuture<Void>> counted =
          new ConcurrentLinkedQueue<MemoryPool.MemoryReservationFuture<Void>>() {
            @Override
            public Iterator<MemoryPool.MemoryReservationFuture<Void>> iterator() {
              return countVisits(super.iterator(), visits);
            }
          };
      counted.addAll((Queue<MemoryPool.MemoryReservationFuture<Void>>) pending);
      field.set(pool, counted);
    } else {
      Map<Long, MemoryPool.MemoryReservationFuture<Void>> counted =
          new ConcurrentSkipListMap<Long, MemoryPool.MemoryReservationFuture<Void>>() {
            @Override
            public Collection<MemoryPool.MemoryReservationFuture<Void>> values() {
              Collection<MemoryPool.MemoryReservationFuture<Void>> values = super.values();
              return new AbstractCollection<MemoryPool.MemoryReservationFuture<Void>>() {
                @Override
                public Iterator<MemoryPool.MemoryReservationFuture<Void>> iterator() {
                  return countVisits(values.iterator(), visits);
                }

                @Override
                public int size() {
                  return values.size();
                }
              };
            }
          };
      counted.putAll((Map<Long, MemoryPool.MemoryReservationFuture<Void>>) pending);
      field.set(pool, counted);
    }
    return visits;
  }

  private static <T> Iterator<T> countVisits(Iterator<T> delegate, AtomicInteger visits) {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public T next() {
        visits.incrementAndGet();
        return delegate.next();
      }

      @Override
      public void remove() {
        delegate.remove();
      }
    };
  }

  @Test
  public void testCancellationKeepsFifoWakeupOrder() {
    pool.tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512, 512);
    pool.registerPlanNodeIdToQueryMemoryMap("holder", "fi", "p", MemorySide.INPUT);
    pool.tryReserve("holder", "fi", "p", 512, 512);
    List<MemoryReservationResult> requests = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String fi = Integer.toString(i);
      pool.registerPlanNodeIdToQueryMemoryMap("waiting", fi, "p", MemorySide.INPUT);
      requests.add(pool.reserveWithPriority("waiting", fi, "p", 64, 512, false));
    }
    Assert.assertEquals(64, pool.tryCancel(requests.get(1).getFuture()));
    Assert.assertEquals(0, pool.tryCancel(requests.get(1).getFuture()));
    Assert.assertEquals(2, pool.getMemoryReservationSize());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 64);
    Assert.assertTrue(requests.get(0).getFuture().isDone());
    Assert.assertFalse(requests.get(2).getFuture().isDone());
    pool.free("waiting", "0", "p", 64);
    Assert.assertTrue(requests.get(2).getFuture().isDone());
    pool.free("waiting", "2", "p", 64);
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 448);
    pool.free("holder", "fi", "p", 512);
    for (int i = 0; i < 3; i++) {
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("waiting", Integer.toString(i), true);
    }
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testConcurrentCancellationAndGrantRemoveAllPendingIndexes() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 32; round++) {
        MemoryPool concurrent = new MemoryPool("race", new MemoryManager(1024), 512);
        concurrent.registerPlanNodeIdToQueryMemoryMap("holder", "fi", "a", MemorySide.INPUT);
        concurrent.registerPlanNodeIdToQueryMemoryMap("holder", "fi", "b", MemorySide.INPUT);
        concurrent.tryReserve("holder", "fi", "a", 512, 512);
        concurrent.tryReserve("holder", "fi", "b", 512, 512);
        List<MemoryReservationResult> requests = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
          String fi = Integer.toString(i);
          concurrent.registerPlanNodeIdToQueryMemoryMap("waiting", fi, "p", MemorySide.INPUT);
          requests.add(concurrent.reserveWithPriority("waiting", fi, "p", 16, 512, false));
        }
        CountDownLatch start = new CountDownLatch(1);
        Future<?> grant =
            executor.submit(
                () -> {
                  start.await();
                  concurrent.free("holder", "fi", "a", 512);
                  return null;
                });
        Future<?> cancel =
            executor.submit(
                () -> {
                  start.await();
                  for (MemoryReservationResult request : requests) {
                    concurrent.tryCancel(request.getFuture());
                  }
                  return null;
                });
        start.countDown();
        grant.get(10, TimeUnit.SECONDS);
        cancel.get(10, TimeUnit.SECONDS);
        for (int i = 0; i < requests.size(); i++) {
          Assert.assertTrue(requests.get(i).getFuture().isDone());
          if (!requests.get(i).getFuture().isCancelled()) {
            concurrent.free("waiting", Integer.toString(i), "p", 16);
          }
          concurrent.deRegisterFragmentInstanceFromQueryMemoryMap(
              "waiting", Integer.toString(i), true);
        }
        concurrent.free("holder", "fi", "b", 512);
        concurrent.deRegisterFragmentInstanceFromQueryMemoryMap("holder", "fi", true);
        Assert.assertEquals(0, concurrent.getMemoryReservationSize());
        Assert.assertEquals(0, concurrent.getQueryMemoryReservationSize());
        Assert.assertEquals(0, concurrent.getReservedBytes());
      }
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testTryReserve() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testTryReserveZero() {

    try {
      pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 0L, Long.MAX_VALUE);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryReserveNegative() {

    try {
      pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, -1L, Long.MAX_VALUE);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryReserveAll() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
  }

  @Test
  public void testOverTryReserve() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, 512L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
    Assert.assertFalse(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 511L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testReserve() {

    ListenableFuture<Void> future =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void tesReserveZero() {

    try {
      pool.reserveWithPriority(
          QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 0L, Long.MAX_VALUE, false);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveNegative() {

    try {
      pool.reserveWithPriority(
          QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, -1L, Long.MAX_VALUE, false);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveAll() {

    ListenableFuture<Void> future =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
  }

  @Test
  public void testOverReserve() {

    ListenableFuture<Void> future =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
    future =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testReserveAndFree() {

    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .getFuture()
            .isDone());
    ListenableFuture<Void> future =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512);
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
    Assert.assertTrue(future.isDone());
  }

  @Test
  public void testMultiReserveAndFree() {

    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture()
            .isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());

    ListenableFuture<Void> future1 =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    ListenableFuture<Void> future2 =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    ListenableFuture<Void> future3 =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    Assert.assertFalse(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    Assert.assertTrue(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertTrue(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertTrue(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testFree() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testFreeAll() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testFreeZero() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 0L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testFreeNegative() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, -1L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testOverFree() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 513L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryCancelBlockedReservation() {

    // Run out of memory.
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));

    ListenableFuture<Void> f =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, 512L, false)
            .getFuture();
    Assert.assertFalse(f.isDone());
    // Cancel the reservation.
    Assert.assertEquals(256L, pool.tryCancel(f));
    Assert.assertTrue(f.isDone());
    Assert.assertTrue(f.isCancelled());
  }

  @Test
  public void testTryCancelCompletedReservation() {

    ListenableFuture<Void> f =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(f.isDone());
    // Cancel the reservation.
    Assert.assertEquals(0L, pool.tryCancel(f));
    Assert.assertTrue(f.isDone());
    Assert.assertFalse(f.isCancelled());
  }

  /**
   * Normal query: requested bytes exceed what the pool can still provide — reserve fails (blocked
   * future, not immediate success).
   */
  @Test
  public void testReserveWithPriorityNormalQueryExceedsAvailable() {
    MemoryReservationResult r1 =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false);
    Assert.assertTrue(r1.isReserveSuccess());
    Assert.assertEquals(512L, r1.getReservedBytes());
    Assert.assertTrue(r1.getFuture().isDone());

    MemoryReservationResult r2 =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false);
    Assert.assertTrue(r2.isReserveSuccess());
    Assert.assertEquals(512L, r2.getReservedBytes());
    Assert.assertEquals(1024L, pool.getReservedBytes());

    MemoryReservationResult r3 =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false);
    Assert.assertFalse(r3.isReserveSuccess());
    Assert.assertEquals(256L, r3.getReservedBytes());
    Assert.assertFalse(r3.getFuture().isDone());
    Assert.assertEquals(1024L, pool.getReservedBytes());
  }

  /** SHOW QUERIES path: exceeds pool capacity — treated as success with zero bytes from pool. */
  @Test
  public void testReserveWithPriorityShowQueriesExceedsAvailable() {
    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .isReserveSuccess());
    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .isReserveSuccess());
    Assert.assertEquals(1024L, pool.getReservedBytes());

    MemoryReservationResult r =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, true);
    Assert.assertTrue(r.isReserveSuccess());
    Assert.assertEquals(0L, r.getReservedBytes());
    Assert.assertTrue(r.getFuture().isDone());
    Assert.assertEquals(1024L, pool.getReservedBytes());
  }

  /** SHOW QUERIES path: pool has room — same as normal successful reserve. */
  @Test
  public void testReserveWithPriorityShowQueriesWithinAvailable() {
    MemoryReservationResult r =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, true);
    Assert.assertTrue(r.isReserveSuccess());
    Assert.assertEquals(256L, r.getReservedBytes());
    Assert.assertTrue(r.getFuture().isDone());
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testSharedInputBudgetAndIndependentOutput() {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "output", MemorySide.OUTPUT);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 200, 256, false)
            .isReserveSuccess());
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100, 256, false);
    Assert.assertFalse(pending.isReserveSuccess());
    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertEquals(
        200, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    Assert.assertEquals(
        0, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2"));
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 256, 256, false)
            .isReserveSuccess());
    Assert.assertEquals(456, pool.getReservedBytes());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 200);
    Assert.assertTrue(pending.getFuture().isDone());
    Assert.assertEquals(
        100, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    Assert.assertEquals(
        256,
        pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.OUTPUT));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100);
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 256);
    Assert.assertEquals(0, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
  }

  @Test
  public void testSideRegistrationCannotChange() {
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
    try {
      pool.registerPlanNodeIdToQueryMemoryMap(
          QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.OUTPUT);
      Assert.fail("conflicting side must be rejected");
    } catch (IllegalArgumentException expected) {
      Assert.assertEquals(0, pool.getReservedBytes());
    }
  }

  @Test
  public void testRequestsUseCurrentSuppliedLimitInsteadOfPoolCreationBudget() {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 768, 768, false)
            .isReserveSuccess());
    Assert.assertEquals(768, pool.getReservedBytes());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 768);
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testRequestLargerThanGlobalCapacityDoesNotWait() {
    try {
      pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 1025, 2048, false);
      Assert.fail("a request larger than the global pool cannot become ready");
    } catch (IllegalArgumentException expected) {
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getMemoryReservationSize());
    }
  }

  @Test
  public void testSharedRequestCannotExceedSuppliedInputBudget() {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    try {
      pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 257, 256, false);
      Assert.fail("request exceeds the supplied input budget");
    } catch (IllegalArgumentException expected) {
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
      Assert.assertEquals(0, pool.getMemoryReservationSize());
    }
  }

  @Test
  public void testSharedReservationsAndWakeupsUseSuppliedSideLimits() throws Exception {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "output", MemorySide.OUTPUT);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 200, 256, false)
            .isReserveSuccess());
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100, 256, false);
    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 256, 256, false)
            .isReserveSuccess());
    // An OUTPUT release cannot grant an INPUT request whose shared side remains full.
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 256);
    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertFalse(pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100, 256));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 200);
    pending.getFuture().get(10, TimeUnit.SECONDS);
    Assert.assertEquals(
        100, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100);
    Assert.assertEquals(0, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getMemoryReservationSize());
  }

  @Test
  public void testSingleDirectionKeepsFullBudgetAndSingleBlockLimit() {
    MemoryPool singleDirectionPool =
        new MemoryPool("single-direction", new MemoryManager(1024), 513);
    for (MemorySide side : MemorySide.values()) {
      String fi = side.name();
      singleDirectionPool.registerFragmentInstanceSharedBudget(QUERY_ID, fi);
      singleDirectionPool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, fi, "active", side);
      singleDirectionPool.registerPlanNodeIdToQueryMemoryMap(
          QUERY_ID, fi, "unused", side == MemorySide.INPUT ? MemorySide.OUTPUT : MemorySide.INPUT);
      Assert.assertTrue(
          singleDirectionPool
              .reserveWithPriority(QUERY_ID, fi, "active", 513, 513, false)
              .isReserveSuccess());
      try {
        singleDirectionPool.reserveWithPriority(QUERY_ID, fi, "unused", 1, 0, false);
        Assert.fail("an unused direction supplies a zero limit");
      } catch (IllegalArgumentException expected) {
        Assert.assertEquals(0, singleDirectionPool.getMemoryReservationSize());
        Assert.assertEquals(513, singleDirectionPool.getReservedBytes());
      }
      singleDirectionPool.free(QUERY_ID, fi, "active", 513);
      Assert.assertEquals(0, singleDirectionPool.getReservedBytes());
    }
  }

  @Test
  public void testConcurrentPlanNodesShareOneSideCounter() throws Exception {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    ExecutorService executor = Executors.newFixedThreadPool(16);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch attempted = new CountDownLatch(16);
    CountDownLatch release = new CountDownLatch(1);
    List<Future<?>> tasks = new ArrayList<>();
    try {
      for (int i = 0; i < 16; i++) {
        String id = "pipeline-" + i;
        pool.registerPlanNodeIdToQueryMemoryMap(
            QUERY_ID, FRAGMENT_INSTANCE_ID, id, MemorySide.INPUT);
        tasks.add(
            executor.submit(
                () -> {
                  start.await();
                  boolean reserved =
                      pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32, 128);
                  attempted.countDown();
                  release.await();
                  if (reserved) pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32);
                  return null;
                }));
      }
      start.countDown();
      Assert.assertTrue(attempted.await(10, TimeUnit.SECONDS));
      long details = 0;
      for (int i = 0; i < 16; i++) {
        details +=
            pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "pipeline-" + i);
      }
      Assert.assertTrue(details > 0 && details <= 128);
      Assert.assertEquals(
          details,
          pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      Assert.assertEquals(details, pool.getReservedBytes());
      release.countDown();
      for (Future<?> task : tasks) task.get(10, TimeUnit.SECONDS);
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
    } finally {
      start.countDown();
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testReleaseBeforePendingReservationIsPublished() throws Exception {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    pool = Mockito.spy(pool);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", MemorySide.INPUT);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 256, false)
            .isReserveSuccess());
    CountDownLatch failedAttempt = new CountDownLatch(1);
    CountDownLatch released = new CountDownLatch(1);
    AtomicBoolean firstAttempt = new AtomicBoolean(true);
    Mockito.doAnswer(
            invocation -> {
              boolean granted = (boolean) invocation.callRealMethod();
              if (!granted && firstAttempt.compareAndSet(true, false)) {
                failedAttempt.countDown();
                Assert.assertTrue(released.await(10, TimeUnit.SECONDS));
              }
              return granted;
            })
        .when(pool)
        .tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 256, 256);
    FutureTask<MemoryReservationResult> reservation =
        new FutureTask<>(
            () ->
                pool.reserveWithPriority(
                    QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 256, 256, false));
    Thread reserver = new Thread(reservation, "reserve-shared-input");
    reserver.start();
    try {
      Assert.assertTrue(failedAttempt.await(10, TimeUnit.SECONDS));
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256);
      released.countDown();
      MemoryReservationResult result = reservation.get(10, TimeUnit.SECONDS);
      Assert.assertTrue(
          "Releasing the last owner must wake the pending request", result.getFuture().isDone());
      result.getFuture().get(10, TimeUnit.SECONDS);
      Assert.assertEquals(256, pool.getReservedBytes());
      Assert.assertEquals(
          256,
          pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      Assert.assertEquals(
          256, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2"));
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 256);
      Assert.assertEquals(0, pool.getReservedBytes());
    } finally {
      released.countDown();
      reserver.join(10000);
      Assert.assertFalse(reserver.isAlive());
    }
  }

  @Test
  public void testCancelledInputDoesNotConsumeReleasedSharedBudget() throws Exception {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "cancelled", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "output", MemorySide.OUTPUT);
    pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 256, false);
    MemoryReservationResult cancelled =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "cancelled", 128, 256, false);
    MemoryReservationResult waiting =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting", 256, 256, false);
    Assert.assertFalse(cancelled.getFuture().isDone());
    Assert.assertFalse(waiting.getFuture().isDone());
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 128, 256, false)
            .isReserveSuccess());
    Assert.assertEquals(128, pool.tryCancel(cancelled.getFuture()));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256);
    waiting.getFuture().get(10, TimeUnit.SECONDS);
    Assert.assertTrue(cancelled.getFuture().isCancelled());
    Assert.assertEquals(
        0, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "cancelled"));
    Assert.assertEquals(
        256, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    Assert.assertEquals(
        128,
        pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.OUTPUT));
    Assert.assertEquals(384, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting", 256);
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 128);
    Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testConcurrentPendingReservationsCompleteAndReleaseAllAccounting() throws Exception {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    int count = 16;
    ExecutorService executor = Executors.newFixedThreadPool(count);
    CountDownLatch attempted = new CountDownLatch(count);
    List<Future<?>> tasks = new ArrayList<>();
    pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 128, 128, false);
    try {
      for (int i = 0; i < count; i++) {
        String id = "waiting-" + i;
        pool.registerPlanNodeIdToQueryMemoryMap(
            QUERY_ID, FRAGMENT_INSTANCE_ID, id, MemorySide.INPUT);
        tasks.add(
            executor.submit(
                () -> {
                  for (int round = 0; round < 50; round++) {
                    MemoryReservationResult result =
                        pool.reserveWithPriority(
                            QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32, 128, false);
                    if (round == 0) attempted.countDown();
                    result.getFuture().get(10, TimeUnit.SECONDS);
                    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, id, result.getReservedBytes());
                  }
                  return null;
                }));
      }
      Assert.assertTrue(attempted.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(count, pool.getMemoryReservationSize());
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 128);
      for (Future<?> task : tasks) task.get(30, TimeUnit.SECONDS);
      for (int i = 0; i < count; i++) {
        Assert.assertEquals(
            0, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting-" + i));
      }
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
      Assert.assertEquals(0, pool.getMemoryReservationSize());
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testDefaultFragmentKeepsIndependentPlanNodeQuotas() {
    MemoryPool legacy = new MemoryPool("legacy", new MemoryManager(1024), 512);
    legacy.registerPlanNodeIdToQueryMemoryMap("q", "f", "a", MemorySide.INPUT);
    legacy.registerPlanNodeIdToQueryMemoryMap("q", "f", "b", MemorySide.INPUT);
    Assert.assertTrue(
        legacy.reserveWithPriority("q", "f", "a", 256, 256, false).isReserveSuccess());
    Assert.assertTrue(
        legacy.reserveWithPriority("q", "f", "b", 256, 256, false).isReserveSuccess());
    Assert.assertEquals(512, legacy.getFragmentMemoryReservedBytes("q", "f", MemorySide.INPUT));
    legacy.free("q", "f", "a", 256);
    legacy.free("q", "f", "b", 256);
    Assert.assertEquals(0, legacy.getReservedBytes());
  }

  @Test
  public void testSharedAndLegacyFragmentsDoNotChangeEachOthersPolicy() {
    pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "legacy", "a", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "legacy", "b", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, "b", MemorySide.INPUT);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 256, false)
            .isReserveSuccess());
    MemoryReservationResult blocked =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "b", 64, 256, false);
    Assert.assertFalse(blocked.getFuture().isDone());
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, "legacy", "a", 256, 256, false).isReserveSuccess());
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, "legacy", "b", 256, 256, false).isReserveSuccess());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256);
    Assert.assertTrue(blocked.getFuture().isDone());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "b", 64);
    pool.free(QUERY_ID, "legacy", "a", 256);
    pool.free(QUERY_ID, "legacy", "b", 256);
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testConcurrentAdmissionAlwaysGrantsOneRequestWhenOneFits() throws Exception {
    assertConcurrentAdmissionAlwaysGrantsOneRequestWhenOneFits(true);
  }

  @Test
  public void testLegacyConcurrentAdmissionAlwaysGrantsOneRequestWhenOneFits() throws Exception {
    assertConcurrentAdmissionAlwaysGrantsOneRequestWhenOneFits(false);
  }

  private void assertConcurrentAdmissionAlwaysGrantsOneRequestWhenOneFits(boolean shared)
      throws Exception {
    if (shared) {
      pool.registerFragmentInstanceSharedBudget(QUERY_ID, FRAGMENT_INSTANCE_ID);
    }
    for (int i = 0; i < 3; i++) {
      String fi = "other" + i;
      pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, fi, PLAN_NODE_ID, MemorySide.OUTPUT);
      Assert.assertTrue(
          pool.reserveWithPriority(QUERY_ID, fi, PLAN_NODE_ID, 256, 256, false).isReserveSuccess());
    }
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, "b", MemorySide.INPUT);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 100; i++) {
        CountDownLatch start = new CountDownLatch(1);
        Future<MemoryReservationResult> first =
            executor.submit(
                () -> {
                  start.await();
                  return pool.reserveWithPriority(
                      QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 256, false);
                });
        Future<MemoryReservationResult> second =
            executor.submit(
                () -> {
                  start.await();
                  return pool.reserveWithPriority(
                      QUERY_ID, FRAGMENT_INSTANCE_ID, "b", 256, 256, false);
                });
        start.countDown();
        MemoryReservationResult a = first.get(10, TimeUnit.SECONDS);
        MemoryReservationResult b = second.get(10, TimeUnit.SECONDS);
        Assert.assertTrue(a.getFuture().isDone() ^ b.getFuture().isDone());
        String granted = a.getFuture().isDone() ? PLAN_NODE_ID : "b";
        String waiting = a.getFuture().isDone() ? "b" : PLAN_NODE_ID;
        pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, granted, 256);
        Assert.assertTrue(a.getFuture().isDone());
        Assert.assertTrue(b.getFuture().isDone());
        pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, waiting, 256);
        Assert.assertEquals(256, pool.getRemainingBytes());
        Assert.assertEquals(
            0,
            pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      }
    } finally {
      executor.shutdownNow();
    }
    for (int i = 0; i < 3; i++) pool.free(QUERY_ID, "other" + i, PLAN_NODE_ID, 256);
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testConcurrentLegacyReservationsKeepIndependentQuotasAndAccurateSideTotals()
      throws Exception {
    for (boolean samePlanNode : new boolean[] {true, false}) {
      MemoryPool legacyPool = new MemoryPool("legacy", new MemoryManager(1024), 512);
      ExecutorService executor = Executors.newFixedThreadPool(16);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch attempted = new CountDownLatch(16);
      CountDownLatch release = new CountDownLatch(1);
      List<Future<?>> tasks = new ArrayList<>();
      try {
        for (int i = 0; i < 16; i++) {
          String id = samePlanNode ? "same" : "pipeline-" + i;
          legacyPool.registerPlanNodeIdToQueryMemoryMap(
              QUERY_ID, FRAGMENT_INSTANCE_ID, id, MemorySide.INPUT);
          tasks.add(
              executor.submit(
                  () -> {
                    start.await();
                    boolean reserved =
                        legacyPool.tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32, 128);
                    attempted.countDown();
                    release.await();
                    if (reserved) {
                      legacyPool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32);
                    }
                    return null;
                  }));
        }
        start.countDown();
        Assert.assertTrue(attempted.await(10, TimeUnit.SECONDS));
        long expected = samePlanNode ? 128 : 512;
        Assert.assertEquals(expected, legacyPool.getReservedBytes());
        Assert.assertEquals(
            expected,
            legacyPool.getFragmentMemoryReservedBytes(
                QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
        if (samePlanNode) {
          Assert.assertEquals(
              128,
              legacyPool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "same"));
        } else {
          for (int i = 0; i < 16; i++) {
            Assert.assertEquals(
                32,
                legacyPool.getPlanNodeMemoryReservedBytes(
                    QUERY_ID, FRAGMENT_INSTANCE_ID, "pipeline-" + i));
          }
        }
        release.countDown();
        for (Future<?> task : tasks) {
          task.get(10, TimeUnit.SECONDS);
        }
        Assert.assertEquals(0, legacyPool.getReservedBytes());
        Assert.assertEquals(0, legacyPool.getQueryMemoryReservedBytes(QUERY_ID));
      } finally {
        start.countDown();
        release.countDown();
        executor.shutdownNow();
      }
    }
  }
}
