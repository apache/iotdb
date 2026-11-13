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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryAttemptId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TQueryAdmissionToken;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryAttemptRegistryTest {
  private final AtomicLong clock = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(1000));
  private final QueryAttemptRegistry registry = new QueryAttemptRegistry(clock::get);
  private final TQueryAdmissionToken token = registry.issueAdmissionToken(1000);
  private final QueryId queryId = new QueryId("attempt_test");

  @Test
  public void keepsSharedStateBetweenSequentiallyDispatchedFragments() {
    QueryAttemptRegistry.Registration first = register(0, 0, 2);
    DataNodeQueryContext context = first.getContext();
    first.close();
    assertTrue(registry.hasSharedContext(key(0)));
    QueryAttemptRegistry.Registration second = register(0, 1, 2);
    assertSame(context, second.getContext());
    second.close();
    assertFalse(registry.hasSharedContext(key(0)));
    assertEquals(1, registry.getRetainedEntryCount());
  }

  @Test
  public void closesPartiallyDispatchedAttemptAfterActualUsersRelease() {
    FragmentInstance instance = instance(0, 0, 3);
    FragmentInstanceStateMachine state = state(instance);
    QueryAttemptRegistry.Registration first = registry.register(instance, state, true);
    QueryAttemptRegistry.Registration failed = register(0, 1, 3);
    failed.close();
    registry.close(key(0), token, true);
    assertEquals(FragmentInstanceState.CANCELLED, state.getState());
    assertTrue(registry.hasSharedContext(key(0)));
    first.close();
    first.close();
    registry.close(key(0), token, true);
    assertFalse(registry.hasSharedContext(key(0)));
    // The third FI never arrived; it cannot keep the shared LAST/filter state alive.
    assertEquals(1, registry.getRetainedEntryCount());
  }

  @Test
  public void lateOldCancellationAndReleaseDoNotAffectRetry() {
    QueryAttemptRegistry.Registration old = register(0, 0, 3);
    QueryAttemptRegistry.Registration retry = register(1, 3, 1);
    assertNotSame(old.getContext(), retry.getContext());
    registry.close(key(0), token, true);
    old.close();
    registry.close(key(0), token, true);
    assertTrue(registry.hasSharedContext(key(1)));
    assertFalse(registry.hasSharedContext(key(0)));
    retry.close();
  }

  @Test
  public void closedBeforeFirstDispatchRemainsFencedAfterRecordExpires() {
    registry.close(key(0), token, true);
    assertEquals(
        TSStatusCode.QUERY_WAS_KILLED.getStatusCode(),
        assertThrows(IoTDBRuntimeException.class, () -> register(0, 0, 1)).getErrorCode());
    clock.set(TimeUnit.MILLISECONDS.toNanos(2000));
    registry.removeExpired();
    assertEquals(0, registry.getRetainedEntryCount());
    assertEquals(
        TSStatusCode.DISPATCH_ERROR.getStatusCode(),
        assertThrows(IoTDBRuntimeException.class, () -> register(0, 0, 1)).getErrorCode());
    assertEquals(0, registry.getRetainedEntryCount());
  }

  @Test
  public void deadlineReclaimsPartialAttemptWhenCoordinatorDoesNotCancel() {
    FragmentInstance instance = instance(0, 0, 3);
    FragmentInstanceStateMachine state = state(instance);
    QueryAttemptRegistry.Registration first = registry.register(instance, state, true);
    clock.set(TimeUnit.MILLISECONDS.toNanos(2000));
    registry.removeExpired();
    assertEquals(FragmentInstanceState.FAILED, state.getState());
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContext(
            instance.getId(), state, null, Long.MAX_VALUE, false, false);
    assertEquals(
        TSStatusCode.QUERY_TIMEOUT.getStatusCode(),
        context.getInstanceInfo().getErrorCode().get().getCode());
    first.close();
    assertEquals(0, registry.getRetainedEntryCount());
  }

  @Test
  public void repeatedDispatchDoesNotReplaceExistingRegistration() {
    FragmentInstance instance = instance(0, 0, 2);
    QueryAttemptRegistry.Registration first = registry.register(instance, state(instance), true);
    assertEquals(
        TSStatusCode.REPEATED_RPC_CALL.getStatusCode(),
        assertThrows(
                IoTDBRuntimeException.class,
                () -> registry.register(instance, state(instance), true))
            .getErrorCode());
    first.close();
    assertTrue(registry.hasSharedContext(key(0)));
    registry.close(key(0), token, true);
    assertFalse(registry.hasSharedContext(key(0)));
  }

  @Test
  public void legacyCompletionDoesNotFenceRetryUsingTheSameQueryId() {
    FragmentInstance instance = instance(0, 0, 1);
    when(instance.hasQueryAttempt()).thenReturn(false);
    QueryAttemptRegistry.Registration first = registry.register(instance, state(instance), true);
    first.close();
    assertEquals(0, registry.getRetainedEntryCount());
    QueryAttemptRegistry.Registration second = registry.register(instance, state(instance), true);
    assertNotSame(first.getContext(), second.getContext());
    second.close();
  }

  @Test
  public void concurrentCloseCannotMissRegistrationInProgress() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch registering = new CountDownLatch(1);
    CountDownLatch allowRegistration = new CountDownLatch(1);
    FragmentInstance instance = instance(0, 0, 3);
    FragmentInstanceStateMachine state = state(instance);
    when(instance.getDataNodeFINum())
        .thenAnswer(
            ignored -> {
              registering.countDown();
              assertTrue(allowRegistration.await(10, TimeUnit.SECONDS));
              return 3;
            });
    try {
      Future<QueryAttemptRegistry.Registration> registration =
          executor.submit(() -> registry.register(instance, state, true));
      assertTrue(registering.await(10, TimeUnit.SECONDS));
      Future<?> cancellation = executor.submit(() -> registry.close(key(0), token, true));
      allowRegistration.countDown();
      QueryAttemptRegistry.Registration result = registration.get(10, TimeUnit.SECONDS);
      cancellation.get(10, TimeUnit.SECONDS);
      assertEquals(FragmentInstanceState.CANCELLED, state.getState());
      result.close();
      assertFalse(registry.hasSharedContext(key(0)));
      assertThrows(IoTDBRuntimeException.class, () -> register(0, 1, 3));
    } finally {
      allowRegistration.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void admissionExpiryDoesNotCancelAnAlreadyRunningLongQuery() {
    FragmentInstance instance = instance(0, 0, 3);
    when(instance.getTimeOut()).thenReturn(Long.MAX_VALUE);
    TQueryAdmissionToken shortToken = registry.issueAdmissionToken(200);
    when(instance.getAdmissionToken()).thenReturn(shortToken);
    FragmentInstanceStateMachine state = state(instance);
    QueryAttemptRegistry.Registration running = registry.register(instance, state, true);
    clock.set(TimeUnit.MILLISECONDS.toNanos(1300));
    registry.removeExpired();
    assertEquals(FragmentInstanceState.RUNNING, state.getState());
    assertTrue(registry.hasSharedContext(key(0)));
    registry.close(key(0), shortToken, true);
    running.close();
    assertEquals(0, registry.getRetainedEntryCount());
    assertThrows(
        IoTDBRuntimeException.class, () -> registry.register(instance, state(instance), true));
  }

  @Test
  public void completedLongQueryRetainsOnlyABoundedAdmissionFence() {
    FragmentInstance instance = instance(0, 0, 1);
    when(instance.getTimeOut()).thenReturn(Long.MAX_VALUE);
    TQueryAdmissionToken shortToken = registry.issueAdmissionToken(200);
    when(instance.getAdmissionToken()).thenReturn(shortToken);
    registry.register(instance, state(instance), true).close();
    assertEquals(1, registry.getRetainedEntryCount());
    assertFalse(registry.hasSharedContext(key(0)));
    clock.set(TimeUnit.MILLISECONDS.toNanos(1200));
    registry.removeExpired();
    assertEquals(0, registry.getRetainedEntryCount());
  }

  @Test
  public void expiryOfClosedFenceDoesNotAddATimeoutCause() {
    FragmentInstance instance = instance(0, 0, 3);
    FragmentInstanceStateMachine state = state(instance);
    QueryAttemptRegistry.Registration registration = registry.register(instance, state, true);
    registry.close(key(0), token, true);
    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    registry.removeExpired();
    assertEquals(FragmentInstanceState.CANCELLED, state.getState());
    assertTrue(state.getFailureCauses().isEmpty());
    registration.close();
    assertEquals(0, registry.getRetainedEntryCount());
  }

  @Test
  public void receiverClockOriginDoesNotShortenExecutionBudget() {
    // The receiver can have an entirely different clock origin from the coordinator. Only the
    // receiver's issued token and local relative execution timeout enter admission/expiry.
    AtomicLong receiverTick = new AtomicLong(TimeUnit.DAYS.toNanos(123));
    QueryAttemptRegistry receiver = new QueryAttemptRegistry(receiverTick::get);
    FragmentInstance instance = instance(0, 0, 1);
    when(instance.getAdmissionToken()).thenReturn(receiver.issueAdmissionToken(20));
    FragmentInstanceStateMachine state = state(instance);
    QueryAttemptRegistry.Registration registration = receiver.register(instance, state, true);
    receiverTick.addAndGet(TimeUnit.MILLISECONDS.toNanos(999));
    receiver.removeExpired();
    assertEquals(FragmentInstanceState.RUNNING, state.getState());
    receiverTick.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
    receiver.removeExpired();
    assertEquals(FragmentInstanceState.FAILED, state.getState());
    assertTrue(state.getFailureCauses().peek() instanceof QueryTimeoutException);
    registration.close();
  }

  @Test
  public void oldProcessTokenCannotAdmitOrCloseANewProcessAttempt() {
    QueryAttemptRegistry restarted = new QueryAttemptRegistry(clock::get);
    FragmentInstance instance = instance(0, 0, 1);
    assertEquals(
        TSStatusCode.DISPATCH_ERROR.getStatusCode(),
        assertThrows(
                IoTDBRuntimeException.class,
                () -> restarted.register(instance, state(instance), true))
            .getErrorCode());
    when(instance.getAdmissionToken()).thenReturn(restarted.issueAdmissionToken(1000));
    QueryAttemptRegistry.Registration registration =
        restarted.register(instance, state(instance), true);
    restarted.close(key(0), token, true);
    assertTrue(restarted.hasSharedContext(key(0)));
    registration.close();
  }

  @Test
  public void freshAttemptWorksAfterOldFenceExpiresAndMonotonicCounterWraps() {
    clock.set(Long.MAX_VALUE - TimeUnit.MILLISECONDS.toNanos(5));
    TQueryAdmissionToken issued = registry.issueAdmissionToken(10);
    FragmentInstance instance = instance(0, 0, 1);
    when(instance.getAdmissionToken()).thenReturn(issued);
    registry.register(instance, state(instance), true).close();
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    registry.removeExpired();
    assertEquals(0, registry.getRetainedEntryCount());
    assertThrows(
        IoTDBRuntimeException.class, () -> registry.register(instance, state(instance), true));
    FragmentInstance fresh = instance(1, 1, 1);
    when(fresh.getAdmissionToken()).thenReturn(registry.issueAdmissionToken(1000));
    registry.register(fresh, state(fresh), true).close();
  }

  @Test
  public void completedAttemptsAreNotScannedBeforeTheirExpiry() {
    java.util.concurrent.atomic.AtomicInteger reads =
        new java.util.concurrent.atomic.AtomicInteger();
    QueryAttemptRegistry many =
        new QueryAttemptRegistry(
            () -> {
              reads.incrementAndGet();
              return clock.get();
            });
    TQueryAdmissionToken issued = many.issueAdmissionToken(1000);
    for (int i = 0; i < 10000; i++) {
      many.close(new QueryAttemptId(new QueryId("closed_" + i), 0), issued, true);
    }
    assertEquals(0, many.getActiveEntryCount());
    assertEquals(10000, many.getRetainedEntryCount());
    reads.set(0);
    many.removeExpired();
    // Only the queue head needs a clock check, independently of the retained marker count.
    assertTrue(reads.get() <= 2);
    assertEquals(10000, many.getRetainedEntryCount());
    clock.set(issued.getDeadlineNanos());
    many.removeExpired();
    assertEquals(0, many.getRetainedEntryCount());
    assertEquals(0, many.getPendingExpirationCount());
  }

  @Test
  public void earlierExpiryCannotDeleteAnExtendedFence() {
    TQueryAdmissionToken shortToken = registry.issueAdmissionToken(100);
    registry.close(key(0), shortToken, true);
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(50));
    TQueryAdmissionToken extended = registry.issueAdmissionToken(1000);
    registry.close(key(0), extended, true);
    clock.set(shortToken.getDeadlineNanos());
    registry.removeExpired();
    assertEquals(1, registry.getRetainedEntryCount());
    FragmentInstance late = instance(0, 0, 1);
    when(late.getAdmissionToken()).thenReturn(extended);
    assertEquals(
        TSStatusCode.QUERY_WAS_KILLED.getStatusCode(),
        assertThrows(IoTDBRuntimeException.class, () -> registry.register(late, state(late), true))
            .getErrorCode());
    clock.set(extended.getDeadlineNanos());
    registry.removeExpired();
    assertEquals(0, registry.getRetainedEntryCount());
  }

  @Test
  public void repeatedCloseDoesNotAccumulateExpiryEvents() {
    registry.close(key(0), token, true);
    for (int i = 0; i < 1000; i++) {
      registry.close(key(0), token, true);
    }
    assertEquals(1, registry.getPendingExpirationCount());
    assertEquals(1, registry.getRetainedEntryCount());
    assertEquals(0, registry.getActiveEntryCount());
  }

  private QueryAttemptId key(int attempt) {
    return new QueryAttemptId(queryId, attempt);
  }

  private QueryAttemptRegistry.Registration register(int attempt, int fragment, int planned) {
    FragmentInstance instance = instance(attempt, fragment, planned);
    return registry.register(instance, state(instance), true);
  }

  private FragmentInstanceStateMachine state(FragmentInstance instance) {
    return new FragmentInstanceStateMachine(instance.getId(), Runnable::run);
  }

  private FragmentInstance instance(int attempt, int fragment, int planned) {
    FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getId())
        .thenReturn(new FragmentInstanceId(new PlanFragmentId(queryId, fragment), "0"));
    when(instance.hasQueryAttempt()).thenReturn(true);
    when(instance.getQueryAttemptId()).thenReturn(attempt);
    when(instance.getAdmissionToken()).thenReturn(token);
    when(instance.getTimeOut()).thenReturn(1000L);
    when(instance.getDataNodeFINum()).thenReturn(planned);
    return instance;
  }
}
