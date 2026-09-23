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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTsFileConversionTaskManagerTest {

  @Test
  public void testDuplicateStatusRespectsTakeoverMode() {
    final String synchronousTaskId = "sync-" + System.nanoTime();
    PipeTsFileConversionTaskManager.registerIfAbsent(synchronousTaskId, false);
    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        PipeTsFileConversionTaskManager.getDuplicateStatus(synchronousTaskId, false).getCode());
    final TSStatus pausedStatus =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("paused conversion");
    PipeTsFileConversionTaskManager.markPaused(synchronousTaskId, pausedStatus);
    Assert.assertSame(
        pausedStatus, PipeTsFileConversionTaskManager.getDuplicateStatus(synchronousTaskId, false));
    PipeTsFileConversionTaskManager.markSuccess(synchronousTaskId);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        PipeTsFileConversionTaskManager.getDuplicateStatus(synchronousTaskId, false).getCode());

    final String asynchronousTaskId = "async-" + System.nanoTime();
    PipeTsFileConversionTaskManager.registerIfAbsent(asynchronousTaskId, true);
    PipeTsFileConversionTaskManager.markPaused(
        asynchronousTaskId,
        new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()));
    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        PipeTsFileConversionTaskManager.getDuplicateStatus(asynchronousTaskId, true).getCode());
    PipeTsFileConversionTaskManager.markReceiverOwned(asynchronousTaskId);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        PipeTsFileConversionTaskManager.getDuplicateStatus(asynchronousTaskId, true).getCode());
  }

  @Test
  public void testContextIsRetainedUntilTerminalState() {
    final String taskId = "context-" + System.nanoTime();
    final AtomicBoolean closed = new AtomicBoolean(false);
    PipeTsFileConversionTaskManager.registerIfAbsent(taskId, true);
    PipeTsFileConversionTaskManager.enter(taskId);
    try {
      final AutoCloseable context =
          PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
              () -> (AutoCloseable) () -> closed.set(true));
      PipeTsFileConversionTaskManager.markPaused(
          taskId, new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()));
      Assert.assertSame(
          context,
          PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
              () -> (AutoCloseable) () -> closed.set(true)));
      Assert.assertFalse(closed.get());
      PipeTsFileConversionTaskManager.markSuccess(taskId);
      Assert.assertTrue(closed.get());
    } finally {
      PipeTsFileConversionTaskManager.leave();
    }
  }

  @Test
  public void testPrepareForActiveLoadDoesNotDowngradeTerminalTask() {
    final String taskId = "handoff-" + System.nanoTime();
    PipeTsFileConversionTaskManager.registerIfAbsent(taskId, true);
    PipeTsFileConversionTaskManager.markRunning(taskId);
    PipeTsFileConversionTaskManager.prepareForActiveLoad(taskId);
    Assert.assertEquals(
        PipeTsFileConversionTaskManager.State.PENDING,
        PipeTsFileConversionTaskManager.get(taskId).getState());

    PipeTsFileConversionTaskManager.markSuccess(taskId);
    PipeTsFileConversionTaskManager.prepareForActiveLoad(taskId);
    Assert.assertEquals(
        PipeTsFileConversionTaskManager.State.SUCCESS,
        PipeTsFileConversionTaskManager.get(taskId).getState());

    final AtomicBoolean lateContextClosed = new AtomicBoolean(false);
    PipeTsFileConversionTaskManager.enter(taskId);
    try {
      PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
          () -> (AutoCloseable) () -> lateContextClosed.set(true));
      Assert.assertEquals(0, PipeTsFileConversionTaskManager.getRetainedContextCount());
    } finally {
      PipeTsFileConversionTaskManager.leave();
    }
    Assert.assertTrue(lateContextClosed.get());
  }

  @Test
  public void testRegisterAndGetDuplicateStatusClaimsTaskAtomically() throws Exception {
    final String taskId = "atomic-register-" + System.nanoTime();
    final int concurrency = 16;
    final ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    final CountDownLatch ready = new CountDownLatch(concurrency);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<TSStatus>> results = new ArrayList<>();
    try {
      for (int i = 0; i < concurrency; i++) {
        results.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  return PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(
                      taskId, false);
                }));
      }

      Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();
      int claimantCount = 0;
      for (final Future<TSStatus> result : results) {
        final TSStatus status = result.get(10, TimeUnit.SECONDS);
        if (status == null) {
          claimantCount++;
        } else {
          Assert.assertEquals(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
              status.getCode());
        }
      }
      Assert.assertEquals(1, claimantCount);
    } finally {
      start.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      PipeTsFileConversionTaskManager.markSuccess(taskId);
    }
  }

  @Test
  public void testFailedHandoffCanBeReclaimedOnlyOnce() {
    final String taskId = "retryable-handoff-" + System.nanoTime();
    final TSStatus pausedStatus =
        new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("active-load handoff failed");

    Assert.assertNull(PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, true));
    PipeTsFileConversionTaskManager.markRunning(taskId);
    PipeTsFileConversionTaskManager.markRetryable(taskId, pausedStatus);

    Assert.assertNull(PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, true));
    Assert.assertEquals(
        PipeTsFileConversionTaskManager.State.PENDING,
        PipeTsFileConversionTaskManager.get(taskId).getState());
    Assert.assertNull(PipeTsFileConversionTaskManager.get(taskId).getStatus());
    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, true).getCode());

    PipeTsFileConversionTaskManager.markRetryable(taskId, pausedStatus);
    PipeTsFileConversionTaskManager.markReceiverOwned(taskId);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, true).getCode());
    PipeTsFileConversionTaskManager.markSuccess(taskId);
  }

  @Test
  public void testReceiverOwnedStatusRespectsTakeoverMode() {
    final String asynchronousTaskId = "receiver-owned-async-" + System.nanoTime();
    final TSStatus failedStatus =
        new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
            .setMessage("active load failed after handoff");
    Assert.assertNull(
        PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(asynchronousTaskId, true));
    PipeTsFileConversionTaskManager.markReceiverOwned(asynchronousTaskId);
    PipeTsFileConversionTaskManager.markFailed(asynchronousTaskId, failedStatus);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(asynchronousTaskId, true)
            .getCode());

    final String synchronousTaskId = "receiver-owned-sync-" + System.nanoTime();
    final TSStatus pausedStatus =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("conversion paused after handoff");
    Assert.assertNull(
        PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(synchronousTaskId, false));
    PipeTsFileConversionTaskManager.markReceiverOwned(synchronousTaskId);
    PipeTsFileConversionTaskManager.markPaused(synchronousTaskId, pausedStatus);
    Assert.assertSame(
        pausedStatus,
        PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(synchronousTaskId, false));
    PipeTsFileConversionTaskManager.markSuccess(synchronousTaskId);
  }

  @Test
  public void testTerminalStateAndStatusTransitionAtomically() throws Exception {
    final int taskCount = 256;
    final String[] taskIds = new String[taskCount];
    final TSStatus[] failedStatuses = new TSStatus[taskCount];
    for (int i = 0; i < taskCount; i++) {
      taskIds[i] = "terminal-race-" + System.nanoTime() + '-' + i;
      failedStatuses[i] =
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(taskIds[i]);
      PipeTsFileConversionTaskManager.registerIfAbsent(taskIds[i], false);
    }

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CyclicBarrier barrier = new CyclicBarrier(3);
    try {
      final Future<Void> successFuture =
          executor.submit(
              () -> {
                for (final String taskId : taskIds) {
                  barrier.await();
                  PipeTsFileConversionTaskManager.markSuccess(taskId);
                  barrier.await();
                }
                return null;
              });
      final Future<Void> failureFuture =
          executor.submit(
              () -> {
                for (int i = 0; i < taskCount; i++) {
                  barrier.await();
                  PipeTsFileConversionTaskManager.markFailed(taskIds[i], failedStatuses[i]);
                  barrier.await();
                }
                return null;
              });

      for (int i = 0; i < taskCount; i++) {
        barrier.await(10, TimeUnit.SECONDS);
        barrier.await(10, TimeUnit.SECONDS);
        final PipeTsFileConversionTaskManager.Task task =
            PipeTsFileConversionTaskManager.get(taskIds[i]);
        if (task.getState() == PipeTsFileConversionTaskManager.State.SUCCESS) {
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(), task.getStatus().getCode());
        } else {
          Assert.assertEquals(PipeTsFileConversionTaskManager.State.FAILED, task.getState());
          Assert.assertSame(failedStatuses[i], task.getStatus());
        }
      }
      successFuture.get(10, TimeUnit.SECONDS);
      failureFuture.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testRetainedContextCountHasHardLimit() {
    Assert.assertEquals(0, PipeTsFileConversionTaskManager.getRetainedContextCount());
    final int maxContextCount = PipeTsFileConversionTaskManager.getMaxRetainedContextCount();
    final List<String> retainedTaskIds = new ArrayList<>();
    final List<AtomicBoolean> retainedContextClosed = new ArrayList<>();
    final String overflowTaskId = "context-overflow-" + System.nanoTime();

    try {
      for (int i = 0; i < maxContextCount; i++) {
        final String taskId = "context-retained-" + System.nanoTime() + '-' + i;
        final AtomicBoolean closed = new AtomicBoolean(false);
        retainedTaskIds.add(taskId);
        retainedContextClosed.add(closed);
        PipeTsFileConversionTaskManager.registerIfAbsent(taskId, true);
        PipeTsFileConversionTaskManager.markRunning(taskId);
        PipeTsFileConversionTaskManager.enter(taskId);
        PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
            () -> (AutoCloseable) () -> closed.set(true));
        PipeTsFileConversionTaskManager.leave();
        Assert.assertFalse(closed.get());
      }
      Assert.assertEquals(
          maxContextCount, PipeTsFileConversionTaskManager.getRetainedContextCount());

      final AtomicBoolean overflowContextClosed = new AtomicBoolean(false);
      PipeTsFileConversionTaskManager.registerIfAbsent(overflowTaskId, true);
      PipeTsFileConversionTaskManager.markRunning(overflowTaskId);
      PipeTsFileConversionTaskManager.enter(overflowTaskId);
      final AutoCloseable overflowContext =
          PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
              () -> (AutoCloseable) () -> overflowContextClosed.set(true));
      Assert.assertSame(
          overflowContext,
          PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
              () -> (AutoCloseable) () -> Assert.fail("must reuse the unretained context")));
      Assert.assertEquals(
          maxContextCount, PipeTsFileConversionTaskManager.getRetainedContextCount());
      Assert.assertFalse(overflowContextClosed.get());

      PipeTsFileConversionTaskManager.leave();
      Assert.assertTrue(overflowContextClosed.get());
      Assert.assertEquals(
          maxContextCount, PipeTsFileConversionTaskManager.getRetainedContextCount());
    } finally {
      PipeTsFileConversionTaskManager.leave();
      PipeTsFileConversionTaskManager.markSuccess(overflowTaskId);
      retainedTaskIds.forEach(PipeTsFileConversionTaskManager::markSuccess);
    }

    for (final AtomicBoolean closed : retainedContextClosed) {
      Assert.assertTrue(closed.get());
    }
    Assert.assertEquals(0, PipeTsFileConversionTaskManager.getRetainedContextCount());
  }

  @Test
  public void testPausedTaskRetryReusesCheckpointWithoutCreatingTask() {
    final String taskId = "paused-retry-" + System.nanoTime();
    final AtomicBoolean closed = new AtomicBoolean(false);
    Assert.assertNull(PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, false));
    PipeTsFileConversionTaskManager.enter(taskId);
    final Object context;
    try {
      context =
          PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
              () -> (AutoCloseable) () -> closed.set(true));
      PipeTsFileConversionTaskManager.markPaused(
          taskId,
          new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()));
    } finally {
      PipeTsFileConversionTaskManager.leave();
    }

    Assert.assertNull(PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, false));
    PipeTsFileConversionTaskManager.enter(taskId);
    try {
      Assert.assertSame(
          context,
          PipeTsFileConversionTaskManager.getOrCreateCurrentContext(
              () ->
                  (AutoCloseable)
                      () -> Assert.fail("a new conversion context must not be created")));
      PipeTsFileConversionTaskManager.markSuccess(taskId);
    } finally {
      PipeTsFileConversionTaskManager.leave();
    }
    Assert.assertTrue(closed.get());
  }

  @Test
  public void testLegacyTaskCanReportTypeMismatchWithoutTaskId() {
    PipeTsFileConversionTaskManager.enter(null);
    try {
      Assert.assertFalse(PipeTsFileConversionTaskManager.isTypeMismatchDetected(null));
      PipeTsFileConversionTaskManager.markTypeMismatchDetected();
      Assert.assertTrue(PipeTsFileConversionTaskManager.isTypeMismatchDetected(null));
    } finally {
      PipeTsFileConversionTaskManager.leave();
    }
    Assert.assertFalse(PipeTsFileConversionTaskManager.isTypeMismatchDetected(null));
  }
}
