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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientRequestTableTest {
  private static final TSStatus SUCCESS = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);

  @Test
  public void committedRequestSurvivesStepdownOfUncommittedRequestsUntilApply() throws Exception {
    ClientRequestTable table = new ClientRequestTable();
    CompletableFuture<TSStatus> committed = new CompletableFuture<>();
    long committedId = table.register(committed);
    table.bind(committedId, 7, 2);
    table.onOutcome(new Action.CompleteClient(committedId, Action.Outcome.COMMITTED, 1));
    CompletableFuture<TSStatus> uncommitted = new CompletableFuture<>();
    long uncommittedId = table.register(uncommitted);
    table.bind(uncommittedId, 8, 2);
    // The kernel fails only uncommitted requests on stepdown; the committed request awaits apply.
    table.onOutcome(new Action.CompleteClient(uncommittedId, Action.Outcome.NOT_LEADER, 2));
    assertTrue(uncommitted.isCompletedExceptionally());
    assertFalse(committed.isDone());
    assertEquals(1, table.boundSize());
    table.onApplied(7, 2, SUCCESS);
    assertEquals(SUCCESS, committed.get(1, TimeUnit.SECONDS));
    table.onApplied(7, 2, RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR));
    assertEquals(SUCCESS, committed.get());
    assertEmpty(table);
  }

  @Test
  public void oldTermApplyCannotCompleteReplacementAtSameIndex() throws Exception {
    ClientRequestTable table = new ClientRequestTable();
    CompletableFuture<TSStatus> old = new CompletableFuture<>();
    long oldId = table.register(old);
    table.bind(oldId, 7, 2);
    table.onOutcome(new Action.CompleteClient(oldId, Action.Outcome.NOT_LEADER, 2));
    CompletableFuture<TSStatus> replacement = new CompletableFuture<>();
    long replacementId = table.register(replacement);
    table.bind(replacementId, 7, 3);
    table.onApplied(7, 2, SUCCESS);
    table.onOutcome(new Action.CompleteClient(oldId, Action.Outcome.COMMITTED, 1));
    table.bind(oldId, 7, 2); // delayed dispatch cannot recreate a terminal request
    assertFalse(replacement.isDone());
    assertEquals(1, table.boundSize());
    table.onApplied(7, 3, SUCCESS);
    assertEquals(SUCCESS, replacement.get(1, TimeUnit.SECONDS));
    assertEmpty(table);
  }

  @Test
  public void everyTerminalOutcomeRemovesTheIdentity() {
    for (Action.Outcome outcome : Action.Outcome.values()) {
      if (outcome == Action.Outcome.COMMITTED) {
        continue;
      }
      ClientRequestTable table = new ClientRequestTable();
      CompletableFuture<TSStatus> future = new CompletableFuture<>();
      long reqId = table.register(future);
      table.bind(reqId, 4, 1);
      table.onOutcome(new Action.CompleteClient(reqId, outcome, 2));
      assertTrue(future.isDone());
      table.bind(reqId, 4, 1);
      table.onApplied(4, 1, SUCCESS);
      assertEmpty(table);
    }
  }

  @Test
  public void cancellationBeforeOrAfterBindingCannotRecreateOrCompleteAWaiter() {
    for (boolean before : new boolean[] {true, false}) {
      ClientRequestTable table = new ClientRequestTable();
      CompletableFuture<TSStatus> canceled = new CompletableFuture<>();
      long id = table.register(canceled);
      if (before) {
        canceled.cancel(false);
      }
      table.bind(id, 8, 3);
      if (!before) {
        canceled.cancel(false);
      }
      table.onOutcome(new Action.CompleteClient(id, Action.Outcome.COMMITTED, 1));
      table.onApplied(8, 3, SUCCESS);
      assertTrue(canceled.isCancelled());
      assertEmpty(table);
    }
  }

  @Test
  public void concurrentBindingAndCancellationLeaveNoAssociation() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      ClientRequestTable table = new ClientRequestTable();
      for (int round = 0; round < 100; round++) {
        CompletableFuture<TSStatus> future = new CompletableFuture<>();
        long id = table.register(future);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> bind =
            pool.submit(
                () -> {
                  start.await();
                  table.bind(id, id, 1);
                  return null;
                });
        Future<?> cancel =
            pool.submit(
                () -> {
                  start.await();
                  future.cancel(false);
                  return null;
                });
        start.countDown();
        bind.get(2, TimeUnit.SECONDS);
        cancel.get(2, TimeUnit.SECONDS);
        assertEmpty(table);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void failAllAndExternalCompletionClearBoundAndUnboundRequests() {
    ClientRequestTable table = new ClientRequestTable();
    CompletableFuture<TSStatus> bound = new CompletableFuture<>();
    table.bind(table.register(bound), 9, 1);
    CompletableFuture<TSStatus> unbound = new CompletableFuture<>();
    table.register(unbound);
    CompletableFuture<TSStatus> external = new CompletableFuture<>();
    table.bind(table.register(external), 10, 1);
    external.completeExceptionally(new IllegalStateException("caller abandoned wait"));
    assertEquals(1, table.boundSize());
    table.failAll(new NotLeaderException(2));
    assertTrue(bound.isCompletedExceptionally());
    assertTrue(unbound.isCompletedExceptionally());
    assertEmpty(table);
  }

  @Test
  public void clientCallbackRunsOutsideTheTableMonitor() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      ClientRequestTable table = new ClientRequestTable();
      CompletableFuture<TSStatus> future = new CompletableFuture<>();
      table.bind(table.register(future), 9, 1);
      CompletableFuture<Integer> callback =
          future.thenApply(
              status -> {
                try {
                  return pool.submit(table::size).get(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                  throw new AssertionError("completion held the table monitor", e);
                }
              });
      table.onApplied(9, 1, SUCCESS);
      assertEquals(Integer.valueOf(0), callback.get(3, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
  }

  private static void assertEmpty(ClientRequestTable table) {
    assertEquals(0, table.size());
    assertEquals(0, table.boundSize());
  }
}
