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
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LsmClientRequestTableCompatibilityTest {

  private static final TSStatus SUCCESS = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);

  private static LogEntry entry(long index, long term, long reqId) {
    return new LogEntry(
        index, term, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.wrap(new byte[] {1}), reqId, null);
  }

  @Test
  public void commitmentWaitsForApplicationOfTheExactTermAndIndex() throws Exception {
    ClientRequestTable table = new ClientRequestTable();
    CompletableFuture<TSStatus> future = new CompletableFuture<>();
    long reqId = table.register(future);
    table.bind(entry(7, 3, reqId));
    table.onOutcome(new Action.CompleteClient(reqId, Action.Outcome.COMMITTED, 1));
    assertFalse(future.isDone());
    table.onApplied(7, 2, SUCCESS);
    assertFalse(future.isDone());
    assertTrue(table.isLocalEntry(7, 3));
    table.onApplied(7, 3, SUCCESS);
    assertEquals(SUCCESS, future.get());
    assertEquals(0, table.size());
    assertFalse(table.isLocalEntry(7, 3));
  }

  @Test
  public void rejectionsAndShutdownRemoveBothDirectionsAndIgnoreLateBinds() {
    for (Action.Outcome outcome :
        new Action.Outcome[] {
          Action.Outcome.NOT_LEADER, Action.Outcome.TIMEOUT,
          Action.Outcome.READ_ONLY, Action.Outcome.REJECTED
        }) {
      ClientRequestTable table = new ClientRequestTable();
      CompletableFuture<TSStatus> future = new CompletableFuture<>();
      long reqId = table.register(future);
      LogEntry original = entry(7, 3, reqId);
      table.bind(original);
      table.onOutcome(new Action.CompleteClient(reqId, outcome, 2));
      table.bind(original);
      assertTrue(future.isDone());
      assertEquals(0, table.size());
      assertFalse(table.isLocalEntry(7, 3));
    }
    ClientRequestTable table = new ClientRequestTable();
    CompletableFuture<TSStatus> future = new CompletableFuture<>();
    long reqId = table.register(future);
    LogEntry original = entry(7, 3, reqId);
    table.bind(original);
    table.failAll(new NotLeaderException(-1));
    table.bind(original);
    assertTrue(future.isCompletedExceptionally());
    assertEquals(0, table.size());
    assertFalse(table.isLocalEntry(7, 3));
  }

  @Test
  public void reusedSlotAndReboundRequestCannotAcknowledgeTheOldIdentity() throws Exception {
    ClientRequestTable table = new ClientRequestTable();
    CompletableFuture<TSStatus> first = new CompletableFuture<>();
    long firstId = table.register(first);
    table.bind(entry(7, 3, firstId));
    table.onOutcome(new Action.CompleteClient(firstId, Action.Outcome.NOT_LEADER, 2));

    CompletableFuture<TSStatus> next = new CompletableFuture<>();
    long nextId = table.register(next);
    table.bind(entry(7, 4, nextId));
    table.onApplied(7, 3, SUCCESS);
    assertFalse(next.isDone());
    try {
      table.bind(entry(8, 4, nextId));
      org.junit.Assert.fail("Conflicting rebind must be rejected");
    } catch (IllegalStateException expected) {
      assertTrue(table.isLocalEntry(7, 4));
    }
    table.onApplied(7, 4, SUCCESS);
    assertEquals(SUCCESS, next.get());
    assertFalse(table.isLocalEntry(8, 4));
    assertEquals(0, table.size());
  }
}
