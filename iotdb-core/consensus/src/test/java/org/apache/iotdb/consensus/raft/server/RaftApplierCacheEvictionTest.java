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
import org.apache.iotdb.consensus.raft.core.PersistToken;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.storage.LogWriter;
import org.apache.iotdb.consensus.raft.storage.SegmentedLogStore;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RaftApplierCacheEvictionTest {
  @Test
  public void smallCacheEvictsPendingPayloadsWithoutLosingRepliesOrCanceledApplication()
      throws Exception {
    File dir = Files.createTempDirectory("raft-pending-eviction").toFile();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (SegmentedLogStore store =
        new SegmentedLogStore(new File(dir, "log"), EntryMetaCodec.EMPTY, 1 << 20, 128)) {
      store.recover();
      ClientRequestTable requests = new ClientRequestTable();
      CountDownLatch durable = new CountDownLatch(3);
      try (LogWriter writer =
          new LogWriter("eviction-writer", store, null, e -> durable.countDown(), 1, 0)) {
        ActionDispatcher dispatcher =
            new ActionDispatcher(
                "eviction", writer, store, null, null, null, requests, null, null, e -> {}, null);
        List<CompletableFuture<TSStatus>> futures = new ArrayList<>();
        List<Long> requestIds = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
          CompletableFuture<TSStatus> future = new CompletableFuture<>();
          long requestId = requests.register(future);
          futures.add(future);
          requestIds.add(requestId);
          LogEntry entry = entry(index, 1, requestId, "v" + index);
          dispatcher.execute(
              Collections.singletonList(
                  new Action.Persist(
                      Collections.singletonList(entry), -1, new PersistToken(index))));
        }
        assertEquals(3, requests.boundSize());
        // Delay all application until every write is durable. The 128-byte LRU holds only one
        // 66-byte entry, so the first still-waiting request is necessarily read back from disk.
        writer.start();
        assertTrue(durable.await(5, TimeUnit.SECONDS));
        assertEquals(LogEntry.NO_REQUEST, store.read(0).reqId);
        assertTrue(store.loadCount() > 0);
        for (long requestId : requestIds) {
          requests.onOutcome(new Action.CompleteClient(requestId, Action.Outcome.COMMITTED, 1));
        }
        for (CompletableFuture<TSStatus> future : futures) {
          assertFalse(future.isDone());
        }
        // Timeout/cancellation abandons the wait, not the already durable state-machine command.
        futures.get(2).cancel(false);
        RecordingStateMachine machine = new RecordingStateMachine();
        GroupExecutor executor = new GroupExecutor("eviction-apply", pool);
        CountDownLatch applied = new CountDownLatch(3);
        RaftApplier applier =
            new RaftApplier(
                "eviction", store, machine, requests, executor, e -> applied.countDown());
        applier.enqueue(new Action.Apply(0, 2));
        assertTrue(applied.await(5, TimeUnit.SECONDS));
        for (int index = 0; index < 2; index++) {
          assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              futures.get(index).get(1, TimeUnit.SECONDS).getCode());
        }
        assertTrue(futures.get(2).isCancelled());
        assertEquals(Arrays.asList("v0", "v1", "v2"), machine.snapshotApplied());
        assertNull(applier.failure());
        assertEquals(0, requests.size());
        assertEquals(0, requests.boundSize());
      }
    } finally {
      pool.shutdownNow();
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
    }
  }

  @Test
  public void truncatedEntryAndLateOldTermCallbackCannotAnswerReplacement() throws Exception {
    File dir = Files.createTempDirectory("raft-replaced-waiter").toFile();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (SegmentedLogStore store =
        new SegmentedLogStore(new File(dir, "log"), EntryMetaCodec.EMPTY, 1 << 20, 0)) {
      store.recover();
      ClientRequestTable requests = new ClientRequestTable();
      CompletableFuture<TSStatus> old = new CompletableFuture<>();
      long oldId = requests.register(old);
      requests.bind(oldId, 0, 1);
      store.write(entry(0, 1, oldId, "old"));
      store.sync();
      requests.onOutcome(new Action.CompleteClient(oldId, Action.Outcome.NOT_LEADER, 2));
      store.deleteFrom(0);
      CompletableFuture<TSStatus> replacement = new CompletableFuture<>();
      long newId = requests.register(replacement);
      requests.bind(newId, 0, 2);
      store.write(entry(0, 2, newId, "replacement"));
      store.sync();
      requests.onApplied(0, 1, RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      assertFalse(replacement.isDone());
      assertTrue(old.isCompletedExceptionally());
      assertEquals(LogEntry.NO_REQUEST, store.read(0).reqId);
      assertEquals(2, store.read(0).term);
      RecordingStateMachine machine = new RecordingStateMachine();
      GroupExecutor executor = new GroupExecutor("replacement-apply", pool);
      CountDownLatch applied = new CountDownLatch(1);
      RaftApplier applier =
          new RaftApplier(
              "replacement", store, machine, requests, executor, e -> applied.countDown());
      applier.enqueue(new Action.Apply(0, 0));
      assertTrue(applied.await(5, TimeUnit.SECONDS));
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          replacement.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(Collections.singletonList("replacement"), machine.snapshotApplied());
      assertEquals(0, requests.boundSize());
    } finally {
      pool.shutdownNow();
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
    }
  }

  private static LogEntry entry(long index, long term, long requestId, String value) {
    return new LogEntry(
        index,
        term,
        EntryType.DATA,
        EntryMeta.EMPTY,
        ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)),
        requestId,
        null);
  }
}
