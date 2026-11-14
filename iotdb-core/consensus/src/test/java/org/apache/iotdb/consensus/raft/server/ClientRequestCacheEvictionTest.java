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
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.storage.LogWriter;
import org.apache.iotdb.consensus.raft.storage.SegmentedLogStore;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** A disk cache miss must not turn an applied leader write into an unanswered client request. */
public class ClientRequestCacheEvictionTest {

  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void applicationCompletesTheClientAfterEvictionBeforeOrAfterCommit() throws Exception {
    for (boolean evictBeforeCommit : new boolean[] {true, false}) {
      ExecutorService pool = Executors.newSingleThreadExecutor();
      GroupExecutor executor = new GroupExecutor("cache-eviction-apply", pool);
      BlockingQueue<Event> durable = new LinkedBlockingQueue<>();
      ClientRequestTable requests = new ClientRequestTable();
      RecordingStateMachine stateMachine = new RecordingStateMachine();
      CompletableFuture<TSStatus> future = new CompletableFuture<>();
      AtomicInteger completions = new AtomicInteger();
      future.whenComplete((status, error) -> completions.incrementAndGet());
      long reqId = requests.register(future);
      // One entry fits; appending another deterministically evicts the live leader request.
      try (SegmentedLogStore store =
              new SegmentedLogStore(temporary.newFolder(), EntryMetaCodec.EMPTY, 1024, 80);
          LogWriter writer =
              new LogWriter("cache-eviction-writer", store, null, durable::add, 1, 0)) {
        store.recover();
        RaftApplier applier =
            new RaftApplier("cache-eviction", store, stateMachine, requests, executor, ev -> {});
        ActionDispatcher dispatcher =
            new ActionDispatcher(
                "cache-eviction",
                writer,
                store,
                null,
                null,
                applier,
                requests,
                null,
                null,
                ev -> {},
                null);
        LogEntry local =
            new LogEntry(
                0,
                3,
                EntryType.DATA,
                EntryMeta.EMPTY,
                payload("local"),
                reqId,
                new RecordingStateMachine.StringRequest("local"));
        LogEntry filler = new LogEntry(1, 3, EntryType.DATA, EntryMeta.EMPTY, payload("evict"));
        writer.start();
        persist(dispatcher, durable, local, 1);
        if (!evictBeforeCommit) {
          commit(dispatcher, future, reqId);
        }
        persist(dispatcher, durable, filler, 2);
        LogEntry reread = store.read(0);
        assertEquals(LogEntry.NO_REQUEST, reread.reqId);
        assertNull(reread.liveRequest);
        assertTrue(store.loadCount() > 0);
        assertTrue(requests.isLocalEntry(0, 3));
        assertFalse(future.isDone());
        if (evictBeforeCommit) {
          commit(dispatcher, future, reqId);
        }
        dispatcher.execute(Collections.singletonList(new Action.Apply(0, 0)));
        assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            future.get(10, TimeUnit.SECONDS).getCode());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!executor.isIdle() && System.nanoTime() < deadline) {
          Thread.sleep(2);
        }
        assertTrue(executor.isIdle());
        assertEquals(Collections.singletonList("local"), stateMachine.applied);
        assertEquals(0, stateMachine.remoteApplied.get());
        assertEquals(1, completions.get());
        assertEquals(0, requests.size());
        assertFalse(requests.isLocalEntry(0, 3));
        assertNull(applier.failure());
      } finally {
        executor.close();
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  private static ByteBuffer payload(String value) {
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }

  private static void persist(
      ActionDispatcher dispatcher, BlockingQueue<Event> durable, LogEntry entry, long token)
      throws InterruptedException {
    dispatcher.execute(
        Collections.singletonList(
            new Action.Persist(Collections.singletonList(entry), -1, new PersistToken(token))));
    Event event = durable.poll(10, TimeUnit.SECONDS);
    assertNotNull(event);
    assertTrue(event instanceof Event.Durable);
    assertEquals(token, ((Event.Durable) event).token.id);
  }

  private static void commit(
      ActionDispatcher dispatcher, CompletableFuture<TSStatus> future, long reqId) {
    dispatcher.execute(
        Collections.singletonList(new Action.CompleteClient(reqId, Action.Outcome.COMMITTED, 1)));
    assertFalse("Commit alone must not answer before application", future.isDone());
  }
}
