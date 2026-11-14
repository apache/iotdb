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

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.storage.LogMeta;
import org.apache.iotdb.consensus.raft.storage.LogStore;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A committed entry the applier cannot read stops it for good, loudly: nothing after the entry is
 * applied (the state machine would otherwise skip it and go on with a hole), the failure is kept
 * and reported once. An in-order applier never replays an index below its applied frontier; an
 * out-of-order one (ParallelRaft) executes every issued range and skips only what the installed
 * snapshot covers.
 */
public class RaftApplierTest {

  private final ExecutorService pool = Executors.newSingleThreadExecutor();

  @After
  public void tearDown() {
    pool.shutdownNow();
  }

  @Test
  public void aMissingCommittedEntryStopsTheApplier() throws Exception {
    MapLogStore store = new MapLogStore();
    for (long i = 0; i <= 5; i++) {
      if (i != 2) {
        store.entries.put(i, entry(i, "v" + i));
      }
    }
    RecordingStateMachine sm = new RecordingStateMachine();
    List<Long> appliedEvents = new CopyOnWriteArrayList<>();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    RaftApplier applier =
        new RaftApplier(
            "test",
            store,
            sm,
            new ClientRequestTable(),
            exec,
            ev -> appliedEvents.add(((Event.Applied) ev).index),
            failures::add);
    applier.enqueue(new Action.Apply(0, 3)); // e2 is missing
    applier.enqueue(new Action.Apply(4, 5));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0", "v1"), sm.applied);
    assertEquals(Arrays.asList(0L, 1L), appliedEvents);
    assertNotNull(applier.failure());
    assertEquals(1, failures.size());
    assertTrue(failures.get(0).getMessage(), failures.get(0).getMessage().contains("entry 2"));

    store.entries.put(2L, entry(2, "v2")); // even if it shows up later: the applier stays stopped
    applier.enqueue(new Action.Apply(2, 5));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0", "v1"), sm.applied);
    assertEquals(1, failures.size());
  }

  @Test
  public void anUnreadableCommittedEntryStopsTheApplier() throws Exception {
    MapLogStore store = new MapLogStore();
    store.entries.put(0L, entry(0, "v0"));
    store.entries.put(2L, entry(2, "v2"));
    store.unreadable = 1;
    RecordingStateMachine sm = new RecordingStateMachine();
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    RaftApplier applier =
        new RaftApplier("test", store, sm, new ClientRequestTable(), exec, ev -> {}, failures::add);
    applier.enqueue(new Action.Apply(0, 0));
    applier.enqueue(new Action.Apply(1, 1));
    applier.enqueue(new Action.Apply(2, 2));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0"), sm.applied);
    assertEquals(1, failures.size());
    assertTrue(failures.get(0) instanceof IOException);
  }

  @Test
  public void aStateMachineExceptionStopsBeforeAdvancingTheAppliedFrontier() throws Exception {
    MapLogStore store = new MapLogStore();
    for (long i = 0; i < 3; i++) {
      store.entries.put(i, entry(i, "v" + i));
    }
    RecordingStateMachine sm =
        new RecordingStateMachine() {
          @Override
          public org.apache.iotdb.common.rpc.thrift.TSStatus write(
              org.apache.iotdb.consensus.common.request.IConsensusRequest request) {
            if (((StringRequest) request).value.equals("v1")) {
              throw new IllegalStateException("injected storage failure");
            }
            return super.write(request);
          }
        };
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    List<Long> applied = new CopyOnWriteArrayList<>();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    RaftApplier applier =
        new RaftApplier(
            "test",
            store,
            sm,
            new ClientRequestTable(),
            exec,
            ev -> applied.add(((Event.Applied) ev).index),
            failures::add);
    applier.enqueue(new Action.Apply(0, 2));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0"), sm.applied);
    assertEquals(Arrays.asList(0L), applied);
    assertEquals(1, failures.size());
    applier.enqueue(new Action.Apply(2, 2));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0"), sm.applied);
  }

  @Test
  public void aHealthyApplierHasNoFailure() throws Exception {
    MapLogStore store = new MapLogStore();
    store.entries.put(0L, entry(0, "v0"));
    RecordingStateMachine sm = new RecordingStateMachine();
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    RaftApplier applier =
        new RaftApplier("test", store, sm, new ClientRequestTable(), exec, ev -> {});
    applier.enqueue(new Action.Apply(0, 0));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0"), sm.applied);
    assertNull(applier.failure());
  }

  /**
   * ParallelRaft's apply policy issues ranges in dependency order (contract R9): a later range may
   * lie below an earlier one, and every issued range is executed as issued.
   */
  @Test
  public void rangesIssuedOutOfIndexOrderAreAllApplied() throws Exception {
    MapLogStore store = new MapLogStore();
    for (long i = 1; i <= 4; i++) {
      store.entries.put(i, entry(i, "v" + i));
    }
    RecordingStateMachine sm = new RecordingStateMachine();
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    List<Long> applied = new CopyOnWriteArrayList<>();
    RaftApplier applier =
        new RaftApplier(
            "test",
            store,
            sm,
            new ClientRequestTable(),
            exec,
            ev -> applied.add(((Event.Applied) ev).index),
            t -> {},
            true);
    applier.enqueue(new Action.Apply(1, 1));
    applier.enqueue(new Action.Apply(3, 3));
    applier.enqueue(new Action.Apply(2, 2));
    applier.enqueue(new Action.Apply(4, 4));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v1", "v3", "v2", "v4"), sm.applied);
    assertEquals(Arrays.asList(1L, 3L, 2L, 4L), applied);
    assertNull(applier.failure());
  }

  /** Whatever the profile, the indexes an installed snapshot covers are never applied again. */
  @Test
  public void indexesCoveredByTheInstalledSnapshotAreSkipped() throws Exception {
    for (boolean outOfOrder : new boolean[] {true, false}) {
      MapLogStore store = new MapLogStore();
      for (long i = 3; i <= 7; i++) {
        store.entries.put(i, entry(i, "v" + i));
      }
      RecordingStateMachine sm = new RecordingStateMachine();
      GroupExecutor exec = new GroupExecutor("test-apply", pool);
      List<Long> applied = new CopyOnWriteArrayList<>();
      RaftApplier applier =
          new RaftApplier(
              "test",
              store,
              sm,
              new ClientRequestTable(),
              exec,
              ev -> applied.add(((Event.Applied) ev).index),
              t -> {},
              outOfOrder);
      applier.installedSnapshot(5);
      applier.enqueue(new Action.Apply(3, 7));
      awaitIdle(exec);
      assertEquals("outOfOrder=" + outOfOrder, Arrays.asList("v6", "v7"), sm.applied);
      assertEquals("outOfOrder=" + outOfOrder, Arrays.asList(6L, 7L), applied);
      assertNull(applier.failure());
    }
  }

  /** The in-order applier (Raft) keeps its running guard: no index below its frontier replays. */
  @Test
  public void anInOrderApplierNeverReappliesBelowItsFrontier() throws Exception {
    MapLogStore store = new MapLogStore();
    for (long i = 0; i <= 4; i++) {
      store.entries.put(i, entry(i, "v" + i));
    }
    RecordingStateMachine sm = new RecordingStateMachine();
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    List<Long> applied = new CopyOnWriteArrayList<>();
    RaftApplier applier =
        new RaftApplier(
            "test",
            store,
            sm,
            new ClientRequestTable(),
            exec,
            ev -> applied.add(((Event.Applied) ev).index),
            t -> {});
    applier.enqueue(new Action.Apply(0, 2));
    applier.enqueue(new Action.Apply(1, 3)); // overlaps the applied prefix: only e3 is new
    applier.enqueue(new Action.Apply(4, 4));
    awaitIdle(exec);
    assertEquals(Arrays.asList("v0", "v1", "v2", "v3", "v4"), sm.applied);
    assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), applied);
    assertNull(applier.failure());
  }

  /**
   * Batched application: a run of data entries reaches the state machine with one writeBatch call;
   * a control entry splits the run and applies on its own; completion follows index order.
   */
  @Test
  public void batchedApplicationWritesARunAtOnceInIndexOrder() throws Exception {
    MapLogStore store = new MapLogStore();
    for (long i = 0; i < 5; i++) {
      store.entries.put(i, i == 2 ? noop(i) : entry(i, "v" + i));
    }
    List<Integer> batches = new CopyOnWriteArrayList<>();
    RecordingStateMachine sm =
        new RecordingStateMachine() {
          @Override
          public List<org.apache.iotdb.common.rpc.thrift.TSStatus> writeBatch(
              List<org.apache.iotdb.consensus.common.request.IConsensusRequest> requests) {
            batches.add(requests.size());
            return super.writeBatch(requests);
          }
        };
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    List<Long> applied = new CopyOnWriteArrayList<>();
    RaftApplier applier =
        new RaftApplier(
            "test",
            store,
            sm,
            new ClientRequestTable(),
            exec,
            ev -> applied.add(((Event.Applied) ev).index));
    applier.setBatch(true);
    applier.enqueue(new Action.Apply(0, 4));
    awaitIdle(exec);
    assertEquals(Arrays.asList(2, 2), batches);
    assertEquals(Arrays.asList("v0", "v1", "v3", "v4"), sm.applied);
    assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), applied);
    assertEquals(4, applier.appliedIndex());
    assertNull(applier.failure());
  }

  /**
   * Batched application: the first unsuccessful status of a run stops the applier at that entry;
   * the entries before it complete, nothing after it does, and no later range runs.
   */
  @Test
  public void anUnsuccessfulStatusInARunStopsAtThatEntry() throws Exception {
    MapLogStore store = new MapLogStore();
    for (long i = 0; i < 4; i++) {
      store.entries.put(i, entry(i, "v" + i));
    }
    RecordingStateMachine sm =
        new RecordingStateMachine() {
          @Override
          public org.apache.iotdb.common.rpc.thrift.TSStatus write(
              org.apache.iotdb.consensus.common.request.IConsensusRequest request) {
            if (((StringRequest) request).value.equals("v2")) {
              return org.apache.iotdb.rpc.RpcUtils.getStatus(
                  org.apache.iotdb.rpc.TSStatusCode.INTERNAL_SERVER_ERROR, "injected");
            }
            return super.write(request);
          }
        };
    GroupExecutor exec = new GroupExecutor("test-apply", pool);
    List<Long> applied = new CopyOnWriteArrayList<>();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    RaftApplier applier =
        new RaftApplier(
            "test",
            store,
            sm,
            new ClientRequestTable(),
            exec,
            ev -> applied.add(((Event.Applied) ev).index),
            failures::add);
    applier.setBatch(true);
    applier.enqueue(new Action.Apply(0, 3));
    awaitIdle(exec);
    assertEquals(Arrays.asList(0L, 1L), applied);
    assertEquals(1, failures.size());
    assertNotNull(applier.failure());
    assertEquals(1, applier.appliedIndex());
    applier.enqueue(new Action.Apply(4, 4));
    awaitIdle(exec);
    assertEquals(Arrays.asList(0L, 1L), applied);
  }

  private static LogEntry noop(long index) {
    return new LogEntry(index, 1, EntryType.NOOP, EntryMeta.EMPTY, ByteBuffer.allocate(0));
  }

  private static LogEntry entry(long index, String data) {
    return new LogEntry(
        index,
        1,
        EntryType.DATA,
        EntryMeta.EMPTY,
        ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
  }

  private static void awaitIdle(GroupExecutor exec) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!exec.isIdle() && System.nanoTime() < deadline) {
      Thread.sleep(2);
    }
    assertTrue(exec.isIdle());
  }

  /** A log store over a map; reading {@code unreadable} fails with an I/O error. */
  private static final class MapLogStore implements LogStore {
    final Map<Long, LogEntry> entries = new ConcurrentHashMap<>();
    volatile long unreadable = -1;

    @Override
    public void write(LogEntry e) {
      entries.put(e.index, e);
    }

    @Override
    public void deleteFrom(long from) {
      entries.keySet().removeIf(i -> i >= from);
    }

    @Override
    public void sync() {}

    @Override
    public LogEntry read(long index) throws IOException {
      if (index == unreadable) {
        throw new IOException("bad record at " + index);
      }
      return entries.get(index);
    }

    @Override
    public void purgeBefore(long index) {}

    @Override
    public LogIndex recover() {
      throw new UnsupportedOperationException();
    }

    @Override
    public LogMeta meta() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long loadCount() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
