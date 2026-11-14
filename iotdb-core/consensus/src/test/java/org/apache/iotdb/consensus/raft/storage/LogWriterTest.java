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

package org.apache.iotdb.consensus.raft.storage;

import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.core.MetaToken;
import org.apache.iotdb.consensus.raft.core.PersistToken;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogWriterTest {

  private File dir;

  @Before
  public void setUp() throws IOException {
    dir = Files.createTempDirectory("raftwriter").toFile();
  }

  @After
  public void tearDown() throws IOException {
    org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
  }

  private static LogEntry e(long index, long term, String data) {
    return LogRecordCodecTest.entry(index, term, data, EntryMeta.EMPTY);
  }

  @Test
  public void eventsArriveInOrderAfterDurability() throws Exception {
    SegmentedLogStore log =
        new SegmentedLogStore(new File(dir, "log"), EntryMetaCodec.EMPTY, 1 << 20, 1 << 20);
    log.recover();
    FileMetaStore meta = new FileMetaStore(dir);
    List<String> events = new CopyOnWriteArrayList<>();
    CountDownLatch done = new CountDownLatch(4);
    LogWriter w =
        new LogWriter(
            "test-writer",
            log,
            meta,
            ev -> {
              if (ev instanceof Event.Durable) {
                events.add("D" + ((Event.Durable) ev).token.id);
              } else {
                events.add("M" + ((Event.MetaDurable) ev).token.id);
              }
              done.countDown();
            },
            64,
            2);
    w.start();
    w.enqueue(
        new Action.Persist(Arrays.asList(e(0, 1, "a"), e(1, 1, "b")), -1, new PersistToken(1)));
    w.enqueue(new Action.PersistMeta(3, 2, new MetaToken(2)));
    w.enqueue(new Action.Persist(Collections.singletonList(e(2, 1, "c")), -1, new PersistToken(3)));
    w.enqueue(new Action.Persist(new ArrayList<>(), 1, new PersistToken(4))); // truncate from 1
    assertTrue(done.await(10, TimeUnit.SECONDS));
    assertEquals(Arrays.asList("D1", "M2", "D3", "D4"), events);
    assertEquals(3, meta.load().term);
    assertEquals(2, meta.load().votedFor);
    w.close();
    log.close();

    SegmentedLogStore again =
        new SegmentedLogStore(new File(dir, "log"), EntryMetaCodec.EMPTY, 1 << 20, 1 << 20);
    assertEquals(0, again.recover().contiguousEnd()); // entries 1 and 2 were truncated durably
    assertEquals("a", again.read(0).payloadAsString());
    again.close();
  }

  @Test
  public void persistMetaCarriesTheProfileMeta() throws Exception {
    SegmentedLogStore log =
        new SegmentedLogStore(new File(dir, "log"), EntryMetaCodec.EMPTY, 1 << 20, 1 << 20);
    log.recover();
    FileMetaStore meta = new FileMetaStore(dir);
    List<Long> durable = new CopyOnWriteArrayList<>();
    LogWriter w =
        new LogWriter(
            "test-writer", log, meta, ev -> durable.add(((Event.MetaDurable) ev).token.id), 64, 2);
    w.start();
    w.enqueue(new Action.PersistMeta(4, 1, new byte[] {9, 8, 7}, new MetaToken(1)));
    waitFor(durable, 1);
    assertEquals(4, meta.load().term);
    assertArrayEquals(new byte[] {9, 8, 7}, meta.load().profileMeta);
    w.enqueue(new Action.PersistMeta(5, 2, new MetaToken(2))); // Raft / TRaft: no profile meta
    waitFor(durable, 2);
    assertEquals(5, meta.load().term);
    assertEquals(0, meta.load().profileMeta.length);
    assertEquals(12, AtomicFiles.read(new File(dir, FileMetaStore.FILE_NAME)).length);
    w.close();
    log.close();
  }

  /**
   * A metadata write durable before the log records queued ahead of it would let a power loss keep
   * e.g. a raised term or sync number and lose an earlier truncation: the writer syncs the log
   * before a metadata write that follows log records of its batch, and only then (group commit
   * otherwise: one sync per batch).
   */
  @Test
  public void metadataIsDurableOnlyAfterTheLogWritesBeforeIt() throws Exception {
    List<String> io = new CopyOnWriteArrayList<>();
    List<String> events = new CopyOnWriteArrayList<>();
    CountDownLatch done = new CountDownLatch(6);
    LogWriter w =
        new LogWriter(
            "test-writer",
            new RecordingLogStore(io),
            recordingMeta(io),
            ev -> {
              events.add(
                  ev instanceof Event.Durable
                      ? "D" + ((Event.Durable) ev).token.id
                      : "M" + ((Event.MetaDurable) ev).token.id);
              done.countDown();
            },
            64,
            1000);
    // queued before the thread starts: one batch
    w.enqueue(new Action.Persist(Collections.singletonList(e(5, 2, "a")), 5, new PersistToken(1)));
    w.enqueue(new Action.PersistMeta(3, -1, new byte[] {1}, new MetaToken(2)));
    w.enqueue(new Action.Persist(Collections.singletonList(e(6, 3, "b")), -1, new PersistToken(3)));
    w.enqueue(new Action.Persist(Collections.singletonList(e(7, 3, "c")), -1, new PersistToken(4)));
    w.enqueue(new Action.PersistMeta(3, 2, new MetaToken(5)));
    w.enqueue(new Action.PersistMeta(4, -1, new MetaToken(6)));
    w.start();
    assertTrue(done.await(10, TimeUnit.SECONDS));
    w.close();
    assertEquals(
        Arrays.asList(
            "delete 5",
            "write 5",
            "sync",
            "meta t3",
            "write 6",
            "write 7",
            "sync",
            "meta t3",
            "meta t4"),
        io);
    assertEquals(Arrays.asList("D1", "M2", "D3", "D4", "M5", "M6"), events);
  }

  /** Without metadata in between, the log of a batch is synced once (group commit). */
  @Test
  public void logRecordsOfABatchShareOneSync() throws Exception {
    List<String> io = new ArrayList<>();
    LogWriter.writeBatch(
        Arrays.asList(
            new Action.PersistMeta(2, 1, new MetaToken(1)),
            new Action.Persist(Collections.singletonList(e(0, 2, "a")), -1, new PersistToken(2)),
            new Action.Persist(Collections.singletonList(e(1, 2, "b")), -1, new PersistToken(3)),
            new Action.Persist(new ArrayList<>(), 1, new PersistToken(4))),
        new RecordingLogStore(io),
        recordingMeta(io));
    assertEquals(Arrays.asList("meta t2", "write 0", "write 1", "delete 1", "sync"), io);
  }

  @Test
  public void onePersistActionCanContainSeveralEntriesInTheMeasuredBatch() throws Exception {
    long batches = ResearchMetrics.value("raft.log_batch_count");
    long actions = ResearchMetrics.value("raft.log_batch_action_count");
    long entries = ResearchMetrics.value("raft.log_batch_entry_count");
    long bytes = ResearchMetrics.value("raft.log_batch_payload_bytes");
    List<String> io = new CopyOnWriteArrayList<>();
    CountDownLatch done = new CountDownLatch(1);
    LogWriter writer =
        new LogWriter(
            "measured-writer",
            new RecordingLogStore(io),
            recordingMeta(io),
            event -> done.countDown(),
            1,
            0);
    writer.enqueue(
        new Action.Persist(Arrays.asList(e(1, 1, "a"), e(2, 1, "bc")), -1, new PersistToken(1)));
    writer.start();
    try {
      assertTrue(done.await(10, TimeUnit.SECONDS));
    } finally {
      writer.close();
    }
    assertEquals(batches + 1, ResearchMetrics.value("raft.log_batch_count"));
    assertEquals(actions + 1, ResearchMetrics.value("raft.log_batch_action_count"));
    assertEquals(entries + 2, ResearchMetrics.value("raft.log_batch_entry_count"));
    assertEquals(bytes + 3, ResearchMetrics.value("raft.log_batch_payload_bytes"));
    assertEquals(Arrays.asList("write 1", "write 2", "sync"), io);
  }

  private static Action.Persist persist(long id, long index) {
    return new Action.Persist(
        Collections.singletonList(e(index, 1, "v" + index)), -1, new PersistToken(id));
  }

  private static String name(Event ev) {
    return ev instanceof Event.Durable
        ? "D" + ((Event.Durable) ev).token.id
        : "M" + ((Event.MetaDurable) ev).token.id;
  }

  /** Buffered forcing: nothing is forced, nor handed back, before maxBatch entries are unforced. */
  @Test
  public void aForceWaitsForTheEntryCount() throws Exception {
    List<String> io = new CopyOnWriteArrayList<>();
    List<String> events = new CopyOnWriteArrayList<>();
    LogWriter w =
        new LogWriter(
            "count-writer",
            new RecordingLogStore(io),
            recordingMeta(io),
            ev -> events.add(name(ev)),
            3,
            60_000);
    w.start();
    try {
      w.enqueue(persist(1, 1));
      w.enqueue(persist(2, 2));
      Thread.sleep(300);
      assertEquals(Arrays.asList("write 1", "write 2"), io); // written at once, not forced
      assertTrue(events.isEmpty());
      w.enqueue(persist(3, 3));
      waitForSize(events, 3);
      assertEquals(Arrays.asList("D1", "D2", "D3"), events);
      assertEquals(Arrays.asList("write 1", "write 2", "write 3", "sync"), io);
    } finally {
      w.close();
    }
  }

  /** Buffered forcing: a lone entry is forced once it has waited maxDelayMs. */
  @Test
  public void aForceIsDueAfterTheDelay() throws Exception {
    List<String> io = new CopyOnWriteArrayList<>();
    List<String> events = new CopyOnWriteArrayList<>();
    LogWriter w =
        new LogWriter(
            "delay-writer",
            new RecordingLogStore(io),
            recordingMeta(io),
            ev -> events.add(name(ev)),
            1000,
            200);
    w.start();
    try {
      long start = System.nanoTime();
      w.enqueue(persist(1, 1));
      waitForSize(events, 1);
      long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      assertTrue("handed back after " + waitedMs + " ms", waitedMs >= 150);
      assertEquals(Arrays.asList("write 1", "sync"), io);
    } finally {
      w.close();
    }
  }

  /** A metadata write (a term or a vote) does not wait for the buffering delay. */
  @Test
  public void metadataForcesTheLogAtOnce() throws Exception {
    List<String> io = new CopyOnWriteArrayList<>();
    List<String> events = new CopyOnWriteArrayList<>();
    LogWriter w =
        new LogWriter(
            "meta-writer",
            new RecordingLogStore(io),
            recordingMeta(io),
            ev -> events.add(name(ev)),
            1000,
            60_000);
    w.start();
    try {
      w.enqueue(persist(1, 1));
      w.enqueue(new Action.PersistMeta(2, 1, new MetaToken(2)));
      waitForSize(events, 2);
      assertEquals(Arrays.asList("D1", "M2"), events);
      assertEquals(Arrays.asList("write 1", "sync", "meta t2"), io);
    } finally {
      w.close();
    }
  }

  /**
   * Pipelining: the writer keeps writing while a force is in progress, and an action is handed back
   * only after a force that started after it was written has finished.
   */
  @Test
  public void writesGoOnWhileAForceIsInProgress() throws Exception {
    List<String> io = new CopyOnWriteArrayList<>();
    List<String> events = new CopyOnWriteArrayList<>();
    CountDownLatch forcing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    LogStore store =
        new RecordingLogStore(io) {
          @Override
          public void sync() {
            super.sync();
            forcing.countDown();
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          }
        };
    LogWriter w =
        new LogWriter(
            "pipelined-writer", store, recordingMeta(io), ev -> events.add(name(ev)), 1, 0);
    w.start();
    try {
      w.enqueue(persist(1, 1));
      assertTrue(forcing.await(10, TimeUnit.SECONDS));
      w.enqueue(persist(2, 2));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!io.contains("write 2") && System.nanoTime() < deadline) {
        Thread.sleep(2);
      }
      assertEquals(Arrays.asList("write 1", "sync", "write 2"), io); // written during the force
      assertTrue(events.isEmpty()); // nothing is durable before the force returns
      release.countDown();
      waitForSize(events, 2);
      assertEquals(Arrays.asList("D1", "D2"), events);
      assertEquals(Arrays.asList("write 1", "sync", "write 2", "sync"), io);
    } finally {
      release.countDown();
      w.close();
    }
  }

  private static void waitForSize(List<String> events, int n) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (events.size() < n && System.nanoTime() < deadline) {
      Thread.sleep(2);
    }
    assertEquals(String.valueOf(events), n, events.size());
  }

  /** Records each metadata write (by its term) in {@code io}. */
  private static MetaStore recordingMeta(List<String> io) {
    return new MetaStore() {
      @Override
      public PersistentMeta load() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void persist(long term, int votedFor, byte[] profileMeta) {
        io.add("meta t" + term);
      }
    };
  }

  /** Records the order of the log writer's store operations. */
  private static class RecordingLogStore implements LogStore {
    private final List<String> io;

    RecordingLogStore(List<String> io) {
      this.io = io;
    }

    @Override
    public void write(LogEntry e) {
      io.add("write " + e.index);
    }

    @Override
    public void deleteFrom(long from) {
      io.add("delete " + from);
    }

    @Override
    public void sync() {
      io.add("sync");
    }

    @Override
    public LogEntry read(long index) {
      return null;
    }

    @Override
    public void purgeBefore(long index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.iotdb.consensus.raft.core.log.LogIndex recover() {
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

  private static void waitFor(List<Long> events, int n) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (events.size() < n && System.nanoTime() < deadline) {
      Thread.sleep(2);
    }
    assertEquals(n, events.size());
  }
}
