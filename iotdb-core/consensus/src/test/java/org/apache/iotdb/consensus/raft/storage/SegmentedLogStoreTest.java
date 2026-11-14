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

import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SegmentedLogStoreTest {

  private File dir;

  @Before
  public void setUp() throws IOException {
    dir = Files.createTempDirectory("raftlog").toFile();
  }

  @After
  public void tearDown() throws IOException {
    org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
  }

  private SegmentedLogStore open(long segmentBytes, long cacheBytes) throws IOException {
    return new SegmentedLogStore(
        new File(dir, "log"), EntryMetaCodec.EMPTY, segmentBytes, cacheBytes);
  }

  private static LogEntry e(long index, long term, String data) {
    return LogRecordCodecTest.entry(index, term, data, EntryMeta.EMPTY);
  }

  private SegmentedLogStore openDated(long segmentBytes, long cacheBytes) throws IOException {
    return new SegmentedLogStore(
        new File(dir, "log"), LogRecordCodecTest.LONG_CODEC, segmentBytes, cacheBytes);
  }

  /** An entry whose metadata is a "date" (one long), as a dated overwrite would carry. */
  private static LogEntry dated(long index, long term, String data, long date) {
    return LogRecordCodecTest.entry(index, term, data, new LogRecordCodecTest.LongMeta(date));
  }

  private static long dateOf(EntryMeta meta) {
    return ((LogRecordCodecTest.LongMeta) meta).v;
  }

  @Test
  public void writeSyncRecoverAcrossSegments() throws IOException {
    SegmentedLogStore s = open(120, 1 << 20);
    s.recover();
    for (int i = 0; i < 10; i++) {
      s.write(e(i, 1, "v" + i));
    }
    s.sync();
    assertTrue(s.segmentCount() > 1);
    s.close();

    SegmentedLogStore r = open(120, 1 << 20);
    LogIndex idx = r.recover();
    assertEquals(9, idx.contiguousEnd());
    assertEquals(9, idx.durableContiguousEnd());
    assertEquals("v4", r.read(4).payloadAsString());
    assertEquals("v9", r.read(9).payloadAsString());
    assertEquals(2, r.loadCount()); // both served from disk after a cold start
    assertEquals("v4", r.read(4).payloadAsString());
    assertEquals(2, r.loadCount()); // second read is a cache hit
    r.close();
  }

  @Test
  public void truncateRecordIsReplayedAndHolesSurvive() throws IOException {
    SegmentedLogStore s = open(1 << 20, 1 << 20);
    s.recover();
    s.write(e(0, 1, "a"));
    s.write(e(1, 1, "b"));
    s.write(e(2, 1, "c"));
    s.write(e(4, 1, "e")); // hole at 3
    s.deleteFrom(2); // removes 2 and 4
    s.write(e(2, 2, "c2"));
    s.sync();
    assertNull(s.read(4));
    assertEquals(2, s.read(2).term);
    s.close();

    SegmentedLogStore r = open(1 << 20, 1 << 20);
    LogIndex idx = r.recover();
    assertEquals(Arrays.asList(0L, 1L, 2L), new ArrayList<>(idx.slots()));
    assertEquals(2, idx.termAt(2));
    assertEquals("c2", r.read(2).payloadAsString());
    r.write(e(5, 2, "f"));
    r.sync();
    r.close();
    SegmentedLogStore r2 = open(1 << 20, 1 << 20);
    LogIndex idx2 = r2.recover();
    assertEquals(2, idx2.contiguousEnd());
    assertTrue(idx2.has(5));
    assertFalse(idx2.has(4));
    r2.close();
  }

  @Test
  public void incompleteTailIsCutOnRecovery() throws IOException {
    SegmentedLogStore s = open(1 << 20, 1 << 20);
    s.recover();
    s.write(e(0, 1, "a"));
    s.write(e(1, 1, "b"));
    s.sync();
    s.close();
    File seg = new File(dir, "log").listFiles((d, n) -> n.startsWith("seg_"))[0];
    long good = seg.length();
    Files.write(
        seg.toPath(), new byte[] {1, 2, 3, 4, 5, 6, 7}, StandardOpenOption.APPEND); // torn record
    SegmentedLogStore r = open(1 << 20, 1 << 20);
    LogIndex idx = r.recover();
    assertEquals(1, idx.contiguousEnd());
    assertEquals(good, seg.length());
    r.write(e(2, 1, "c")); // appends after the cut point
    r.sync();
    r.close();
    SegmentedLogStore r2 = open(1 << 20, 1 << 20);
    assertEquals(2, r2.recover().contiguousEnd());
    assertEquals("c", r2.read(2).payloadAsString());
    r2.close();
  }

  @Test
  public void completeCorruptionInLastOrSealedSegmentFailsRecovery() throws IOException {
    SegmentedLogStore store = open(28, 1 << 20);
    store.recover();
    store.write(e(0, 1, "a"));
    store.write(e(1, 1, "b"));
    store.sync();
    store.close();
    File[] files = new File(dir, "log").listFiles((d, name) -> name.startsWith("seg_"));
    assertEquals(2, files.length);
    for (File file : files) {
      byte[] good = Files.readAllBytes(file.toPath());
      byte[] bad = good.clone();
      bad[bad.length - 1] ^= 1;
      Files.write(file.toPath(), bad);
      try (SegmentedLogStore recovering = open(28, 1 << 20)) {
        try {
          recovering.recover();
          org.junit.Assert.fail("Complete corrupt record accepted");
        } catch (IOException expected) {
          org.junit.Assert.assertArrayEquals(bad, Files.readAllBytes(file.toPath()));
        }
      }
      Files.write(file.toPath(), good);
    }
  }

  @Test
  public void purgeDropsWholeSegmentsBelowFirstIndex() throws IOException {
    SegmentedLogStore s = open(100, 1 << 20);
    s.recover();
    for (int i = 0; i < 12; i++) {
      s.write(e(i, 1, "value-" + i));
    }
    s.sync();
    int before = s.segmentCount();
    s.meta().save(8, 1, new byte[0]);
    s.purgeBefore(8);
    assertTrue(s.segmentCount() < before);
    assertNull(s.read(3));
    assertEquals("value-9", s.read(9).payloadAsString());
    s.close();
    SegmentedLogStore r = open(100, 1 << 20);
    LogIndex idx = r.recover();
    assertEquals(8, idx.firstIndex());
    assertEquals(11, idx.contiguousEnd());
    assertEquals(1, idx.termAt(7)); // snapshot term below firstIndex
    r.close();
  }

  @Test
  public void cacheBoundsBytesAndCountsMisses() throws IOException {
    SegmentedLogStore s = open(1 << 20, 200);
    s.recover();
    for (int i = 0; i < 5; i++) {
      s.write(e(i, 1, "0123456789"));
    }
    s.sync();
    assertTrue(s.cache().size() < 5);
    assertEquals("0123456789", s.read(0).payloadAsString());
    assertEquals(1, s.loadCount());
    s.close();
  }

  @Test
  public void overwriteOfTheSameIndexIsConsistentInCacheIndexMapAndReplay() throws IOException {
    // one record per segment: every copy of e1 lives in its own segment
    SegmentedLogStore s = openDated(1, 1 << 20);
    s.recover();
    s.write(dated(0, 1, "a", 1));
    s.write(dated(1, 1, "old", 1));
    s.write(dated(1, 1, "newer", 2)); // same (index, term), later write: overwrite
    s.sync();
    assertEquals("newer", s.read(1).payloadAsString()); // cache
    assertEquals(2, dateOf(s.read(1).meta));
    s.close();

    SegmentedLogStore r = openDated(1, 1 << 20);
    LogIndex idx = r.recover(); // replay
    assertEquals(Arrays.asList(0L, 1L), new ArrayList<>(idx.slots()));
    assertEquals(1, idx.termAt(1));
    assertEquals(2, dateOf(idx.metaAt(1)));
    assertEquals("newer".length(), idx.sizeAt(1));
    assertEquals("newer", r.read(1).payloadAsString()); // index map: served from disk
    assertEquals(1, r.loadCount());
    r.write(dated(1, 1, "newest", 3)); // overwrite again after a restart
    r.sync();
    assertEquals("newest", r.read(1).payloadAsString());
    assertEquals(1, r.loadCount()); // the cache holds the new copy, not the stale one
    r.close();

    SegmentedLogStore r2 = openDated(1, 1 << 20);
    LogIndex idx2 = r2.recover();
    assertEquals(3, dateOf(idx2.metaAt(1)));
    assertEquals("newest", r2.read(1).payloadAsString());
    assertEquals("a", r2.read(0).payloadAsString());
    r2.close();
  }

  @Test
  public void overwriteIsServedFromTheIndexMapWhenTheCacheIsCold() throws IOException {
    SegmentedLogStore s = openDated(1 << 20, 0); // no cache: every read goes through the index map
    s.recover();
    s.write(dated(4, 2, "first", 2));
    s.write(dated(5, 2, "other", 2));
    s.write(dated(4, 2, "second", 3)); // same segment, later offset
    assertEquals("second", s.read(4).payloadAsString());
    assertEquals(3, dateOf(s.read(4).meta));
    assertEquals("other", s.read(5).payloadAsString());
    s.sync();
    s.close();
    SegmentedLogStore r = openDated(1 << 20, 0);
    LogIndex idx = r.recover();
    assertEquals(3, dateOf(idx.metaAt(4)));
    assertEquals("second", r.read(4).payloadAsString());
    r.close();
  }

  @Test
  public void purgeKeepsTruncateRecordsThatOlderSegmentsStillNeed() throws IOException {
    // two records per segment
    SegmentedLogStore s = openDated(50, 1 << 20);
    s.recover();
    s.write(dated(1, 1, "a", 1)); // segment 1: e1 (live) and e3 (truncated below)
    s.write(dated(3, 1, "x", 1));
    s.deleteFrom(2); // segment 2: TRUNCATE(2) and e2
    s.write(dated(2, 2, "b", 2));
    s.write(dated(2, 2, "c", 3)); // segment 3: e2 overwritten, so segment 2 holds no live entry
    s.sync();
    assertNull(s.read(3));
    s.purgeBefore(0); // nothing below the first index; segment 2 is no longer referenced
    s.close();

    SegmentedLogStore r = openDated(50, 1 << 20);
    LogIndex idx = r.recover();
    assertFalse("a truncated entry came back on replay", idx.has(3));
    assertEquals(Arrays.asList(1L, 2L), new ArrayList<>(idx.slots()));
    assertEquals("c", r.read(2).payloadAsString());
    assertNull(r.read(3));
    // once the older segment goes (compaction past e1), the truncating segment goes with it
    r.meta().save(2, 1, new byte[0]);
    r.purgeBefore(2);
    assertEquals(1, r.segmentCount());
    r.close();
    SegmentedLogStore r2 = openDated(50, 1 << 20);
    LogIndex idx2 = r2.recover();
    assertEquals(Collections.singletonList(2L), new ArrayList<>(idx2.slots()));
    assertEquals("c", r2.read(2).payloadAsString());
    r2.close();
  }

  @Test
  public void purgeKeepsATruncateWhoseOwnEntriesWereCutOnTheRaftPath() throws IOException {
    // The plain Raft write path (deleteFrom, then write; no overwrite). An entry record is 27 B
    // plus its payload, a TRUNCATE record 27 B; a segment rolls once it reaches 100 B.
    SegmentedLogStore s = open(100, 1 << 20);
    s.recover();
    for (long i = 1; i <= 4; i++) {
      s.write(e(i, 1, "a")); // segment 1: e1..e4 at term 1
    }
    StringBuilder big = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      big.append('x');
    }
    s.write(e(5, 1, big.toString())); // segment 2: e5 ...
    s.deleteFrom(3); // ... then TRUNCATE(3), which cuts e5 itself and fills the segment
    s.write(e(3, 2, "b")); // segment 3: e3 at term 2
    s.sync();
    assertEquals(3, s.segmentCount());
    // segment 2 holds no live entry, but its TRUNCATE still cancels e3 and e4 of segment 1
    s.purgeBefore(1);
    assertEquals(3, s.segmentCount());
    s.close();

    SegmentedLogStore r = open(100, 1 << 20);
    LogIndex idx = r.recover();
    assertFalse("a truncated entry came back on replay", idx.has(4));
    assertEquals(Arrays.asList(1L, 2L, 3L), new ArrayList<>(idx.slots()));
    assertEquals(2, idx.termAt(3));
    assertEquals("b", r.read(3).payloadAsString());
    r.close();
  }

  /** writeAll appends the records write would, with one write call, and they replay the same. */
  @Test
  public void writeAllRecordsReplayLikeSingleWrites() throws IOException {
    SegmentedLogStore s = open(1 << 20, 1 << 20);
    s.recover();
    List<LogEntry> batch = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      batch.add(e(i, 1, "v" + i));
    }
    s.writeAll(batch);
    s.write(e(5, 1, "v5"));
    s.writeAll(Arrays.asList(e(6, 2, "v6"), e(7, 2, "v7")));
    assertEquals("v6", s.read(6).payloadAsString()); // cached by writeAll
    s.sync();
    s.close();

    SegmentedLogStore r = open(1 << 20, 1 << 20);
    LogIndex idx = r.recover();
    assertEquals(7, idx.contiguousEnd());
    for (int i = 0; i <= 7; i++) {
      assertEquals("v" + i, r.read(i).payloadAsString());
    }
    assertEquals(2, idx.termAt(7));
    r.close();
  }

  /**
   * A force runs outside the store's lock: while one is in progress, cache hits, disk reads and
   * further writes all go ahead.
   */
  @Test
  public void readsAndWritesDoNotWaitForAForceInProgress() throws Exception {
    // room for one entry (64 + payload bytes each): index 0 is evicted and read from disk
    SegmentedLogStore s = open(1 << 20, 75);
    s.recover();
    s.write(e(0, 1, "first"));
    s.write(e(1, 1, "second"));
    CountDownLatch forcing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    s.setBeforeForce(
        () -> {
          forcing.countDown();
          try {
            release.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        });
    AtomicReference<Throwable> syncFailure = new AtomicReference<>();
    Thread syncer =
        new Thread(
            () -> {
              try {
                s.sync();
              } catch (Throwable t) {
                syncFailure.set(t);
              }
            });
    syncer.start();
    assertTrue(forcing.await(10, TimeUnit.SECONDS));
    try {
      assertEquals("second", s.read(1).payloadAsString()); // cache hit
      assertEquals("first", s.read(0).payloadAsString()); // disk read under the store lock
      s.write(e(2, 1, "third"));
      assertEquals("third", s.read(2).payloadAsString());
      assertTrue(syncer.isAlive()); // all of that while the force was still blocked
    } finally {
      release.countDown();
    }
    syncer.join(TimeUnit.SECONDS.toMillis(10));
    assertNull(syncFailure.get());
    s.setBeforeForce(() -> {});
    s.sync(); // entry 2 was written during the force: dirty again
    s.close();

    SegmentedLogStore r = open(1 << 20, 1 << 20);
    assertEquals(2, r.recover().contiguousEnd());
    assertEquals("third", r.read(2).payloadAsString());
    r.close();
  }

  /** Segments roll over while another thread keeps forcing: nothing is lost or left torn. */
  @Test
  public void rolloverWhileSyncingConcurrently() throws Exception {
    SegmentedLogStore s = open(512, 1 << 20);
    s.recover();
    AtomicBoolean stop = new AtomicBoolean();
    AtomicReference<Throwable> syncFailure = new AtomicReference<>();
    Thread syncer =
        new Thread(
            () -> {
              while (!stop.get()) {
                try {
                  s.sync();
                } catch (Throwable t) {
                  syncFailure.set(t);
                  return;
                }
              }
            });
    syncer.start();
    try {
      int index = 0;
      while (index < 2000) {
        if (index % 3 == 0) {
          s.write(e(index, 1, "v" + index));
          index++;
        } else {
          List<LogEntry> batch = new ArrayList<>();
          for (int k = 0; k < 4; k++, index++) {
            batch.add(e(index, 1, "v" + index));
          }
          s.writeAll(batch);
        }
      }
    } finally {
      stop.set(true);
      syncer.join(TimeUnit.SECONDS.toMillis(10));
    }
    assertNull(syncFailure.get());
    assertTrue(s.segmentCount() > 10);
    s.close();

    SegmentedLogStore r = open(512, 1 << 20);
    LogIndex idx = r.recover();
    assertTrue(idx.contiguousEnd() >= 1999);
    for (int i = 0; i < 2000; i += 97) {
      assertEquals("v" + i, r.read(i).payloadAsString());
    }
    r.close();
  }
}
