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

package org.apache.iotdb.consensus.traft.paper.runtime;

import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.traft.paper.core.TRaftEngine;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Application;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TRaftFileStoreTest {
  private File directory;

  @Before
  public void setUp() throws Exception {
    directory = Files.createTempDirectory("traft-journal").toFile();
  }

  @After
  public void tearDown() {
    org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(directory);
  }

  private Path journal() {
    return new File(directory, "paper-journal-v1").toPath();
  }

  private static DurableRecord term(long term) {
    return new DurableRecord(
        RecordType.TERM_VOTE, term, 2, null, null, null, -1, Collections.emptyList());
  }

  private static DurableRecord data(long index) {
    Entry entry =
        new Entry(new EntryId(1, index), 0, index, 0, Kind.DATA, new byte[] {(byte) index});
    return new DurableRecord(RecordType.ENTRY, 1, -1, entry, null, null, -1, null);
  }

  @Test
  public void bufferedForceIsDueAfterEnoughDataRecordsOrTheInterval() throws Exception {
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.setPipelined(true);
      store.awaitForceDue(); // Nothing written: no wait.
      store.setForcePolicy(60_000, 3);
      store.append(data(1));
      store.append(data(2));
      Thread waiter =
          new Thread(
              () -> {
                try {
                  store.awaitForceDue();
                } catch (InterruptedException ignored) {
                  Thread.currentThread().interrupt();
                }
              });
      waiter.start();
      waiter.join(200);
      assertTrue("Two of three data records are not due", waiter.isAlive());
      store.append(data(3));
      waiter.join(5_000);
      assertFalse("The third data record makes the force due", waiter.isAlive());
      assertTrue(store.hasPendingForce());
      store.forceWritten();
      assertFalse(store.hasPendingForce());

      store.setForcePolicy(50, 1000);
      long start = System.nanoTime();
      store.append(data(4));
      store.awaitForceDue();
      assertTrue(
          "One record waits for the interval",
          System.nanoTime() - start >= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(45));
      store.forceWritten();

      store.setForcePolicy(60_000, 1000);
      store.append(data(5));
      Thread closing =
          new Thread(
              () -> {
                try {
                  store.awaitForceDue();
                } catch (InterruptedException ignored) {
                  Thread.currentThread().interrupt();
                }
              });
      closing.start();
      closing.join(100);
      assertTrue(closing.isAlive());
      store.close();
      closing.join(5_000);
      assertFalse("Closing wakes a waiting force", closing.isAlive());
    }
  }

  @Test
  public void appendForcesEveryRecordAndRecoversExactPayloads() throws Exception {
    Entry entry = new Entry(new EntryId(7, 14), 2, -10, 3, Kind.DATA, new byte[] {1, 2, 3});
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.append(term(7));
      store.append(new DurableRecord(RecordType.ENTRY, 7, -1, entry, null, null, -1, null));
      assertEquals(2, store.forcedRecords());
      assertTrue(store.writtenBytes() > 24);
      assertTrue(store.forceNanos() > 0);
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      List<DurableRecord> records = store.load();
      assertEquals(2, records.size());
      assertEquals(7, records.get(0).term);
      assertEquals(2, records.get(0).vote);
      assertEquals(entry, records.get(1).entry);
    }
  }

  @Test
  public void proposalBatchForcesOnceAndRetainsBothExistingFormatRecords() throws Exception {
    Entry entry = new Entry(new EntryId(3, 1), 0, 14, 0, Kind.DATA, new byte[] {3, 1, 4});
    Proposal proposal = new Proposal(null, Collections.emptyList(), Entry.metadata(entry), false);
    DurableRecord data = new DurableRecord(RecordType.ENTRY, 3, -1, entry, null, null, -1, null);
    DurableRecord retained =
        new DurableRecord(RecordType.RETAIN_PROPOSAL, 3, -1, null, proposal, null, -1, null);
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      long before = ResearchMetrics.value("traft.log_force_count");
      store.appendBatch(Arrays.asList(data, retained));
      assertEquals(
          "The complete pair crosses one synchronous durability barrier",
          before + 1,
          ResearchMetrics.value("traft.log_force_count"));
      assertEquals(
          "Record accounting is distinct from force-call accounting", 2, store.forcedRecords());
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
    }
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      List<DurableRecord> records = store.load();
      assertEquals(2, records.size());
      assertEquals(RecordType.ENTRY, records.get(0).type);
      assertEquals(RecordType.RETAIN_PROPOSAL, records.get(1).type);
      assertEquals(entry, records.get(0).entry);
      assertEquals(proposal, records.get(1).proposal);
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
    }
  }

  @Test
  public void invalidBatchDoesNotWriteAnEarlierValidRecord() throws Exception {
    Entry entry = new Entry(new EntryId(1, 1), 0, 10, 0, Kind.DATA, new byte[] {7});
    Entry conflict = new Entry(entry.id, 0, 11, 0, Kind.DATA, new byte[] {7});
    DurableRecord data = new DurableRecord(RecordType.ENTRY, 1, -1, entry, null, null, -1, null);
    DurableRecord conflicting =
        new DurableRecord(RecordType.ENTRY, 1, -1, conflict, null, null, -1, null);
    Proposal proposal = new Proposal(null, Collections.emptyList(), Entry.metadata(entry), false);
    DurableRecord retained =
        new DurableRecord(RecordType.RETAIN_PROPOSAL, 1, -1, null, proposal, null, -1, null);
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      long size = Files.size(journal());
      long forces = ResearchMetrics.value("traft.log_force_count");
      for (List<DurableRecord> invalid :
          Arrays.asList(Arrays.asList(data, conflicting), Arrays.asList(retained, data))) {
        try {
          store.appendBatch(invalid);
          fail("Reject conflicting identities or metadata preceding its payload");
        } catch (IOException expected) {
          assertEquals(size, Files.size(journal()));
          assertEquals(forces, ResearchMetrics.value("traft.log_force_count"));
          assertEquals(0, store.forcedRecords());
        }
      }
      store.appendBatch(Arrays.asList(data, retained));
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
    }
  }

  @Test
  public void incompleteRetainRecoversOnlyTheCompletePrefix() throws Exception {
    Entry entry = new Entry(new EntryId(4, 1), 0, 19, 0, Kind.DATA, new byte[] {8, 6});
    Proposal proposal = new Proposal(null, Collections.emptyList(), Entry.metadata(entry), false);
    DurableRecord retained =
        new DurableRecord(RecordType.RETAIN_PROPOSAL, 4, -1, null, proposal, null, -1, null);
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.append(new DurableRecord(RecordType.ENTRY, 4, -1, entry, null, null, -1, null));
      store.append(retained);
    }
    byte[] complete = Files.readAllBytes(journal());
    int firstFrameLength = 12 + java.nio.ByteBuffer.wrap(complete).getInt(4);
    Files.write(journal(), Arrays.copyOf(complete, complete.length - 1));
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      List<DurableRecord> records = store.load();
      assertEquals("An incomplete RETAIN cannot fix a proposal", 1, records.size());
      assertEquals(RecordType.ENTRY, records.get(0).type);
      assertEquals(firstFrameLength, Files.size(journal()));
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
      store.append(retained);
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      assertEquals(2, store.load().size());
    }
  }

  @Test
  public void batchedEntryAndRetainShareOneFrameAndTearTogether() throws Exception {
    Entry entry = new Entry(new EntryId(4, 3), 1, 19, 2, Kind.DATA, new byte[] {8, 6});
    Proposal proposal =
        new Proposal(
            new EntryId(4, 1),
            Arrays.asList(new EntryId(4, 1), new EntryId(4, 2)),
            Entry.metadata(entry),
            false);
    List<DurableRecord> pair =
        Arrays.asList(
            new DurableRecord(RecordType.ENTRY, 4, -1, entry, null, null, -1, null),
            new DurableRecord(RecordType.RETAIN_PROPOSAL, 4, -1, null, proposal, null, -1, null));
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.appendBatch(pair);
      assertEquals(2, store.forcedRecords());
      assertEquals(store.payloadEnd(entry.id), store.retainEnd(entry.id));
    }
    byte[] complete = Files.readAllBytes(journal());
    assertEquals(
        "One frame holds both records",
        complete.length,
        12 + java.nio.ByteBuffer.wrap(complete).getInt(4));
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      List<DurableRecord> records = store.load();
      assertEquals(2, records.size());
      for (List<DurableRecord> view : Arrays.asList(records, new ArrayList<>(records))) {
        assertEquals(RecordType.ENTRY, view.get(0).type);
        assertEquals(entry, view.get(0).entry);
        assertEquals(null, view.get(0).proposal);
        assertEquals(RecordType.RETAIN_PROPOSAL, view.get(1).type);
        assertEquals(proposal, view.get(1).proposal);
      }
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
    }
    Files.write(journal(), Arrays.copyOf(complete, complete.length - 1));
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      assertEquals("A torn frame keeps neither record", 0, store.load().size());
      assertEquals(0, Files.size(journal()));
      store.appendBatch(pair);
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      assertEquals(2, store.load().size());
    }
  }

  @Test
  public void everyIncompleteTailIsTruncatedBeforeSubsequentAppend() throws Exception {
    int boundary;
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.append(term(1));
      boundary = (int) Files.size(journal());
      store.append(term(2));
    }
    byte[] complete = Files.readAllBytes(journal());
    for (int length = boundary + 1; length < complete.length; length++) {
      Files.write(journal(), Arrays.copyOf(complete, length));
      try (TRaftFileStore store = new TRaftFileStore(directory)) {
        List<DurableRecord> loaded = store.load();
        assertEquals("tail length " + length, 1, loaded.size());
        assertEquals(boundary, Files.size(journal()));
        store.append(term(3));
      }
      try (TRaftFileStore store = new TRaftFileStore(directory)) {
        List<DurableRecord> loaded = store.load();
        assertEquals(2, loaded.size());
        assertEquals(3, loaded.get(1).term);
      }
    }
  }

  @Test
  public void completeCorruptRecordFailsClosedAndIsNotTruncated() throws Exception {
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.append(term(4));
      store.append(term(5));
    }
    byte[] valid = Files.readAllBytes(journal());
    for (int offset : new int[] {0, 4, 8, 12, valid.length - 1}) {
      byte[] corrupt = valid.clone();
      corrupt[offset] ^= (byte) 0x80;
      Files.write(journal(), corrupt);
      try (TRaftFileStore store = new TRaftFileStore(directory)) {
        try {
          store.load();
          fail("Complete corruption at " + offset + " must fail startup");
        } catch (IOException expected) {
          assertArrayEquals(corrupt, Files.readAllBytes(journal()));
        }
      }
    }
  }

  @Test
  public void payloadLargerThanCacheReloadsFromDiskBeforeAndAfterRestart() throws Exception {
    byte[] payload = new byte[4096];
    new Random(931).nextBytes(payload);
    Entry entry = new Entry(new EntryId(5, 8), 2, 17, 3, Kind.DATA, payload);
    try (TRaftFileStore store = new TRaftFileStore(directory, 8)) {
      store.load();
      store.append(new DurableRecord(RecordType.ENTRY, 5, -1, entry, null, null, -1, null));
      Entry resident = store.residentEntry(entry);
      assertEquals(4096, resident.payloadSize());
      assertEquals(0, store.cacheBytes());
      byte[] first = resident.payload();
      assertArrayEquals(payload, first);
      first[0] ^= 1;
      assertArrayEquals(
          "Payload must remain immutable after caller mutation", payload, resident.payload());
      assertEquals(2, store.payloadLoads());
      assertEquals(0, store.cacheBytes());
    }
    try (TRaftFileStore store = new TRaftFileStore(directory, 8)) {
      Entry recovered = store.residentEntry(store.load().get(0).entry);
      assertArrayEquals(payload, recovered.payload());
      assertEquals(1, store.payloadLoads());
      assertEquals(0, store.cacheBytes());
    }
  }

  @Test
  public void emptyDurablePayloadsShareAConstantCacheWithoutJournalRereads() throws Exception {
    Entry noop = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      store.append(new DurableRecord(RecordType.ENTRY, 1, -1, noop, null, null, -1, null));
      long reads = ResearchMetrics.value("traft.log_read_count");
      assertArrayEquals(new byte[0], store.residentEntry(noop).payload());
      assertEquals(reads, ResearchMetrics.value("traft.log_read_count"));
      assertEquals(0, store.cacheBytes());
    }
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      Entry recovered = store.load().get(0).entry;
      long reads = ResearchMetrics.value("traft.log_read_count");
      for (int i = 0; i < 3; i++) {
        assertArrayEquals(new byte[0], store.residentEntry(recovered).payload());
      }
      assertEquals(reads, ResearchMetrics.value("traft.log_read_count"));
      assertEquals(0, store.payloadLoads());
      assertEquals(0, store.cacheBytes());
    }
  }

  @Test
  public void uncacheableAppendDoesNotLoadPayloadAgainForDiscardedCacheCopy() throws Exception {
    byte[] payload = new byte[4096];
    new Random(214).nextBytes(payload);
    for (long budget : new long[] {0, 8}) {
      File path = new File(directory, "budget-" + budget);
      Entry metadata = Entry.metadata(new Entry(new EntryId(1, 1), 0, 12, 0, Kind.DATA, payload));
      AtomicInteger loads = new AtomicInteger();
      Entry entry =
          Entry.lazy(
              metadata,
              () -> {
                loads.incrementAndGet();
                return payload;
              });
      try (TRaftFileStore store = new TRaftFileStore(path, budget)) {
        store.load();
        store.append(new DurableRecord(RecordType.ENTRY, 1, -1, entry, null, null, -1, null));
        assertEquals("Only the durable encoding needs the payload", 1, loads.get());
        assertEquals(0, store.cacheBytes());
        // A later metadata reference must neither become a payload location nor load bytes.
        store.append(new DurableRecord(RecordType.INSTALL, 1, -1, metadata, null, null, -1, null));
        assertEquals(1, loads.get());
        assertArrayEquals(payload, store.residentEntry(entry).payload());
      }
      try (TRaftFileStore store = new TRaftFileStore(path, budget)) {
        assertEquals(2, store.load().size());
        assertArrayEquals(payload, store.residentEntry(metadata).payload());
      }
    }
  }

  @Test
  public void boundedCacheEvictsAndLazyReadsVerifyTheDurableChecksum() throws Exception {
    Entry a = new Entry(new EntryId(1, 11), 0, 0, 0, Kind.DATA, new byte[16]);
    Entry b = new Entry(new EntryId(1, 12), 0, 1, 0, Kind.DATA, new byte[16]);
    try (TRaftFileStore store = new TRaftFileStore(directory, 16)) {
      store.load();
      store.append(new DurableRecord(RecordType.ENTRY, 1, -1, a, null, null, -1, null));
      Entry resident = store.residentEntry(a);
      store.append(new DurableRecord(RecordType.ENTRY, 1, -1, b, null, null, -1, null));
      assertEquals(16, store.cacheBytes());
      assertArrayEquals(a.payload(), resident.payload());
      assertEquals(1, store.payloadLoads());
      store.residentEntry(b).payload(); // evict a again
      byte[] journal = Files.readAllBytes(journal());
      journal[12] ^= 1;
      Files.write(journal(), journal);
      try {
        resident.payload();
        fail("A lazy payload must not bypass durable checksum validation");
      } catch (UncheckedIOException expected) {
        /* checksum failure */
      }
    }
  }

  @Test
  public void conflictingIdentityCannotChangePreviouslyResidentEntry() throws Exception {
    Entry a = new Entry(new EntryId(2, 3), 1, 7, 0, Kind.DATA, new byte[] {1});
    Entry b = new Entry(new EntryId(2, 3), 1, 8, 0, Kind.DATA, new byte[] {2, 2});
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      store.append(new DurableRecord(RecordType.ENTRY, 2, -1, a, null, null, -1, null));
      Entry resident = store.residentEntry(a);
      try {
        store.append(new DurableRecord(RecordType.ENTRY, 2, -1, b, null, null, -1, null));
        fail("An immutable entry identity cannot acquire different metadata");
      } catch (IOException expected) {
        /* reject before publication */
      }
      assertArrayEquals(a.payload(), resident.payload());
    }
  }

  @Test
  public void retainedMetadataValidatesWithoutPayloadRereadsEvenWithZeroCache() throws Exception {
    Entry entry = new Entry(new EntryId(2, 1), 0, 7, 0, Kind.DATA, new byte[16]);
    Proposal proposal = new Proposal(null, Collections.emptyList(), Entry.metadata(entry), false);
    DurableRecord retained =
        new DurableRecord(RecordType.RETAIN_PROPOSAL, 2, -1, null, proposal, null, -1, null);
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      long emptySize = Files.size(journal());
      try {
        store.append(retained);
        fail("Metadata cannot become a payload location before its durable entry exists");
      } catch (IOException expected) {
        assertEquals(emptySize, Files.size(journal()));
      }
      store.append(new DurableRecord(RecordType.ENTRY, 2, -1, entry, null, null, -1, null));
      long reads = ResearchMetrics.value("traft.log_read_count");
      store.append(retained);
      assertEquals(reads, ResearchMetrics.value("traft.log_read_count"));
      assertEquals(0, store.cacheBytes());
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
      assertEquals(1, store.payloadLoads());
    }
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      long reads = ResearchMetrics.value("traft.log_read_count");
      store.append(retained);
      assertEquals(reads, ResearchMetrics.value("traft.log_read_count"));
      long size = Files.size(journal());
      Entry changed = new Entry(entry.id, 0, 8, 0, Kind.DATA, new byte[16]);
      try {
        store.append(new DurableRecord(RecordType.ENTRY, 2, -1, changed, null, null, -1, null));
        fail("Recovered immutable metadata must still reject a changed timestamp");
      } catch (IOException expected) {
        assertEquals(size, Files.size(journal()));
      }
      assertArrayEquals(entry.payload(), store.residentEntry(entry).payload());
    }
  }

  @Test
  public void deferredMarkerIsForcedByTheNextAppendOrByClose() throws Exception {
    Entry entry = new Entry(new EntryId(7, 14), 2, -10, 3, Kind.DATA, new byte[] {1, 2, 3});
    DurableRecord marker =
        new DurableRecord(RecordType.INSTALL, 7, -1, null, null, entry.id, -1, null);
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.append(term(7));
      long forces = ResearchMetrics.value("traft.log_force_count");
      store.appendDeferred(marker);
      assertTrue(store.hasPendingForce());
      assertEquals(
          "A deferred marker costs no force of its own",
          forces,
          ResearchMetrics.value("traft.log_force_count"));
      store.append(new DurableRecord(RecordType.ENTRY, 7, -1, entry, null, null, -1, null));
      assertFalse("The next forced record covers the marker", store.hasPendingForce());
      assertEquals(forces + 1, ResearchMetrics.value("traft.log_force_count"));
      store.appendDeferred(marker);
      assertTrue(store.hasPendingForce());
      store.forcePending();
      assertFalse("The runtime's bounded flush forces a pending marker", store.hasPendingForce());
      assertEquals(forces + 2, ResearchMetrics.value("traft.log_force_count"));
      store.forcePending();
      assertEquals(
          "Nothing pending, nothing forced",
          forces + 2,
          ResearchMetrics.value("traft.log_force_count"));
      store.appendDeferred(marker);
      store.appendBatch(
          Arrays.asList(
              new DurableRecord(RecordType.TERM_VOTE, 8, -1, null, null, null, -1, null),
              new DurableRecord(RecordType.TERM_VOTE, 9, -1, null, null, null, -1, null)));
      assertFalse("A forced batch covers the marker too", store.hasPendingForce());
      assertEquals(forces + 3, ResearchMetrics.value("traft.log_force_count"));
      store.appendDeferred(marker);
      assertTrue(store.hasPendingForce());
      long beforeClose = ResearchMetrics.value("traft.log_force_count");
      store.close();
      assertEquals(
          "close forces the trailing deferred marker",
          beforeClose + 1,
          ResearchMetrics.value("traft.log_force_count"));
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      List<DurableRecord> records = store.load();
      // TERM_VOTE, marker, ENTRY, marker, marker, TERM_VOTE(8), TERM_VOTE(9), marker
      assertEquals(8, records.size());
      assertEquals(RecordType.TERM_VOTE, records.get(0).type);
      assertEquals(RecordType.INSTALL, records.get(1).type);
      assertEquals(entry.id, records.get(1).reference);
      assertEquals(RecordType.ENTRY, records.get(2).type);
      assertEquals(RecordType.INSTALL, records.get(3).type);
      assertEquals(RecordType.INSTALL, records.get(4).type);
      assertEquals(8, records.get(5).term);
      assertEquals(9, records.get(6).term);
      assertEquals(RecordType.INSTALL, records.get(7).type);
      assertFalse(store.hasPendingForce());
      long forces = ResearchMetrics.value("traft.log_force_count");
      store.close();
      assertEquals(
          "close without a pending marker forces nothing",
          forces,
          ResearchMetrics.value("traft.log_force_count"));
    }
  }

  @Test
  public void asyncDurabilityForcesOnlyVotesAndTheTimedFlush() throws Exception {
    Entry first = new Entry(new EntryId(7, 1), 0, 10, 0, Kind.DATA, new byte[] {1});
    Entry second = new Entry(new EntryId(7, 2), 0, 11, 0, Kind.DATA, new byte[] {2});
    Entry third = new Entry(new EntryId(7, 3), 0, 12, 0, Kind.DATA, new byte[] {3});
    Entry fourth = new Entry(new EntryId(7, 4), 0, 13, 0, Kind.DATA, new byte[] {4});
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.setAsyncForceInterval(60_000);
      long forces = ResearchMetrics.value("traft.log_force_count");
      store.append(term(7));
      assertEquals("A vote is forced in every mode", forces + 1, value());
      assertFalse(store.hasPendingForce());
      store.append(entryRecord(first));
      store.appendBatch(Arrays.asList(entryRecord(second), entryRecord(third)));
      assertEquals("Data appends only write", forces + 1, value());
      assertTrue(store.hasPendingForce());
      store.forcePending();
      assertEquals("The timed flush waits for its interval", forces + 1, value());
      store.forceNow();
      assertEquals(forces + 2, value());
      assertFalse(store.hasPendingForce());
      store.append(entryRecord(fourth));
      store.appendBatch(
          Arrays.asList(
              new DurableRecord(RecordType.TERM_VOTE, 8, -1, null, null, null, -1, null),
              new DurableRecord(RecordType.TERM_VOTE, 9, -1, null, null, null, -1, null)));
      assertEquals("A batch with a vote is forced, covering the data", forces + 3, value());
      assertFalse(store.hasPendingForce());
      store.setAsyncForceInterval(0);
      store.append(term(10));
      store.appendDeferred(
          new DurableRecord(RecordType.INSTALL, 10, -1, null, null, first.id, -1, null));
      store.forcePending();
      assertEquals("An elapsed interval forces on the next flush", forces + 5, value());
      store.setAsyncForceInterval(-1);
      store.append(entryRecord(new Entry(new EntryId(10, 5), 0, 14, 0, Kind.DATA, new byte[] {5})));
      assertEquals("Negative restores a force per append", forces + 6, value());
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      List<DurableRecord> records = store.load();
      assertEquals(10, records.size());
      assertEquals(first, records.get(1).entry);
      assertEquals(third, records.get(3).entry);
      assertEquals(fourth, records.get(4).entry);
    }
  }

  private static long value() {
    return ResearchMetrics.value("traft.log_force_count");
  }

  private static DurableRecord entryRecord(Entry entry) {
    return new DurableRecord(RecordType.ENTRY, 1, -1, entry, null, null, -1, null);
  }

  private static DurableRecord compactRecord(long frontier, EntryId anchor) {
    return new DurableRecord(RecordType.COMPACT, 1, -1, null, null, anchor, frontier, null);
  }

  private static void assertNotResident(TRaftFileStore store, Entry entry) {
    try {
      store.residentEntry(entry);
      fail("A compacted partition keeps no payload location: " + entry.id);
    } catch (IllegalStateException expected) {
      /* pruned */
    }
  }

  @Test
  public void handleMadeBeforeCompactionStillReadsItsPayload() throws Exception {
    Entry old = new Entry(new EntryId(1, 1), 0, 5, 0, Kind.DATA, new byte[] {1, 2, 3});
    Entry kept = new Entry(new EntryId(1, 2), 1, 6, 1, Kind.DATA, new byte[] {4, 5});
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      store.append(entryRecord(old));
      store.append(entryRecord(kept));
      Entry handle = store.residentEntry(old);
      assertEquals(2, store.indexedEntries());
      store.appendDeferred(compactRecord(0, kept.id));
      assertEquals("The COMPACT record prunes partition 0 at once", 1, store.indexedEntries());
      assertNotResident(store, old);
      assertArrayEquals("The handle keeps its own offset", old.payload(), handle.payload());
      assertArrayEquals(kept.payload(), store.residentEntry(kept).payload());
    }
  }

  @Test
  public void loadingACompactedJournalPrunesTheIndexAndMetadataCannotNameAPrunedEntry()
      throws Exception {
    Entry old = new Entry(new EntryId(1, 1), 0, 5, 0, Kind.DATA, new byte[] {1});
    Entry kept = new Entry(new EntryId(1, 2), 1, 6, 1, Kind.DATA, new byte[] {2});
    Proposal oldManifest = new Proposal(null, Collections.emptyList(), Entry.metadata(old), false);
    DurableRecord retainOld =
        new DurableRecord(RecordType.RETAIN_PROPOSAL, 1, -1, null, oldManifest, null, -1, null);
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      store.load();
      store.append(entryRecord(old));
      store.append(retainOld);
      store.append(entryRecord(kept));
      store.append(compactRecord(0, kept.id));
      long size = Files.size(journal());
      try {
        store.append(retainOld);
        fail("A metadata-only RETAIN naming a compacted first entry must fail closed");
      } catch (IOException expected) {
        assertEquals(size, Files.size(journal()));
      }
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      List<DurableRecord> records = store.load();
      assertEquals(4, records.size());
      assertEquals(RecordType.COMPACT, records.get(3).type);
      assertEquals(0, records.get(3).frontier);
      assertEquals(kept.id, records.get(3).reference);
      assertEquals(1, store.indexedEntries());
      assertNotResident(store, old);
      assertArrayEquals(kept.payload(), store.residentEntry(kept).payload());
      try {
        store.append(retainOld);
        fail("The pruned index survives a restart");
      } catch (IOException expected) {
        /* fail closed */
      }
    }
  }

  @Test
  public void iterationReindexesInStepSoAReplayCanStillMakeHandles() throws Exception {
    Entry old = new Entry(new EntryId(1, 1), 0, 5, 0, Kind.DATA, new byte[] {7, 7});
    Entry kept = new Entry(new EntryId(1, 2), 1, 6, 1, Kind.DATA, new byte[] {8});
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      store.load();
      store.append(term(1));
      store.append(entryRecord(old));
      store.append(entryRecord(kept));
      store.append(compactRecord(0, kept.id));
    }
    try (TRaftFileStore store = new TRaftFileStore(directory, 0)) {
      List<DurableRecord> records = store.load();
      assertNotResident(store, old);
      for (int round = 0; round < 2; round++) {
        Iterator<DurableRecord> replay = records.iterator();
        assertEquals(RecordType.TERM_VOTE, replay.next().type);
        assertEquals(old, replay.next().entry);
        // The replaying reader can make a handle until the COMPACT record goes past.
        Entry handle = store.residentEntry(old);
        assertEquals(kept, replay.next().entry);
        assertEquals(2, store.indexedEntries());
        assertEquals(RecordType.COMPACT, replay.next().type);
        assertFalse(replay.hasNext());
        assertEquals(1, store.indexedEntries());
        assertNotResident(store, old);
        assertArrayEquals(old.payload(), handle.payload());
      }
      List<DurableRecord> streamed = new ArrayList<>(records);
      assertEquals(records.size(), streamed.size());
      for (int i = 0; i < records.size(); i++) {
        assertEquals(streamed.get(i).type, records.get(i).type);
        assertEquals(streamed.get(i).entry, records.get(i).entry);
      }
      try {
        records.get(records.size());
        fail("get() is bounded by the loaded journal");
      } catch (IndexOutOfBoundsException expected) {
        /* bounded */
      }
    }
  }

  private static TRaftEngine follower(TRaftFileStore store) throws IOException {
    return new TRaftEngine(
        2,
        new HashSet<>(Arrays.asList(1, 2, 3)),
        16,
        0,
        store,
        new Application() {
          @Override
          public void apply(Entry entry) {}

          @Override
          public boolean requiresDurableCommitRecords() {
            return false;
          }

          @Override
          public boolean requiresDurableAppliedRecords() {
            return false;
          }
        });
  }

  private static Message frame(
      MessageType type, Entry entry, Proposal proposal, EntryId reference) {
    return new Message(type, 1, 2, 1, entry, proposal, reference, null, null, -1, null);
  }

  /** The replica answers a FETCH with the payload it holds. */
  private static void assertServes(TRaftEngine engine, Entry entry) throws IOException {
    engine.drainMessages();
    engine.receive(frame(MessageType.FETCH_ENTRY, null, null, entry.id));
    List<Message> answers = engine.drainMessages();
    assertEquals(1, answers.size());
    assertEquals(MessageType.TRANSFER_ENTRY, answers.get(0).type);
    assertArrayEquals(entry.payload(), answers.get(0).entry.payload());
  }

  /** Staging on the real journal: repeats, restart, a torn tail and the opening's confirmation. */
  @Test
  public void stagedEntriesReplayAsBytesAndInstallWithTheirOpeningsConfirmation() throws Exception {
    Entry root = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal p0 = new Proposal(null, Collections.emptyList(), root, true);
    Entry first = new Entry(new EntryId(1, 2), 1, 5, 1, Kind.NOOP, new byte[0]);
    Proposal p1 = new Proposal(root.id, Collections.singletonList(root.id), first, false);
    Entry e3 = new Entry(new EntryId(1, 3), 1, 10, 1, Kind.DATA, new byte[] {3, 3, 3});
    Entry e4 = new Entry(new EntryId(1, 4), 1, 20, 1, Kind.DATA, new byte[] {4, 4});
    Entry e5 = new Entry(new EntryId(1, 5), 1, 30, 1, Kind.DATA, new byte[] {5});
    List<Message> run =
        Arrays.asList(
            frame(MessageType.ENTRY, e3, null, p1.id()),
            frame(MessageType.ENTRY, e3, null, p1.id()),
            frame(MessageType.ENTRY, e4, null, p1.id()));
    long tail;
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      TRaftEngine f = follower(store);
      f.receive(frame(MessageType.CONFIRM, null, p0, null));
      assertEquals(0, f.currentPartition());
      assertTrue("The install marker is deferred", store.hasPendingForce());
      long records = store.forcedRecords();
      assertFalse(f.receiveEntries(run));
      assertEquals("One record per identity", records + 2, store.forcedRecords());
      assertFalse("The staging force covers the deferred marker", store.hasPendingForce());
      for (int wave = 0; wave < 10; wave++) {
        assertFalse(f.receiveEntries(run));
      }
      assertEquals("Repeated waves write nothing", records + 2, store.forcedRecords());
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      TRaftEngine f = follower(store);
      assertEquals(0, f.currentPartition());
      assertEquals(1, f.installedEntries().size());
      assertServes(f, e3);
      assertServes(f, e4);
      tail = journal().toFile().length();
      f.receive(frame(MessageType.ENTRY, e5, null, p1.id()));
      assertTrue(journal().toFile().length() > tail);
    }
    try (RandomAccessFile file = new RandomAccessFile(journal().toFile(), "rw")) {
      file.setLength(file.length() - 1);
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      TRaftEngine f = follower(store);
      assertEquals("The torn staged record is gone", tail, journal().toFile().length());
      f.receive(frame(MessageType.FETCH_ENTRY, null, null, e5.id));
      assertTrue(f.drainMessages().isEmpty());
      // The opening arrives and installs the staged run with it.
      f.receive(frame(MessageType.CONFIRM, null, p1, null));
      assertEquals(1, f.currentPartition());
      assertEquals(3, f.currentRank().count);
      f.drainMessages();
      // A retried run writes nothing and is acknowledged in one frame.
      long length = journal().toFile().length();
      assertTrue(f.receiveEntries(Arrays.asList(run.get(0), run.get(2))));
      assertEquals(length, journal().toFile().length());
      assertEquals(1, f.drainMessages().size());
      assertEquals(3, f.currentRank().count);
    }
    try (TRaftFileStore store = new TRaftFileStore(directory)) {
      TRaftEngine f = follower(store);
      assertEquals(3, f.currentRank().count);
      assertServes(f, e3);
      assertServes(f, e4);
    }
  }
}
