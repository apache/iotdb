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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

public class LogicalRepairSessionJournalTest {

  @Test
  public void stagedBatchesShouldPersistAcrossJournalReload() throws Exception {
    Path journalDir = Files.createTempDirectory("logical-repair-journal");
    try {
      LogicalRepairSessionJournal journal = new LogicalRepairSessionJournal(journalDir);
      String repairEpoch = "1:7:1000:2000:2000:1-1_2_3";

      journal.stageBatch(
          "DataRegion-7",
          7L,
          repairEpoch,
          "session-a",
          "LIVE",
          "leaf:1:0",
          2,
          "INSERT_ROWS",
          ByteBuffer.wrap(new byte[] {2}));
      journal.stageBatch(
          "DataRegion-7",
          7L,
          repairEpoch,
          "session-a",
          "LIVE",
          "leaf:1:0",
          0,
          "RESET_LEAF",
          ByteBuffer.allocate(0));
      journal.stageBatch(
          "DataRegion-7",
          7L,
          repairEpoch,
          "session-a",
          "LIVE",
          "leaf:1:0",
          1,
          "INSERT_ROWS",
          ByteBuffer.wrap(new byte[] {1}));

      LogicalRepairSessionJournal recovered = new LogicalRepairSessionJournal(journalDir);
      List<LogicalRepairSessionJournal.StagedBatch> recoveredBatches =
          recovered.loadStagedBatches("DataRegion-7", 7L, repairEpoch, "session-a");

      Assert.assertEquals(3, recoveredBatches.size());
      Assert.assertEquals(0, recoveredBatches.get(0).getSeqNo());
      Assert.assertEquals("RESET_LEAF", recoveredBatches.get(0).getBatchKind());
      Assert.assertEquals(1, recoveredBatches.get(1).getSeqNo());
      Assert.assertEquals(2, recoveredBatches.get(2).getSeqNo());
      Assert.assertArrayEquals(
          new byte[] {1}, toByteArray(recoveredBatches.get(1).duplicatePayload()));
      Assert.assertArrayEquals(
          new byte[] {2}, toByteArray(recoveredBatches.get(2).duplicatePayload()));
    } finally {
      deleteRecursively(journalDir);
    }
  }

  @Test
  public void duplicateSeqNoShouldBeDeduplicatedDurably() throws Exception {
    Path journalDir = Files.createTempDirectory("logical-repair-journal");
    try {
      LogicalRepairSessionJournal journal = new LogicalRepairSessionJournal(journalDir);
      String repairEpoch = "1:8:1000:2000:2000:1-1_2_3";

      journal.stageBatch(
          "DataRegion-8",
          8L,
          repairEpoch,
          "session-b",
          "TOMBSTONE",
          "leaf:2:0",
          0,
          "DELETE_DATA",
          ByteBuffer.wrap(new byte[] {1, 2, 3}));
      journal.stageBatch(
          "DataRegion-8",
          8L,
          repairEpoch,
          "session-b",
          "TOMBSTONE",
          "leaf:2:0",
          0,
          "DELETE_DATA",
          ByteBuffer.wrap(new byte[] {9, 9, 9}));

      LogicalRepairSessionJournal recovered = new LogicalRepairSessionJournal(journalDir);
      List<LogicalRepairSessionJournal.StagedBatch> recoveredBatches =
          recovered.loadStagedBatches("DataRegion-8", 8L, repairEpoch, "session-b");

      Assert.assertEquals(1, recoveredBatches.size());
      Assert.assertArrayEquals(
          new byte[] {1, 2, 3}, toByteArray(recoveredBatches.get(0).duplicatePayload()));
    } finally {
      deleteRecursively(journalDir);
    }
  }

  @Test
  public void completeShouldDeletePersistedSession() throws Exception {
    Path journalDir = Files.createTempDirectory("logical-repair-journal");
    try {
      LogicalRepairSessionJournal journal = new LogicalRepairSessionJournal(journalDir);
      String repairEpoch = "1:9:1000:2000:2000:1-1_2_3";

      journal.stageBatch(
          "DataRegion-9",
          9L,
          repairEpoch,
          "session-c",
          "LIVE",
          "leaf:3:0",
          0,
          "RESET_LEAF",
          ByteBuffer.allocate(0));
      journal.completeSession("session-c");

      LogicalRepairSessionJournal recovered = new LogicalRepairSessionJournal(journalDir);
      Assert.assertTrue(
          recovered.loadStagedBatches("DataRegion-9", 9L, repairEpoch, "session-c").isEmpty());
    } finally {
      deleteRecursively(journalDir);
    }
  }

  @Test
  public void defaultJournalShouldResolveSystemDirLazily() throws Exception {
    String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    Path initialSystemDir = Files.createTempDirectory("logical-repair-journal-initial");
    Path effectiveSystemDir = Files.createTempDirectory("logical-repair-journal-effective");
    try {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(initialSystemDir.toString());
      LogicalRepairSessionJournal journal = new LogicalRepairSessionJournal();

      IoTDBDescriptor.getInstance().getConfig().setSystemDir(effectiveSystemDir.toString());
      String repairEpoch = "1:10:1000:2000:2000:1-1_2_3";
      journal.stageBatch(
          "DataRegion-10",
          10L,
          repairEpoch,
          "session-d",
          "LIVE",
          "leaf:4:0",
          0,
          "RESET_LEAF",
          ByteBuffer.allocate(0));

      Assert.assertTrue(
          Files.exists(
              effectiveSystemDir
                  .resolve("consistency-repair")
                  .resolve("sessions")
                  .resolve("session-d.session")));
      Assert.assertFalse(
          Files.exists(
              initialSystemDir
                  .resolve("consistency-repair")
                  .resolve("sessions")
                  .resolve("session-d.session")));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
      deleteRecursively(initialSystemDir);
      deleteRecursively(effectiveSystemDir);
    }
  }

  private byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  private void deleteRecursively(Path dir) throws Exception {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
      stream.sorted(Comparator.reverseOrder()).forEach(this::deleteSilently);
    }
  }

  private void deleteSilently(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (Exception ignored) {
      // best effort for test cleanup
    }
  }
}
