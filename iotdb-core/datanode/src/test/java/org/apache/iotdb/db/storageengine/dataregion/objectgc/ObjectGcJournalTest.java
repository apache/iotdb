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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.NOP;

import org.apache.tsfile.read.common.TimeRange;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ObjectGcJournalTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void appendScanAndDropThenReplayFromCheckpoint() throws Exception {
    File sysDir = folder.newFolder("region-sys");
    ObjectGcJournal journal = new ObjectGcJournal(sysDir, 1024);
    TableDeletionEntry deletion =
        new TableDeletionEntry(new DeletionPredicate("tbl", new NOP()), new TimeRange(1, 10));
    Map<Long, Long> exclusive = new HashMap<>();
    exclusive.put(0L, 5L);
    Map<Long, Set<Long>> protectedVersions = new HashMap<>();
    protectedVersions.put(0L, new HashSet<>(Collections.singleton(4L)));

    journal.append(ObjectGcRecord.scan(1L, deletion, exclusive, protectedVersions));
    journal.append(ObjectGcRecord.dropTable(2L, Collections.singletonList("/tmp/object/tbl")));

    try (ObjectGcJournalReader reader = new ObjectGcJournalReader(journal)) {
      ObjectGcRecord scan = reader.next();
      assertNotNull(scan);
      assertEquals(ObjectGcRecord.TYPE_SCAN, scan.getType());
      assertEquals("tbl", scan.getDeletion().getTableName());
      assertTrue(scan.shouldDeleteVersion(0L, 3L));
      assertFalse(scan.shouldDeleteVersion(0L, 4L));
      assertFalse(scan.shouldDeleteVersion(0L, 5L));
      journal.checkpoint(scan.getSeq(), scan.getEndOffset());

      ObjectGcRecord drop = reader.next();
      assertNotNull(drop);
      assertEquals(ObjectGcRecord.TYPE_DROP_TABLE, drop.getType());
      assertEquals(1, drop.getDropTableDirs().size());
      journal.checkpoint(drop.getSeq(), drop.getEndOffset());
    }
    journal.close();

    ObjectGcJournal recovered = new ObjectGcJournal(sysDir, 1024);
    try (ObjectGcJournalReader reader = new ObjectGcJournalReader(recovered)) {
      assertNull(reader.next());
    }
    recovered.close();
  }

  @Test
  public void rollsWhenExceedingThreshold() throws Exception {
    File sysDir = folder.newFolder("region-sys-roll");
    ObjectGcJournal journal = new ObjectGcJournal(sysDir, 64);
    TableDeletionEntry deletion =
        new TableDeletionEntry(new DeletionPredicate("tbl", new NOP()), new TimeRange(0, 1));
    for (int i = 0; i < 20; i++) {
      journal.append(ObjectGcRecord.dropTable(i, Collections.singletonList("/path/" + i)));
    }
    File[] logs =
        new File(sysDir, ObjectGcJournal.JOURNAL_DIR_NAME)
            .listFiles((d, n) -> n.startsWith("gc.log."));
    assertNotNull(logs);
    assertTrue(logs.length >= 2);
    journal.close();
  }
}
