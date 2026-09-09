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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ObjectGcWorkerTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void signalCoalescesWorkAndAllowsResubmissionAfterDrain() throws Exception {
    File systemDir = folder.newFolder("region-system");
    try (ObjectGcJournal journal = new ObjectGcJournal(systemDir)) {
      Queue<Runnable> submittedTasks = new ConcurrentLinkedQueue<>();
      Executor executor = submittedTasks::add;
      ObjectGcWorker worker = new ObjectGcWorker("db", "1", journal, executor);

      journal.append(ObjectGcRecord.dropTable(1, Collections.emptyList()));
      worker.signal();
      worker.signal();
      assertEquals(1, submittedTasks.size());

      submittedTasks.remove().run();
      try (ObjectGcJournalReader reader = new ObjectGcJournalReader(journal)) {
        assertNull(reader.next());
      }

      journal.append(ObjectGcRecord.dropTable(2, Collections.emptyList()));
      worker.signal();
      assertEquals(1, submittedTasks.size());
      submittedTasks.remove().run();
      try (ObjectGcJournalReader reader = new ObjectGcJournalReader(journal)) {
        assertNull(reader.next());
      }
    }
  }

  @Test
  public void drainsExistingJournalAfterRecovery() throws Exception {
    File systemDir = folder.newFolder("recovered-region-system");
    try (ObjectGcJournal journal = new ObjectGcJournal(systemDir)) {
      journal.append(ObjectGcRecord.dropTable(1, Collections.emptyList()));
    }

    try (ObjectGcJournal recoveredJournal = new ObjectGcJournal(systemDir)) {
      Queue<Runnable> submittedTasks = new ConcurrentLinkedQueue<>();
      ObjectGcWorker worker = new ObjectGcWorker("db", "1", recoveredJournal, submittedTasks::add);
      worker.signal();
      assertEquals(1, submittedTasks.size());
      submittedTasks.remove().run();
      try (ObjectGcJournalReader reader = new ObjectGcJournalReader(recoveredJournal)) {
        assertNull(reader.next());
      }
    }
  }
}
