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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DataPointLocator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RepairSessionTest {

  @Test
  public void shouldApplyAndJournalRecordsAtomically() {
    List<String> journalEvents = new ArrayList<>();
    List<RepairRecord> appliedInserts = new ArrayList<>();
    List<RepairRecord> appliedDeletes = new ArrayList<>();
    RepairSession session =
        new RepairSession(
            5L,
            (sessionId, partitionId, inserts, deletes) -> {
              appliedInserts.addAll(inserts);
              appliedDeletes.addAll(deletes);
            },
            new RepairSession.RepairSessionJournal() {
              @Override
              public void append(String sessionId, RepairRecord record) {
                journalEvents.add("append:" + record.getType());
              }

              @Override
              public void markCommitted(String sessionId) {
                journalEvents.add("commit");
              }

              @Override
              public void delete(String sessionId) {
                journalEvents.add("delete");
              }
            });
    RepairRecord insert =
        RepairRecord.insert(new DataPointLocator("root.sg.d1", "s1", 100L), 1L, "v1", 100L);
    RepairRecord delete =
        RepairRecord.delete(new DataPointLocator("root.sg.d1", "s1", 101L), 2L, 101L);

    session.stage(insert);
    session.stage(delete);

    Assert.assertTrue(session.promoteAtomically());
    Assert.assertEquals(RepairSession.SessionState.COMMITTED, session.getState());
    Assert.assertEquals(Collections.singletonList(insert), appliedInserts);
    Assert.assertEquals(Collections.singletonList(delete), appliedDeletes);

    session.cleanup();

    Assert.assertEquals(
        Arrays.asList("append:INSERT", "append:DELETE", "commit", "delete"), journalEvents);
  }
}
