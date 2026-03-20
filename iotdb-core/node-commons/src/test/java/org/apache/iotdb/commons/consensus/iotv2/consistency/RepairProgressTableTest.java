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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RepairProgressTableTest {

  @Test
  public void verifiedPartitionWithSameSnapshotShouldBePrunedFromNextCheck() {
    RepairProgressTable table = new RepairProgressTable("DataRegion-1");

    table.markVerified(100L, 1000L, 2000L, 3000L, 3000L, RepairProgressTable.SnapshotState.READY);

    RepairProgressTable.PartitionProgress progress = table.getPartition(100L);
    Assert.assertNotNull(progress);
    Assert.assertFalse(progress.shouldCheck(3000L, 3000L, RepairProgressTable.SnapshotState.READY));
    Assert.assertTrue(progress.shouldCheck(3001L, 3001L, RepairProgressTable.SnapshotState.READY));
  }

  @Test
  public void replicaObservationTokenChangeShouldForceRecheck() {
    RepairProgressTable table = new RepairProgressTable("DataRegion-1");

    table.markVerified(
        100L,
        1000L,
        2000L,
        3000L,
        3000L,
        RepairProgressTable.SnapshotState.READY,
        "leader=4|follower=3:snapshot=3000");

    RepairProgressTable.PartitionProgress progress = table.getPartition(100L);
    Assert.assertNotNull(progress);
    Assert.assertFalse(
        progress.shouldCheck(
            3000L,
            3000L,
            RepairProgressTable.SnapshotState.READY,
            "leader=4|follower=3:snapshot=3000"));
    Assert.assertTrue(
        progress.shouldCheck(
            3000L,
            3000L,
            RepairProgressTable.SnapshotState.READY,
            "leader=4|follower=3:snapshot=0"));
  }

  @Test
  public void dirtyTransitionShouldClearVerifiedRepairStateAndScope() {
    RepairProgressTable table = new RepairProgressTable("DataRegion-1");

    table.markMismatch(
        100L,
        1000L,
        2000L,
        3000L,
        3000L,
        RepairProgressTable.SnapshotState.READY,
        "LIVE@leaf:1:0",
        1,
        "leader:1:2000:3000:3000");
    table.markRepairSucceeded(
        100L,
        1100L,
        2100L,
        3001L,
        3001L,
        RepairProgressTable.SnapshotState.READY,
        "leader:1:2000:3000:3000");
    table.markDirty(100L);

    RepairProgressTable.PartitionProgress progress = table.getPartition(100L);
    Assert.assertEquals(RepairProgressTable.CheckState.DIRTY, progress.getCheckState());
    Assert.assertEquals(RepairProgressTable.RepairState.IDLE, progress.getRepairState());
    Assert.assertNull(progress.getMismatchScopeRef());
  }

  @Test
  public void serDeShouldPreserveCheckAndRepairState() throws Exception {
    RepairProgressTable table = new RepairProgressTable("DataRegion-7");
    table.markMismatch(
        10L,
        111L,
        222L,
        333L,
        333L,
        RepairProgressTable.SnapshotState.READY,
        "TOMBSTONE@leaf:7:1",
        2,
        "leader:7:222:333:333");
    table.markRepairRunning(10L, "leader:7:222:333:333");
    table.markRepairFailed(10L, "leader:7:222:333:333", "ERR_CODE", "ERR_MSG");

    RepairProgressTable restored;
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      table.serialize(outputStream);
      restored =
          RepairProgressTable.deserialize(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    Assert.assertEquals(table, restored);
  }
}
