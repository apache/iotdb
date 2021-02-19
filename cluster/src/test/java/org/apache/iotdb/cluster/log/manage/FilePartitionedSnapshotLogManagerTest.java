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

package org.apache.iotdb.cluster.log.manage;

import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FilePartitionedSnapshotLogManagerTest extends IoTDBTest {

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testSnapshot() throws Exception {
    PartitionTable partitionTable = TestUtils.getPartitionTable(3);
    LogApplier applier = new TestLogApplier();
    FilePartitionedSnapshotLogManager manager =
        new FilePartitionedSnapshotLogManager(
            applier,
            partitionTable,
            TestUtils.getNode(0),
            TestUtils.getNode(0),
            new TestDataGroupMember());

    try {
      List<Log> logs = TestUtils.prepareTestLogs(10);
      manager.append(logs);
      manager.commitTo(10);
      manager.setMaxHaveAppliedCommitIndex(manager.getCommitLogIndex());

      Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds = new HashMap<>();
      // create files for sgs
      for (int i = 1; i < 4; i++) {
        PartialPath sg = new PartialPath(TestUtils.getTestSg(i));
        storageGroupPartitionIds.put(sg, null);
        for (int j = 0; j < 4; j++) {
          // closed files
          prepareData(i, j * 10, 10);
          StorageEngine.getInstance().closeStorageGroupProcessor(sg, true, true);
        }
        // un closed files
        prepareData(i, 40, 10);
      }

      FlushPlan plan = new FlushPlan(null, true, storageGroupPartitionIds);
      PlanExecutor executor = new PlanExecutor();
      executor.processNonQuery(plan);

      manager.takeSnapshot();
      PartitionedSnapshot snapshot = (PartitionedSnapshot) manager.getSnapshot();
      for (int i = 1; i < 4; i++) {
        FileSnapshot fileSnapshot =
            (FileSnapshot)
                snapshot.getSnapshot(
                    SlotPartitionTable.getSlotStrategy()
                        .calculateSlotByTime(TestUtils.getTestSg(i), 0, ClusterConstant.SLOT_NUM));
        assertEquals(10, fileSnapshot.getTimeseriesSchemas().size());
        assertEquals(5, fileSnapshot.getDataFiles().size());
      }
    } finally {
      manager.close();
    }
  }
}
