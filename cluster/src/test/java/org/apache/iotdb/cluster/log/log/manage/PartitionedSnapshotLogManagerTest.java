/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.log.manage;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.applier.MetaLogApplier;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.DataSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Test;

public class PartitionedSnapshotLogManagerTest extends IoTDBTest {

  @Test
  public void testSnapshot() throws QueryProcessException {
    PartitionTable partitionTable = TestUtils.getPartitionTable(3);
    LogApplier applier = new MetaLogApplier(null);
    PartitionedSnapshotLogManager manager = new PartitionedSnapshotLogManager(applier,
        partitionTable, TestUtils.getNode(0));

    for (int i = 10; i < 20; i++) {
      setStorageGroup(TestUtils.getTestSg(i));

      PhysicalPlanLog log = new PhysicalPlanLog();
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan();
      createTimeSeriesPlan.setCompressor(CompressionType.UNCOMPRESSED);
      createTimeSeriesPlan.setDataType(TSDataType.DOUBLE);
      createTimeSeriesPlan.setEncoding(TSEncoding.PLAIN);
      createTimeSeriesPlan.setProps(Collections.emptyMap());
      createTimeSeriesPlan.setPath(new Path(TestUtils.getTestSeries(i, 0)));
      log.setPlan(createTimeSeriesPlan);
      log.setPreviousLogIndex(i - 11L);
      log.setPreviousLogTerm(i - 11L);
      log.setCurrLogTerm(i - 10L);
      log.setCurrLogIndex(i - 10L);
      manager.appendLog(log);
    }
    manager.commitLog(10);

    manager.takeSnapshot();
    PartitionedSnapshot snapshot = (PartitionedSnapshot) manager.getSnapshot();
    for (int i = 10; i < 20; i++) {
      DataSimpleSnapshot simpleSnapshot =
          (DataSimpleSnapshot) snapshot.getSnapshot(PartitionUtils.calculateStorageGroupSlot(
              TestUtils.getTestSg(i), 0));
      assertEquals(1, simpleSnapshot.getTimeseriesSchemas().size());
      assertEquals(1, simpleSnapshot.getSnapshot().size());
    }
  }

  @Test
  public void testSetSnapshot() {
    PartitionTable partitionTable = TestUtils.getPartitionTable(3);
    LogApplier applier = new MetaLogApplier(null);
    PartitionedSnapshotLogManager manager = new PartitionedSnapshotLogManager(applier,
        partitionTable, TestUtils.getNode(0));

    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    SimpleSnapshot simpleSnapshot = new SimpleSnapshot(testLogs);
    manager.setSnapshot(simpleSnapshot, 0);

    PartitionedSnapshot snapshot = (PartitionedSnapshot) manager.getSnapshot();
    assertEquals(testLogs, ((SimpleSnapshot) snapshot.getSnapshot(0)).getSnapshot());
  }
}