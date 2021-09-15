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

package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.partition.slot.SlotManager.SlotStatus;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PartitionedSnapshotTest extends DataSnapshotTest {

  @Test
  public void testSerialize() throws IOException, WriteProcessException {

    List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
    PartitionedSnapshot partitionedSnapshot =
        new PartitionedSnapshot(FileSnapshot.Factory.INSTANCE);
    for (int i = 0; i < 10; i++) {
      FileSnapshot snapshot = new FileSnapshot();
      snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
      snapshot.setTimeseriesSchemas(
          Collections.singletonList(TestUtils.getTestTimeSeriesSchema(0, i)));
      partitionedSnapshot.putSnapshot(i, snapshot);
    }
    partitionedSnapshot.setLastLogIndex(10);
    partitionedSnapshot.setLastLogTerm(5);

    assertEquals(
        "PartitionedSnapshot{slotSnapshots=10, lastLogIndex=10, lastLogTerm=5}",
        partitionedSnapshot.toString());

    ByteBuffer buffer = partitionedSnapshot.serialize();

    PartitionedSnapshot deserialized = new PartitionedSnapshot(FileSnapshot.Factory.INSTANCE);
    deserialized.deserialize(buffer);
    assertEquals(partitionedSnapshot, deserialized);
  }

  @Test
  public void testInstall()
      throws IOException, WriteProcessException, SnapshotInstallationException,
          IllegalPathException, StorageEngineException {
    List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
    PartitionedSnapshot snapshot = new PartitionedSnapshot(FileSnapshot.Factory.INSTANCE);
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      fileSnapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
      timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
      fileSnapshot.setTimeseriesSchemas(
          Collections.singletonList(TestUtils.getTestTimeSeriesSchema(0, i)));
      snapshot.putSnapshot(i, fileSnapshot);
    }
    snapshot.setLastLogIndex(10);
    snapshot.setLastLogTerm(5);

    SnapshotInstaller<PartitionedSnapshot> defaultInstaller =
        snapshot.getDefaultInstaller(dataGroupMember);
    for (int i = 0; i < 10; i++) {
      dataGroupMember.getSlotManager().setToPulling(i, TestUtils.getNode(0));
    }
    defaultInstaller.install(snapshot, -1, false);
    // after installation, the slot should be available again
    for (int i = 0; i < 10; i++) {
      assertEquals(SlotStatus.NULL, dataGroupMember.getSlotManager().getStatus(i));
    }

    for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
      assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(timeseriesSchema.getFullPath())));
    }
    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(0)));
    assertEquals(9, processor.getPartitionMaxFileVersions(0));
    List<TsFileResource> loadedFiles = processor.getSequenceFileTreeSet();
    assertEquals(tsFileResources.size(), loadedFiles.size());
    for (int i = 0; i < 9; i++) {
      assertEquals(i, loadedFiles.get(i).getMaxPlanIndex());
    }
    assertEquals(0, processor.getUnSequenceFileList().size());

    for (TsFileResource tsFileResource : tsFileResources) {
      // source files should be deleted after being pulled
      assertFalse(tsFileResource.getTsFile().exists());
    }
  }
}
