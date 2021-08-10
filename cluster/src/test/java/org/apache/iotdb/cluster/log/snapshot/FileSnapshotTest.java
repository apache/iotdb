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

import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.partition.slot.SlotManager.SlotStatus;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileSnapshotTest extends DataSnapshotTest {

  @Test
  public void testSerialize() throws IOException, WriteProcessException {
    FileSnapshot snapshot = new FileSnapshot();
    List<RemoteTsFileResource> dataFiles = new ArrayList<>();
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
    for (int i = 0; i < 10; i++) {
      RemoteTsFileResource dataFile =
          new RemoteTsFileResource(tsFileResources.get(i), TestUtils.getNode(i));
      dataFiles.add(dataFile);
      snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
      timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
    }
    assertEquals(dataFiles, snapshot.getDataFiles());
    snapshot.setTimeseriesSchemas(timeseriesSchemas);

    assertEquals("FileSnapshot{10 files, 10 series, index-term: 0-0}", snapshot.toString());

    ByteBuffer buffer = snapshot.serialize();

    FileSnapshot deserialized = new FileSnapshot();
    deserialized.deserialize(buffer);
    assertEquals(snapshot, deserialized);
  }

  @Test
  public void testInstallSingle()
      throws IOException, SnapshotInstallationException, IllegalPathException,
          StorageEngineException, WriteProcessException {
    testInstallSingle(false);
  }

  @Test
  public void testInstallSingleWithFailure()
      throws IOException, SnapshotInstallationException, IllegalPathException,
          StorageEngineException, WriteProcessException {
    testInstallSingle(true);
  }

  public void testInstallSingle(boolean addNetFailure)
      throws IOException, SnapshotInstallationException, IllegalPathException,
          StorageEngineException, WriteProcessException {
    this.addNetFailure = addNetFailure;

    FileSnapshot snapshot = new FileSnapshot();
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
    for (int i = 0; i < 10; i++) {
      snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
      timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
    }
    snapshot.setTimeseriesSchemas(timeseriesSchemas);

    SnapshotInstaller<FileSnapshot> defaultInstaller =
        snapshot.getDefaultInstaller(dataGroupMember);
    dataGroupMember.getSlotManager().setToPulling(0, TestUtils.getNode(0));
    defaultInstaller.install(snapshot, 0, false);
    // after installation, the slot should be available again
    assertEquals(SlotStatus.NULL, dataGroupMember.getSlotManager().getStatus(0));

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

  @Test
  public void testInstallSync()
      throws IOException, SnapshotInstallationException, IllegalPathException,
          StorageEngineException, WriteProcessException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);

    try {
      FileSnapshot snapshot = new FileSnapshot();
      List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
      List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
      for (int i = 0; i < 10; i++) {
        snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
        timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
      }
      snapshot.setTimeseriesSchemas(timeseriesSchemas);

      SnapshotInstaller<FileSnapshot> defaultInstaller =
          snapshot.getDefaultInstaller(dataGroupMember);
      dataGroupMember.getSlotManager().setToPulling(0, TestUtils.getNode(0));
      defaultInstaller.install(snapshot, 0, false);
      // after installation, the slot should be available again
      assertEquals(SlotStatus.NULL, dataGroupMember.getSlotManager().getStatus(0));

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
    } finally {
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testInstallWithModFile()
      throws IOException, SnapshotInstallationException, IllegalPathException,
          StorageEngineException, WriteProcessException {
    FileSnapshot snapshot = new FileSnapshot();
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
    for (int i = 0; i < 10; i++) {
      ModificationFile modFile = tsFileResources.get(i).getModFile();
      modFile.write(new Deletion(new PartialPath(TestUtils.getTestSg(0)), 0, 10));
      modFile.close();
      snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
      timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
    }
    snapshot.setTimeseriesSchemas(timeseriesSchemas);

    ByteBuffer buffer = snapshot.serialize();
    FileSnapshot fileSnapshot = new FileSnapshot();
    fileSnapshot.deserialize(buffer);

    SnapshotInstaller<FileSnapshot> defaultInstaller =
        fileSnapshot.getDefaultInstaller(dataGroupMember);
    dataGroupMember.getSlotManager().setToPulling(0, TestUtils.getNode(0));
    defaultInstaller.install(fileSnapshot, 0, false);
    // after installation, the slot should be available again
    assertEquals(SlotStatus.NULL, dataGroupMember.getSlotManager().getStatus(0));

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
      ModificationFile modFile = loadedFiles.get(i).getModFile();
      assertTrue(modFile.exists());

      Deletion deletion = new Deletion(new PartialPath(TestUtils.getTestSg(0)), 0, 10);
      assertTrue(modFile.getModifications().contains(deletion));
      assertEquals(1, modFile.getModifications().size());
      modFile.close();
    }
    assertEquals(0, processor.getUnSequenceFileList().size());
  }

  @Test
  public void testInstallMultiple()
      throws IOException, WriteProcessException, SnapshotInstallationException,
          IllegalPathException, StorageEngineException {
    Map<Integer, FileSnapshot> snapshotMap = new HashMap<>();
    for (int j = 0; j < 10; j++) {
      FileSnapshot snapshot = new FileSnapshot();
      List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
      List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(j, 10, 10, 10, true);
      for (int i = 0; i < 10; i++) {
        snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
        timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
      }
      snapshot.setTimeseriesSchemas(timeseriesSchemas);
      snapshotMap.put(j, snapshot);
    }

    SnapshotInstaller<FileSnapshot> defaultInstaller =
        snapshotMap.get(0).getDefaultInstaller(dataGroupMember);
    defaultInstaller.install(snapshotMap, false);

    for (int j = 0; j < 10; j++) {
      StorageGroupProcessor processor =
          StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(j)));
      assertEquals(9, processor.getPartitionMaxFileVersions(0));
      List<TsFileResource> loadedFiles = processor.getSequenceFileTreeSet();
      assertEquals(10, loadedFiles.size());
      for (int i = 0; i < 9; i++) {
        assertEquals(i, loadedFiles.get(i).getMaxPlanIndex());
      }
      assertEquals(0, processor.getUnSequenceFileList().size());
    }
  }

  @Test
  public void testInstallPartial()
      throws IOException, SnapshotInstallationException, IllegalPathException,
          StorageEngineException, WriteProcessException, LoadFileException {
    // dataGroupMember already have some of the files
    FileSnapshot snapshot = new FileSnapshot();
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    List<TsFileResource> tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
    for (int i = 0; i < 10; i++) {
      snapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
      timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
    }
    for (int i = 0; i < 5; i++) {
      StorageGroupProcessor processor =
          StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(0)));
      TsFileResource resource = tsFileResources.get(i);
      String pathWithoutHardlinkSuffix =
          resource.getTsFilePath().substring(0, resource.getTsFilePath().lastIndexOf('.'));
      File fileWithoutHardlinkSuffix = new File(pathWithoutHardlinkSuffix);
      resource.getTsFile().renameTo(fileWithoutHardlinkSuffix);
      resource.setFile(fileWithoutHardlinkSuffix);
      resource.serialize();
      processor.loadNewTsFile(resource);
    }
    snapshot.setTimeseriesSchemas(timeseriesSchemas);

    SnapshotInstaller<FileSnapshot> defaultInstaller =
        snapshot.getDefaultInstaller(dataGroupMember);
    defaultInstaller.install(snapshot, 0, false);

    for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
      assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(timeseriesSchema.getFullPath())));
    }
    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(0)));
    assertEquals(10, processor.getPartitionMaxFileVersions(0));
    List<TsFileResource> loadedFiles = processor.getSequenceFileTreeSet();
    assertEquals(tsFileResources.size(), loadedFiles.size());
    for (int i = 0; i < 9; i++) {
      assertEquals(i, loadedFiles.get(i).getMaxPlanIndex());
    }
    assertEquals(1, processor.getUnSequenceFileList().size());
  }
}
