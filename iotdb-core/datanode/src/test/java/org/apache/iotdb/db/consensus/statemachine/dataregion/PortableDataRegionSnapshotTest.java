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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotLoader;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotTaker;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PortableDataRegionSnapshotTest {
  // Snapshot fixtures write only a few KiB; host free-space ratios must not change their behavior.
  @Rule
  public final ExternalResource snapshotDiskPolicy =
      new ExternalResource() {
        @Override
        protected void before() {
          JVMCommonUtils.setDiskSpaceWarningThreshold(0);
        }

        @Override
        protected void after() {
          JVMCommonUtils.setDiskSpaceWarningThreshold(
              CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold());
        }
      };

  private String[][] oldDataDirs;
  private Path root;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    oldDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    root =
        Files.createTempDirectory(new File("target").toPath(), "portable-region-").toAbsolutePath();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTierDataDirs(new String[][] {{root.resolve("data").toString()}});
    TierManager.getInstance().resetFolders();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(oldDataDirs);
    TierManager.getInstance().resetFolders();
    EnvironmentUtils.cleanEnv();
    FileUtils.recursivelyDeleteFolder(root.toString());
  }

  @Test
  public void snapshotSurvivesRelocationAndRemovalOfExternalSnapshotFiles() throws Exception {
    String database = "root.testsg";
    Path data = root.resolve("data/sequence/" + database + "/0/0/1-1-0-0.tsfile");
    Files.createDirectories(data.getParent());
    TsFileGeneratorUtils.generateMixTsFile(data.toString(), 1, 1, 10, 0, 100, 10, 10);
    TsFileResource resource = new TsFileResource(data.toFile());
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create(database + ".d0");
    resource.updateStartTime(device, 0);
    resource.updateEndTime(device, 100);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.serialize();
    DataRegion region = new DataRegion(database, "0");
    region.setAllowCompaction(false);
    region.getTsFileManager().add(resource, true);
    ResearchRaftDataRegionStateMachine machine = new ResearchRaftDataRegionStateMachine(region);
    Path snapshot = root.resolve("consensus-snapshot");
    assertTrue(machine.takeSnapshot(snapshot.toFile()));
    assertFalse(Files.exists(snapshot.resolve("snapshot.log")));
    List<File> files = machine.getSnapshotFiles(snapshot.toFile());
    assertTrue(files.size() >= 2);
    Path received = root.resolve("received-snapshot");
    for (File file : files) {
      Path source = file.toPath().toAbsolutePath();
      assertTrue(source.startsWith(snapshot));
      Path destination = received.resolve(snapshot.relativize(source));
      Files.createDirectories(destination.getParent());
      Files.copy(source, destination);
    }
    // The receiving node has only transferred files, not the sender's local snapshot links/log.
    FileUtils.recursivelyDeleteFolder(snapshot.toString());
    assertTrue(SnapshotTaker.clearSnapshotOfDataRegion(region));
    DataRegion restored =
        new SnapshotLoader(received.toString(), database, "0").loadSnapshotForStateMachine();
    assertNotNull(restored);
    assertEquals(1, restored.getTsFileManager().getTsFileList(true).size());
    assertEquals(
        Files.size(data),
        Files.size(received.resolve("sequence/" + database + "/0/0/1-1-0-0.tsfile")));
  }
}
