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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.DirectoryNotLegalException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.consensus.iot.IoTConsensusServerImpl.SNAPSHOT_DIR_NAME;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class IoTDBSnapshotTest {
  private String[][] testDataDirs =
      new String[][] {{"target/data/data1", "target/data/data2", "target/data/data3"}};
  private String testSgName = "root.testsg";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    FileUtils.recursivelyDeleteFolder("target" + File.separator + "data");
    EnvironmentUtils.cleanEnv();
    FileUtils.recursivelyDeleteFolder("target" + File.separator + "tmp");
  }

  private List<TsFileResource> writeTsFiles(String[] dataDirs)
      throws IOException, WriteProcessException {
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String filePath =
          dataDirs[i % dataDirs.length]
              + File.separator
              + "sequence"
              + File.separator
              + testSgName
              + File.separator
              + "0"
              + File.separator
              + "0"
              + File.separator
              + String.format("%d-%d-0-0.tsfile", i + 1, i + 1);
      File newFile = new File(filePath);
      Assert.assertTrue(newFile.getParentFile().exists() || newFile.getParentFile().mkdirs());
      TsFileGeneratorUtils.generateMixTsFile(filePath, 5, 5, 10, i * 100, (i + 1) * 100, 10, 10);
      TsFileResource resource = new TsFileResource(new File(filePath));
      Assert.assertTrue(new File(filePath).exists());
      resources.add(resource);
      for (int idx = 0; idx < 5; idx++) {
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(testSgName + PATH_SEPARATOR + "d" + i),
            i * 100);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(testSgName + PATH_SEPARATOR + "d" + i),
            (i + 1) * 100);
      }
      resource.updatePlanIndexes(i);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      resource.serialize();
    }
    return resources;
  }

  @Test
  public void testCreateSnapshot()
      throws IOException, WriteProcessException, DataRegionException, DirectoryNotLegalException {
    String[][] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    try {
      List<TsFileResource> resources = writeTsFiles(testDataDirs[0]);
      DataRegion region = new DataRegion(testSgName, "0");
      region.getTsFileManager().addAll(resources, true);
      File snapshotDir = new File("target" + File.separator + "snapshot");
      Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
      try {
        new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
        File[] files =
            snapshotDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
        assertEquals(1, files.length);
        SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(files[0]);
        Assert.assertTrue(analyzer.isSnapshotComplete());
        int cnt = analyzer.getTotalFileCountInSnapshot();
        analyzer.close();
        assertEquals(200, cnt);
        for (TsFileResource resource : resources) {
          Assert.assertTrue(resource.tryWriteLock());
        }
      } finally {
        FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
      TierManager.getInstance().resetFolders();
    }
  }

  @Test
  public void testCreateSnapshotWithUnclosedTsFile()
      throws IOException, WriteProcessException, DirectoryNotLegalException {
    String[][] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    try {
      List<TsFileResource> resources = writeTsFiles(testDataDirs[0]);
      resources.subList(50, 100).forEach(x -> x.setStatusForTest(TsFileResourceStatus.UNCLOSED));
      DataRegion region = new DataRegion(testSgName, "0");
      region.setAllowCompaction(false);
      region.getTsFileManager().addAll(resources, true);
      File snapshotDir = new File("target" + File.separator + "snapshot");
      Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
      try {
        new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
        File[] files =
            snapshotDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
        assertEquals(1, files.length);
        SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(files[0]);
        int cnt = 0;
        Assert.assertTrue(analyzer.isSnapshotComplete());
        cnt = analyzer.getTotalFileCountInSnapshot();
        analyzer.close();
        assertEquals(100, cnt);
        for (TsFileResource resource : resources) {
          Assert.assertTrue(resource.tryWriteLock());
        }
      } finally {
        FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
      TierManager.getInstance().resetFolders();
    }
  }

  @Test
  public void testLoadSnapshot()
      throws IOException, WriteProcessException, DataRegionException, DirectoryNotLegalException {
    String[][] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    try {
      List<TsFileResource> resources = writeTsFiles(testDataDirs[0]);
      DataRegion region = new DataRegion(testSgName, "0");
      CompressionRatio.getInstance().updateRatio(100, 100, "0");
      region.getTsFileManager().addAll(resources, true);
      File snapshotDir = new File("target" + File.separator + "snapshot");
      Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
      try {
        Assert.assertTrue(
            new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true));
        CompressionRatio.getInstance().reset();

        DataRegion dataRegion =
            new SnapshotLoader(snapshotDir.getAbsolutePath(), testSgName, "0")
                .loadSnapshotForStateMachine();
        Assert.assertNotNull(dataRegion);
        List<TsFileResource> resource = dataRegion.getTsFileManager().getTsFileList(true);
        assertEquals(100, resource.size());
        assertEquals(
            new Pair<>(100L, 100L),
            CompressionRatio.getInstance().getDataRegionRatioMap().get("0"));
      } finally {
        FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
      TierManager.getInstance().resetFolders();
    }
  }

  @Ignore("Need manual execution to specify different disks")
  @Test
  public void testLoadSnapshotNoHardLink()
      throws IOException, WriteProcessException, DirectoryNotLegalException {
    IoTDBDescriptor.getInstance().getConfig().setKeepSameDiskWhenLoadingSnapshot(true);
    // initialize dirs
    String[][] dataDirsForDB = new String[][] {{"C://snapshot_test", "D://snapshot_test"}};
    File snapshotDir = new File("D://snapshot_store//");
    if (snapshotDir.exists()) {
      FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
    }
    for (String[] dirs : dataDirsForDB) {
      for (String dir : dirs) {
        if (new File(dir).exists()) {
          FileUtils.recursivelyDeleteFolder(dir);
        }
      }
    }
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(dataDirsForDB);
    TierManager.getInstance().resetFolders();

    // prepare files, files should be written into two folders
    List<TsFileResource> resources = writeTsFiles(dataDirsForDB[0]);
    DataRegion region = new DataRegion(testSgName, "0");
    region.getTsFileManager().addAll(resources, true);

    // take a snapshot into one disk
    Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
    try {
      Assert.assertTrue(
          new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true));
      File[] files =
          snapshotDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
      // use loadWithoutLog
      if (files != null && files.length > 0) {
        files[0].delete();
      }
      // move files to snapshot store (simulate snapshot transfer)
      for (String dir : dataDirsForDB[0]) {
        File internalSnapshotDir = new File(dir, SNAPSHOT_DIR_NAME);
        if (internalSnapshotDir.exists()) {
          for (File file : FileUtils.listFilesRecursively(internalSnapshotDir, f -> true)) {
            if (file.isFile()) {
              String absolutePath = file.getAbsolutePath();
              int snapshotIdIndex = absolutePath.indexOf("snapshot_store");
              int suffixIndex = snapshotIdIndex + "snapshot_store".length();
              String suffix = absolutePath.substring(suffixIndex);
              File snapshotFile = new File(snapshotDir, suffix);
              FileUtils.copyFile(file, snapshotFile);
            }
          }
        }
      }

      // load the snapshot
      DataRegion dataRegion =
          new SnapshotLoader(snapshotDir.getAbsolutePath(), testSgName, "0")
              .loadSnapshotForStateMachine();
      Assert.assertNotNull(dataRegion);
      resources = dataRegion.getTsFileManager().getTsFileList(true);
      assertEquals(100, resources.size());

      // files should not be moved to another disk
      Path snapshotDirPath = snapshotDir.toPath();
      FileStore snapshotFileStore = Files.getFileStore(snapshotDirPath);
      for (TsFileResource tsFileResource : resources) {
        Path tsfilePath = tsFileResource.getTsFile().toPath();
        FileStore tsFileFileStore = Files.getFileStore(tsfilePath);
        assertEquals(snapshotFileStore, tsFileFileStore);
      }
    } finally {
      FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
      for (String[] dirs : dataDirsForDB) {
        for (String dir : dirs) {
          FileUtils.recursivelyDeleteFolder(dir);
        }
      }
    }
  }

  @Test
  public void testGetSnapshotFile() throws IOException {
    File tsFile =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs()[0]
                + File.separator
                + "sequence"
                + File.separator
                + "root.test"
                + File.separator
                + "0"
                + File.separator
                + "0"
                + File.separator
                + "1-1-0-0.tsfile");
    DataRegion region = Mockito.mock(DataRegion.class);
    Mockito.when(region.getDatabaseName()).thenReturn("root.test");
    Mockito.when(region.getDataRegionId()).thenReturn("0");
    File snapshotFile =
        new SnapshotTaker(region).getSnapshotFilePathForTsFile(tsFile, "test-snapshotId");
    assertEquals(
        new File(
                IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs()[0]
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + "root.test-0"
                    + File.separator
                    + "test-snapshotId"
                    + File.separator
                    + "sequence"
                    + File.separator
                    + "root.test"
                    + File.separator
                    + "0"
                    + File.separator
                    + "0"
                    + File.separator
                    + "1-1-0-0.tsfile")
            .getAbsolutePath(),
        snapshotFile.getAbsolutePath());

    Assert.assertTrue(snapshotFile.getParentFile().exists());
  }
}
