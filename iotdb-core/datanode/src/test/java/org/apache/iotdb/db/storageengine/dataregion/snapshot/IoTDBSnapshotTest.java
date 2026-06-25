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

import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
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
import java.lang.reflect.Method;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public void testLoadEmptySnapshotWithoutLog() throws IOException {
    String[][] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    File snapshotDir = new File("target" + File.separator + "empty-snapshot");
    try {
      if (snapshotDir.exists()) {
        FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
      }
      Assert.assertTrue(snapshotDir.mkdirs());

      DataRegion dataRegion =
          new SnapshotLoader(snapshotDir.getAbsolutePath(), testSgName, "0")
              .loadSnapshotForStateMachine();

      Assert.assertNotNull(dataRegion);
      assertEquals(0, dataRegion.getTsFileManager().getTsFileList(true).size());
      assertEquals(0, dataRegion.getTsFileManager().getTsFileList(false).size());
    } finally {
      FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
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

  /**
   * Regression test for the multi-receive-folder snapshot load. IoTConsensus spreads a single
   * snapshot's fragments across one receive folder per local data dir, so loading must clear the
   * data dirs once and relink the fragments from every folder. Before the fix each folder was
   * loaded with its own clear-then-relink, so every folder but the last had its just-linked
   * fragments wiped by the next folder's clear, losing all but the last folder's data.
   */
  @Test
  public void testLoadSnapshotSpreadAcrossReceiveFolders()
      throws IOException, WriteProcessException {
    loadSnapshotSpreadAcrossReceiveFolders(false);
  }

  /**
   * When IoTConsensus spreads a tsfile and its exclusive mods across different receive folders, the
   * loader must still relink them to the same data dir. Otherwise the mods file is not found next
   * to the tsfile and deletion markers are silently ignored.
   */
  @Test
  public void testLoadSnapshotKeepsTsFileAndModsOnSameDataDirWhenFragmentsAreSpread()
      throws IOException, WriteProcessException {
    String[][] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    String recvBase0 = "target" + File.separator + "recv-snapshot-mods-0";
    String recvBase1 = "target" + File.separator + "recv-snapshot-mods-1";
    File recvFolder0 = new File(recvBase0, SNAPSHOT_DIR_NAME);
    File recvFolder1 = new File(recvBase1, SNAPSHOT_DIR_NAME);
    try {
      Assert.assertTrue(recvFolder0.mkdirs());
      Assert.assertTrue(recvFolder1.mkdirs());

      writeSnapshotFragmentWithExclusiveModsSpread(recvFolder0.getAbsolutePath(), 0, recvFolder1);

      DataRegion dataRegion =
          new SnapshotLoader(
                  Arrays.asList(recvFolder0.getAbsolutePath(), recvFolder1.getAbsolutePath()),
                  testSgName,
                  "0")
              .loadSnapshotForStateMachine();

      Assert.assertNotNull(dataRegion);
      TsFileResource resource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
      File tsFile = resource.getTsFile();
      File modsFile =
          org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile
              .getExclusiveMods(tsFile);
      Assert.assertTrue(modsFile.exists());
      Assert.assertEquals(
          tsFile.getParentFile().getAbsolutePath(), modsFile.getParentFile().getAbsolutePath());
    } finally {
      FileUtils.recursivelyDeleteFolder(recvBase0);
      FileUtils.recursivelyDeleteFolder(recvBase1);
      IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
      TierManager.getInstance().resetFolders();
    }
  }

  /**
   * The fragments of one snapshot are disjoint across the receive folders, so the order in which
   * the folders are relinked must not change the loaded data. This loads the same spread snapshot
   * with the receive folders presented in the opposite order and expects the identical result.
   */
  @Test
  public void testLoadSnapshotFromReceiveFoldersIsOrderIndependent()
      throws IOException, WriteProcessException {
    loadSnapshotSpreadAcrossReceiveFolders(true);
  }

  private void loadSnapshotSpreadAcrossReceiveFolders(boolean reversedOrder)
      throws IOException, WriteProcessException {
    String[][] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    String recvBase0 = "target" + File.separator + "recv-snapshot-0";
    String recvBase1 = "target" + File.separator + "recv-snapshot-1";
    // Each receive folder holds the snapshot under a "snapshot" subdir, exactly as the IoTConsensus
    // receiver materializes it, and carries no snapshot log (the log is never transferred).
    File recvFolder0 = new File(recvBase0, SNAPSHOT_DIR_NAME);
    File recvFolder1 = new File(recvBase1, SNAPSHOT_DIR_NAME);
    try {
      Assert.assertTrue(recvFolder0.mkdirs());
      Assert.assertTrue(recvFolder1.mkdirs());

      // Spread the fragments across the two receive folders: even-indexed files in the first,
      // odd-indexed files in the second, so neither folder holds the whole snapshot.
      int fileNum = 6;
      for (int i = 0; i < fileNum; i++) {
        writeSnapshotFragment((i % 2 == 0 ? recvFolder0 : recvFolder1).getAbsolutePath(), i);
      }

      List<String> snapshotDirs =
          reversedOrder
              ? Arrays.asList(recvFolder1.getAbsolutePath(), recvFolder0.getAbsolutePath())
              : Arrays.asList(recvFolder0.getAbsolutePath(), recvFolder1.getAbsolutePath());

      DataRegion dataRegion =
          new SnapshotLoader(snapshotDirs, testSgName, "0").loadSnapshotForStateMachine();

      Assert.assertNotNull(dataRegion);
      // Every fragment from every receive folder must be present, regardless of relink order.
      assertEquals(fileNum, dataRegion.getTsFileManager().getTsFileList(true).size());
    } finally {
      FileUtils.recursivelyDeleteFolder(recvBase0);
      FileUtils.recursivelyDeleteFolder(recvBase1);
      IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
      TierManager.getInstance().resetFolders();
    }
  }

  /**
   * Materialize a single snapshot fragment (a TsFile and its resource) under {@code
   * <recvSnapshotDir>/sequence/<sg>/0/0/}, i.e. the layout the without-log loader expects from a
   * received snapshot.
   */
  private void writeSnapshotFragment(String recvSnapshotDir, int i)
      throws IOException, WriteProcessException {
    String filePath =
        recvSnapshotDir
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
    resource.updateStartTime(
        IDeviceID.Factory.DEFAULT_FACTORY.create(testSgName + PATH_SEPARATOR + "d" + i), i * 100);
    resource.updateEndTime(
        IDeviceID.Factory.DEFAULT_FACTORY.create(testSgName + PATH_SEPARATOR + "d" + i),
        (i + 1) * 100);
    resource.updatePlanIndexes(i);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.serialize();
  }

  private void writeSnapshotFragmentWithExclusiveModsSpread(
      String tsFileRecvSnapshotDir, int i, File modsRecvFolder)
      throws IOException, WriteProcessException {
    writeSnapshotFragment(tsFileRecvSnapshotDir, i);
    String tsFileName = String.format("%d-%d-0-0.tsfile", i + 1, i + 1);
    File tsFile =
        new File(
            tsFileRecvSnapshotDir
                + File.separator
                + "sequence"
                + File.separator
                + testSgName
                + File.separator
                + "0"
                + File.separator
                + "0"
                + File.separator
                + tsFileName);
    File sourceMods =
        org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile.getExclusiveMods(
            tsFile);
    Assert.assertTrue(sourceMods.exists() || sourceMods.createNewFile());

    File targetModsDir =
        new File(
            modsRecvFolder,
            "sequence" + File.separator + testSgName + File.separator + "0" + File.separator + "0");
    Assert.assertTrue(targetModsDir.exists() || targetModsDir.mkdirs());
    Files.copy(
        sourceMods.toPath(),
        new File(targetModsDir, sourceMods.getName()).toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    Files.delete(sourceMods.toPath());
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
    Mockito.when(region.getDataRegionIdString()).thenReturn("0");
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

  /**
   * Ensure snapshot-related files with the same fileKey are placed into the same data directory
   * even when hard link succeeds and the method returns early.
   */
  @Test
  public void testFileTargetRecordedWhenHardLinkSuccess() throws Exception {
    // snapshot source dir
    File snapshotDir = new File("target/test/snapshot-hardlink");
    if (snapshotDir.exists()) {
      FileUtils.recursivelyDeleteFolder(snapshotDir.getAbsolutePath());
    }
    Assert.assertTrue(snapshotDir.mkdirs());

    // same fileKey
    File tsFile = new File(snapshotDir, "1-1-0-0.tsfile");
    File resFile = new File(snapshotDir, "1-1-0-0.resource");
    File modsFile = new File(snapshotDir, "1-1-0-0.mods");

    Assert.assertTrue(tsFile.createNewFile());
    Assert.assertTrue(resFile.createNewFile());
    Assert.assertTrue(modsFile.createNewFile());

    File[] files = new File[] {tsFile, resFile, modsFile};

    // data dirs
    String[] dataDirs =
        new String[] {"target/test/data1", "target/test/data2", "target/test/data3"};

    for (String dir : dataDirs) {
      File base = new File(dir);
      if (base.exists()) {
        FileUtils.recursivelyDeleteFolder(base.getAbsolutePath());
      }
      Assert.assertTrue(base.mkdirs());
    }

    FolderManager folderManager =
        new FolderManager(Arrays.asList(dataDirs), DirectoryStrategyType.SEQUENCE_STRATEGY);

    String targetSuffix = "sequence/root.testsg/0/0";

    Method method =
        SnapshotLoader.class.getDeclaredMethod(
            "createLinksFromSnapshotToSourceDir",
            String.class,
            File[].class,
            FolderManager.class,
            Map.class,
            boolean.class);
    method.setAccessible(true);

    SnapshotLoader loader = new SnapshotLoader("dummy", "root.testsg", "0");

    // Tracks fileKey -> chosen data dir, so files sharing a fileKey land in the same dir.
    Map<String, String> fileTarget = new HashMap<>();
    method.invoke(loader, targetSuffix, files, folderManager, fileTarget, true);

    // The shared fileKey must be recorded exactly once, pointing at one of the data dirs.
    String fileKey = tsFile.getName().split("\\.")[0];
    Assert.assertEquals(1, fileTarget.size());
    Assert.assertTrue(Arrays.asList(dataDirs).contains(fileTarget.get(fileKey)));

    // verify: only ONE dir contains all three files
    int hitDirCount = 0;

    for (String dir : dataDirs) {
      File targetDir = new File(dir + "/" + targetSuffix);
      if (!targetDir.exists()) {
        continue;
      }

      boolean ts = new File(targetDir, tsFile.getName()).exists();
      boolean res = new File(targetDir, resFile.getName()).exists();
      boolean mods = new File(targetDir, modsFile.getName()).exists();

      if (ts || res || mods) {
        hitDirCount++;
        Assert.assertTrue(ts);
        Assert.assertTrue(res);
        Assert.assertTrue(mods);
      }
    }

    Assert.assertEquals(1, hitDirCount);
  }
}
