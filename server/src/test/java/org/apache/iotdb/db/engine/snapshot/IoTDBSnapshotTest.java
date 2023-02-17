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

package org.apache.iotdb.db.engine.snapshot;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.snapshot.exception.DirectoryNotLegalException;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class IoTDBSnapshotTest {
  private String[] testDataDirs =
      new String[] {"target/data/data1", "target/data/data2", "target/data/data3"};
  private String testSgName = "root.testsg";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    FileUtils.recursiveDeleteFolder("target" + File.separator + "data");
    EnvironmentUtils.cleanEnv();
    FileUtils.recursiveDeleteFolder("target" + File.separator + "tmp");
  }

  private List<TsFileResource> writeTsFiles() throws IOException, WriteProcessException {
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String filePath =
          testDataDirs[i % 3]
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
      TsFileGeneratorUtils.generateMixTsFile(filePath, 5, 5, 10, i * 100, (i + 1) * 100, 10, 10);
      TsFileResource resource = new TsFileResource(new File(filePath));
      resources.add(resource);
      for (int idx = 0; idx < 5; idx++) {
        resource.updateStartTime(testSgName + PATH_SEPARATOR + "d" + i, i * 100);
        resource.updateEndTime(testSgName + PATH_SEPARATOR + "d" + i, (i + 1) * 100);
      }
      resource.updatePlanIndexes(i);
      resource.setStatus(TsFileResourceStatus.CLOSED);
      resource.serialize();
    }
    return resources;
  }

  @Test
  public void testCreateSnapshot()
      throws IOException, WriteProcessException, DataRegionException, DirectoryNotLegalException {
    String[] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setDataDirs(testDataDirs);
    DirectoryManager.getInstance().resetFolders();
    try {
      List<TsFileResource> resources = writeTsFiles();
      DataRegion region = new DataRegion(testSgName, "0");
      region.getTsFileManager().addAll(resources, true);
      File snapshotDir = new File("target" + File.separator + "snapshot");
      Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
      try {
        new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
        File[] files =
            snapshotDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
        Assert.assertEquals(1, files.length);
        SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(files[0]);
        Assert.assertTrue(analyzer.isSnapshotComplete());
        int cnt = analyzer.getTotalFileCountInSnapshot();
        analyzer.close();
        Assert.assertEquals(200, cnt);
        for (TsFileResource resource : resources) {
          Assert.assertTrue(resource.tryWriteLock());
        }
      } finally {
        FileUtils.recursiveDeleteFolder(snapshotDir.getAbsolutePath());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDataDirs(originDataDirs);
      DirectoryManager.getInstance().resetFolders();
    }
  }

  @Test
  public void testCreateSnapshotWithUnclosedTsFile()
      throws IOException, WriteProcessException, DirectoryNotLegalException {
    String[] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setDataDirs(testDataDirs);
    DirectoryManager.getInstance().resetFolders();
    try {
      List<TsFileResource> resources = writeTsFiles();
      resources.subList(50, 100).forEach(x -> x.setStatus(TsFileResourceStatus.UNCLOSED));
      DataRegion region = new DataRegion(testSgName, "0");
      region.getTsFileManager().addAll(resources, true);
      File snapshotDir = new File("target" + File.separator + "snapshot");
      Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
      try {
        new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
        File[] files =
            snapshotDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
        Assert.assertEquals(1, files.length);
        SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(files[0]);
        int cnt = 0;
        Assert.assertTrue(analyzer.isSnapshotComplete());
        cnt = analyzer.getTotalFileCountInSnapshot();
        analyzer.close();
        Assert.assertEquals(100, cnt);
        for (TsFileResource resource : resources) {
          Assert.assertTrue(resource.tryWriteLock());
        }
      } finally {
        FileUtils.recursiveDeleteFolder(snapshotDir.getAbsolutePath());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDataDirs(originDataDirs);
      DirectoryManager.getInstance().resetFolders();
    }
  }

  @Test
  public void testLoadSnapshot()
      throws IOException, WriteProcessException, DataRegionException, DirectoryNotLegalException {
    String[] originDataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setDataDirs(testDataDirs);
    DirectoryManager.getInstance().resetFolders();
    try {
      List<TsFileResource> resources = writeTsFiles();
      DataRegion region = new DataRegion(testSgName, "0");
      region.getTsFileManager().addAll(resources, true);
      File snapshotDir = new File("target" + File.separator + "snapshot");
      Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
      try {
        Assert.assertTrue(
            new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true));
        DataRegion dataRegion =
            new SnapshotLoader(snapshotDir.getAbsolutePath(), testSgName, "0")
                .loadSnapshotForStateMachine();
        Assert.assertNotNull(dataRegion);
        List<TsFileResource> resource = dataRegion.getTsFileManager().getTsFileList(true);
        Assert.assertEquals(100, resource.size());
      } finally {
        FileUtils.recursiveDeleteFolder(snapshotDir.getAbsolutePath());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDataDirs(originDataDirs);
      DirectoryManager.getInstance().resetFolders();
    }
  }

  @Test
  public void testGetSnapshotFile() throws IOException {
    File tsFile =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0]
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
    Assert.assertEquals(
        new File(
                IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0]
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
