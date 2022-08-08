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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class SnapshotTest {
  String[] originDataDirs;
  private String[] testDataDirs =
      new String[] {"target/data/data1", "target/data/data2", "target/data/data3"};
  private String testSgName = "root.testsg";

  @Before
  public void setUp() throws Exception {
    originDataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    IoTDBDescriptor.getInstance().getConfig().setDataDirs(testDataDirs);
    DirectoryManager.restartDirectoryManager();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance().getConfig().setDataDirs(originDataDirs);
    EnvironmentUtils.recursiveDeleteFolder("target" + File.separator + "data");
    EnvironmentUtils.cleanEnv();
  }

  private void writeTsFiles() throws IOException, WriteProcessException {
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
      for (int idx = 0; idx < 5; idx++) {
        resource.updateStartTime(testSgName + PATH_SEPARATOR + "d" + i, i * 100);
        resource.updateEndTime(testSgName + PATH_SEPARATOR + "d" + i, (i + 1) * 100);
      }
      resource.updatePlanIndexes(i);
      resource.setStatus(TsFileResourceStatus.CLOSED);
      resource.serialize();
    }
  }

  @Test
  public void testCreateSnapshot()
      throws IOException, WriteProcessException, DataRegionException, DirectoryNotLegalException {
    writeTsFiles();
    DataRegion region = new DataRegion(testSgName, "0");
    File snapshotDir = new File("target" + File.separator + "snapshot");
    Assert.assertTrue(snapshotDir.exists() || snapshotDir.mkdirs());
    try {
      new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
      File[] files =
          snapshotDir.listFiles(
              new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                  return name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME);
                }
              });
      Assert.assertEquals(files.length, 1);
      SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(files[0]);
      Assert.assertEquals(analyzer.getType(), SnapshotLogger.SnapshotType.LOCAL_DISK);
      int cnt = 0;
      while (analyzer.hasNext()) {
        analyzer.getNextPairs();
        cnt++;
      }
      analyzer.close();
      Assert.assertEquals(200, cnt);
    } finally {
      EnvironmentUtils.recursiveDeleteFolder(snapshotDir.getAbsolutePath());
    }
  }

  @Test
  public void testLoadSnapshot()
      throws IOException, WriteProcessException, DataRegionException, DirectoryNotLegalException {
    writeTsFiles();
    DataRegion region = new DataRegion(testSgName, "0");
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
      EnvironmentUtils.recursiveDeleteFolder(snapshotDir.getAbsolutePath());
    }
  }
}
