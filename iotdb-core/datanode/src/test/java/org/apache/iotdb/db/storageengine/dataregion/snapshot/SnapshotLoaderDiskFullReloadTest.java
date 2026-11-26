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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest(JVMCommonUtils.class)
public class SnapshotLoaderDiskFullReloadTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private String[][] originalTierDataDirs;
  private NodeStatus originalStatus;
  private String originalReason;
  private TierManager tierManager;

  @Before
  public void setUp() {
    originalTierDataDirs = config.getTierDataDirs();
    originalStatus = commonConfig.getNodeStatus();
    originalReason = commonConfig.getStatusReason();
    config.setTierDataDirs(
        new String[][] {{new File(temporaryFolder.getRoot(), "old").getAbsolutePath()}});
    tierManager = TierManager.getInstance();
    commonConfig.setNodeStatus(NodeStatus.Running);
    PowerMockito.mockStatic(JVMCommonUtils.class);
  }

  @After
  public void tearDown() {
    when(JVMCommonUtils.hasSpace(anyString())).thenReturn(true);
    config.setTierDataDirs(originalTierDataDirs);
    tierManager.resetFolders();
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setNodeStatusWithReason(originalStatus, originalReason);
  }

  @Test
  public void testFailedSnapshotManagerRecoversAfterAddingDataDir() throws Exception {
    File snapshotDir = temporaryFolder.newFolder("snapshot");
    assertTrue(new File(snapshotDir, "sequence/root.sg/0").mkdirs());
    SnapshotLoader loader = new SnapshotLoader(snapshotDir.getAbsolutePath(), "root.sg", "0");
    assertThrows(
        DiskSpaceInsufficientException.class,
        () ->
            loader.createLinksFromSnapshotDirToDataDirWithoutLog(
                snapshotDir, new HashMap<>(), false));
    assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());

    AtomicBoolean walAvailable = new AtomicBoolean();
    commonConfig.setReadOnlyForDiskFull("wal", walAvailable::get);
    File oldDir = new File(temporaryFolder.getRoot(), "old");
    File newDir = new File(temporaryFolder.getRoot(), "new");
    config.setTierDataDirs(new String[][] {{oldDir.getAbsolutePath(), newDir.getAbsolutePath()}});
    when(JVMCommonUtils.hasSpaceWithoutRetry(newDir.getCanonicalPath())).thenReturn(true);
    for (String folderName :
        new String[] {
          IoTDBConstant.SEQUENCE_FOLDER_NAME,
          IoTDBConstant.UNSEQUENCE_FOLDER_NAME,
          IoTDBConstant.OBJECT_FOLDER_NAME
        }) {
      String folder = new File(newDir, folderName).getCanonicalPath();
      when(JVMCommonUtils.hasSpace(folder)).thenReturn(true);
      when(JVMCommonUtils.hasSpaceWithoutRetry(folder)).thenReturn(true);
    }
    tierManager.resetFolders();

    commonConfig.tryRecoverFromDiskFull();
    assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    walAvailable.set(true);
    commonConfig.tryRecoverFromDiskFull();
    assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    assertEquals(
        new File(newDir, IoTDBConstant.SEQUENCE_FOLDER_NAME).getCanonicalPath(),
        tierManager.getNextFolderForTsFile(0, true));
  }
}
