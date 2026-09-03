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

package com.timecho.iotdb.dataregion.compaction.tool;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.utils.EnvironmentUtils;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SharedStorageCompactionUtilsTest {

  private static final FSFactory FS_FACTORY = FSFactoryProducer.getFSFactory();
  private static final String HOT_DIR = TestConstant.BASE_OUTPUT_PATH.concat("shared_obj_hot");
  private static final String COLD_DIR = TestConstant.BASE_OUTPUT_PATH.concat("shared_obj_cold");
  private static final String DATA_REGION_ID = "9";

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[][] originalTierDataDirs;

  @Before
  public void setUp() throws Exception {
    originalTierDataDirs = config.getTierDataDirs();
    new File(HOT_DIR).mkdirs();
    new File(COLD_DIR).mkdirs();
    config.setTierDataDirs(new String[][] {{HOT_DIR}, {COLD_DIR}});
    TierManager.getInstance().resetFolders();
  }

  @After
  public void tearDown() throws Exception {
    config.setTierDataDirs(originalTierDataDirs);
    TierManager.getInstance().resetFolders();
    EnvironmentUtils.cleanDir(HOT_DIR);
    EnvironmentUtils.cleanDir(COLD_DIR);
  }

  @Test
  public void shouldDeleteLocalRemoteObjectHonorsVersionSnapshot() {
    long timePartition = TimePartitionUtils.getTimePartitionId(1000L);
    long maxVersion = 3L;

    assertTrue(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "1000_2.bin", timePartition, maxVersion));
    assertTrue(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "1000_3.bin", timePartition, maxVersion));
    // Flushed after the task copied the TsFile list.
    assertFalse(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "1000_4.bin", timePartition, maxVersion));
    assertTrue(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "1000.bin", timePartition, maxVersion));
    assertFalse(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "1000.bin.tmp", timePartition, maxVersion));
    assertTrue(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "1000.bin.back", timePartition, maxVersion));

    long otherPartitionTime = TimePartitionUtils.getTimePartitionInterval() + 1000L;
    assertFalse(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            otherPartitionTime + "_1.bin", timePartition, maxVersion));
    assertFalse(
        SharedStorageCompactionUtils.shouldDeleteLocalRemoteObject(
            "not-an-object.tsfile", timePartition, maxVersion));
  }

  @Test
  public void deleteLocalRemoteObjectsSkipsWhenLowerTierObjectsRemain() throws Exception {
    long timePartition = TimePartitionUtils.getTimePartitionId(1000L);
    long maxVersion = 3L;
    int lastTier = TierManager.getInstance().getTiersNum() - 1;

    File hotObject =
        new File(
            HOT_DIR,
            IoTDBConstant.OBJECT_FOLDER_NAME
                + File.separator
                + DATA_REGION_ID
                + File.separator
                + "tbl"
                + File.separator
                + "1000_2.bin");
    Files.createDirectories(hotObject.getParentFile().toPath());
    Files.write(hotObject.toPath(), "payload".getBytes(StandardCharsets.UTF_8));

    assertTrue(
        SharedStorageCompactionUtils.hasUnmigratedObjects(
            DATA_REGION_ID, timePartition, maxVersion, lastTier));
    assertFalse(
        SharedStorageCompactionUtils.deleteLocalRemoteObjects(
            DATA_REGION_ID, timePartition, maxVersion));

    Files.deleteIfExists(hotObject.toPath());
    assertFalse(
        SharedStorageCompactionUtils.hasUnmigratedObjects(
            DATA_REGION_ID, timePartition, maxVersion, lastTier));
    assertTrue(
        SharedStorageCompactionUtils.deleteLocalRemoteObjects(
            DATA_REGION_ID, timePartition, maxVersion));
  }
}
