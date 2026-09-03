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

package com.timecho.iotdb.dataregion.migration;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ObjectFileMigrationUtilsTest {

  private static final String HOT_DIR = TestConstant.BASE_OUTPUT_PATH.concat("object_mig_hot");
  private static final String COLD_DIR = TestConstant.BASE_OUTPUT_PATH.concat("object_mig_cold");

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[][] originalTierDataDirs;

  @Before
  public void setUp() throws Exception {
    originalTierDataDirs = config.getTierDataDirs();
    new File(HOT_DIR).mkdirs();
    new File(COLD_DIR).mkdirs();
    config.setTierDataDirs(new String[][] {{HOT_DIR}, {COLD_DIR}});
    TierManager.getInstance().resetFolders();
    MigrationTaskManager.getInstance().reloadMigrateSpeedLimit();
  }

  @After
  public void tearDown() throws Exception {
    config.setTierDataDirs(originalTierDataDirs);
    TierManager.getInstance().resetFolders();
    EnvironmentUtils.cleanDir(HOT_DIR);
    EnvironmentUtils.cleanDir(COLD_DIR);
  }

  @Test
  public void testParseObjectTimestamp() {
    assertEquals(123L, ObjectFileMigrationUtils.parseObjectTimestamp("123.bin"));
    assertEquals(123L, ObjectFileMigrationUtils.parseObjectTimestamp("123_9.bin"));
    assertEquals(-1L, ObjectFileMigrationUtils.parseObjectTimestamp("abc.bin"));
    assertEquals(-1L, ObjectFileMigrationUtils.parseObjectTimestamp("123.tmp"));
  }

  @Test
  public void testMigrateObjectFileCopiesThenDeletesSource() throws Exception {
    String relativePath = "9" + File.separator + "tbl" + File.separator + "1.bin";
    File srcObject =
        new File(HOT_DIR, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    Files.createDirectories(srcObject.getParentFile().toPath());
    byte[] payload = "object-payload".getBytes(StandardCharsets.UTF_8);
    Files.write(srcObject.toPath(), payload);

    File dest = ObjectFileMigrationUtils.migrateObjectFile(srcObject, relativePath, 1);
    assertTrue(dest.exists());
    assertArrayEquals(payload, Files.readAllBytes(dest.toPath()));
    assertFalse("source should be deleted after successful migration", srcObject.exists());
    assertEquals(1, TierManager.getInstance().getObjectFileTierLevel(dest));
  }

  @Test
  public void testObjectMigrationTaskDeletesSource() throws Exception {
    String relativePath = "9" + File.separator + "tbl" + File.separator + "2.bin";
    File srcObject =
        new File(HOT_DIR, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    Files.createDirectories(srcObject.getParentFile().toPath());
    Files.write(srcObject.toPath(), "x".getBytes(StandardCharsets.UTF_8));

    Set<String> inFlight = ConcurrentHashMap.newKeySet();
    inFlight.add(srcObject.getAbsolutePath());
    new ObjectMigrationTask(MigrationCause.TTL, srcObject, relativePath, 0, 1, inFlight).run();

    assertFalse(srcObject.exists());
    assertTrue(inFlight.isEmpty());
    File cold =
        new File(COLD_DIR, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    assertTrue(cold.exists());
  }

  @Test
  public void testSkipWhenTempSiblingExists() throws Exception {
    String relativePath = "9" + File.separator + "tbl" + File.separator + "3.bin";
    File srcObject =
        new File(HOT_DIR, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    Files.createDirectories(srcObject.getParentFile().toPath());
    Files.write(srcObject.toPath(), "x".getBytes(StandardCharsets.UTF_8));
    Files.write(
        new File(srcObject.getPath() + ".tmp").toPath(), "t".getBytes(StandardCharsets.UTF_8));

    Set<String> inFlight = ConcurrentHashMap.newKeySet();
    inFlight.add(srcObject.getAbsolutePath());
    new ObjectMigrationTask(MigrationCause.DISK_SPACE, srcObject, relativePath, 0, 1, inFlight)
        .run();

    assertTrue(srcObject.exists());
    assertTrue(inFlight.isEmpty());
  }
}
