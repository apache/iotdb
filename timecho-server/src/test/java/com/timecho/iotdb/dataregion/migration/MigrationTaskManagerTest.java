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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.os.conf.ObjectStorageConfig;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.conf.provider.TestConfig;
import com.timecho.iotdb.os.utils.ObjectStorageType;
import com.timecho.iotdb.utils.EnvironmentUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.utils.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MigrationTaskManagerTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final TSFileConfig tsfileConfig = TSFileDescriptor.getInstance().getConfig();
  private static final ObjectStorageConfig osConfig =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final String MIGRATION_SOURCE_BASE_DIR =
      TestConstant.BASE_OUTPUT_PATH.concat("migration_src");
  private static final String MIGRATION_SOURCE_TIME_PARTITION_DIR =
      MIGRATION_SOURCE_BASE_DIR + File.separator + "sg" + File.separator + 0 + File.separator + 0;
  private static final String MIGRATION_DESTINATION_OS_DIR =
      FSUtils.getOSDefaultPath("migration_destination", config.getDataNodeId());
  private static final String MIGRATION_DESTINATION_BASE_DIR =
      TestConstant.BASE_OUTPUT_PATH.concat("migration_destination");
  private static final String MIGRATION_DESTINATION_TIME_PARTITION_DIR =
      MIGRATION_DESTINATION_BASE_DIR
          + File.separator
          + config.getDataNodeId()
          + File.separator
          + "sg"
          + File.separator
          + 0
          + File.separator
          + 0;
  private FSType[] prevTSFileStorageFs;
  private ObjectStorageType prevOSType;
  private long[] prevTieredStorageMigrateSpeedLimitBytesPerSec;
  private String[][] prevTierDataDirs;

  @Before
  public void setUp() throws Exception {
    prevTSFileStorageFs = tsfileConfig.getTSFileStorageFs();
    tsfileConfig.setObjectStorageFile("com.timecho.iotdb.os.fileSystem.OSFile");
    tsfileConfig.setObjectStorageTsFileInput("com.timecho.iotdb.os.fileSystem.OSTsFileInput");
    tsfileConfig.setObjectStorageTsFileOutput("com.timecho.iotdb.os.fileSystem.OSTsFileOutput");
    tsfileConfig.setTSFileStorageFs(new FSType[] {FSType.LOCAL, FSType.OBJECT_STORAGE});
    FSUtils.reload();
    prevOSType = osConfig.getOsType();
    osConfig.setOsType(ObjectStorageType.TEST);
    ((TestConfig) osConfig.getProviderConfig()).setTestDir(MIGRATION_DESTINATION_BASE_DIR);
    new File(MIGRATION_SOURCE_TIME_PARTITION_DIR).mkdirs();
    new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR).mkdirs();
    prevTierDataDirs = config.getTierDataDirs();
    config.setTierDataDirs(
        new String[][] {new String[] {"/tmp/test1"}, new String[] {"/tmp/test1"}});
    prevTieredStorageMigrateSpeedLimitBytesPerSec =
        config.getTieredStorageMigrateSpeedLimitBytesPerSec();
    config.setTieredStorageMigrateSpeedLimitBytesPerSec(new long[] {1024 * 1024});
    MigrationTaskManager.getInstance().start();
  }

  @After
  public void tearDown() throws Exception {
    tsfileConfig.setTSFileStorageFs(prevTSFileStorageFs);
    FSUtils.reload();
    osConfig.setOsType(prevOSType);
    EnvironmentUtils.cleanDir(MIGRATION_SOURCE_BASE_DIR);
    EnvironmentUtils.cleanDir(MIGRATION_DESTINATION_BASE_DIR);
    config.setTieredStorageMigrateSpeedLimitBytesPerSec(
        prevTieredStorageMigrateSpeedLimitBytesPerSec);
    config.setTierDataDirs(prevTierDataDirs);
    MigrationTaskManager.getInstance().stop();
  }

  @Test
  public void testTrafficLimit() throws Exception {
    String fileName = TsFileNameGenerator.generateNewTsFileName(0, 0, 0, 0);
    // create source files
    File srcFile = new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName);
    srcFile.createNewFile();
    try (OutputStream out = Files.newOutputStream(srcFile.toPath())) {
      byte[] bytes = new byte[6 * 1024 * 1024];
      out.write(bytes);
    }
    File srcResourceFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    srcResourceFile.createNewFile();
    try (OutputStream out = Files.newOutputStream(srcResourceFile.toPath())) {
      byte[] bytes = new byte[1024 * 1024];
      out.write(bytes);
    }
    File srcModsFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    srcModsFile.createNewFile();
    File destFile = new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName);
    File destResourceFile =
        new File(
            MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    File destModsFile =
        new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    // migrate
    TsFileResource tsfile = new TsFileResource(srcFile);
    MigrationTask task =
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_OS_DIR);
    long startTime = System.currentTimeMillis();
    task.migrate();
    System.out.println(System.currentTimeMillis() - startTime);
    assertTrue(System.currentTimeMillis() - startTime > 5_000);
    // check
    assertFalse(srcFile.exists());
    assertTrue(srcResourceFile.exists());
    assertTrue(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(srcFile, tsfile.getTsFile());
  }

  @Test
  public void testMigrationTaskSlotWaitsForCompletion() throws Exception {
    int migrationTaskLimit = config.getMigrateThreadCount() + 50;
    // Exhaust every slot so the next submission must wait for a completed migration task.
    for (int i = 0; i < migrationTaskLimit; i++) {
      MigrationTaskManager.getInstance().acquireMigrationTaskSlot();
    }

    CountDownLatch waiterStarted = new CountDownLatch(1);
    AtomicBoolean waiterAcquiredSlot = new AtomicBoolean(false);
    AtomicReference<Throwable> waiterError = new AtomicReference<>();
    boolean slotReleasedForWaiter = false;
    Thread waiter =
        new Thread(
            () -> {
              waiterStarted.countDown();
              try {
                MigrationTaskManager.getInstance().acquireMigrationTaskSlot();
                waiterAcquiredSlot.set(true);
              } catch (Throwable t) {
                waiterError.set(t);
              }
            });
    waiter.start();

    try {
      assertTrue(waiterStarted.await(5, TimeUnit.SECONDS));
      long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (waiter.getState() != Thread.State.WAITING && System.nanoTime() < waitDeadline) {
        Thread.yield();
      }
      assertEquals(Thread.State.WAITING, waiter.getState());

      MigrationTaskManager.getInstance().releaseMigrationTaskSlot();
      slotReleasedForWaiter = true;
      waiter.join(TimeUnit.SECONDS.toMillis(5));

      assertFalse(waiter.isAlive());
      assertTrue(waiterAcquiredSlot.get());
      assertNull(waiterError.get());
    } finally {
      if (waiter.isAlive()) {
        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
      }
      int heldSlots = migrationTaskLimit - (slotReleasedForWaiter ? 1 : 0);
      if (waiterAcquiredSlot.get()) {
        heldSlots++;
      }
      // Restore the singleton manager's permits for other tests and the service tear-down.
      for (int i = 0; i < heldSlots; i++) {
        MigrationTaskManager.getInstance().releaseMigrationTaskSlot();
      }
    }
  }
}
