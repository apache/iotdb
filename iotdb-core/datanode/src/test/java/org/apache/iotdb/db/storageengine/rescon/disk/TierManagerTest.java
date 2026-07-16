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

package org.apache.iotdb.db.storageengine.rescon.disk;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TierManagerTest {

  private static final int DATA_DIR_NUM = 16;
  private static final int RESET_TIMES = 20;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[][] originalTierDataDirs;
  private TierManager tierManager;
  private File sequenceFile;

  @Before
  public void setUp() throws Exception {
    originalTierDataDirs = config.getTierDataDirs();
    String[] dataDirs = new String[DATA_DIR_NUM];
    for (int i = 0; i < DATA_DIR_NUM; i++) {
      dataDirs[i] = new File(temporaryFolder.getRoot(), "data" + i).getAbsolutePath();
    }
    config.setTierDataDirs(new String[][] {dataDirs});
    tierManager = TierManager.getInstance();
    tierManager.resetFolders();
    sequenceFile =
        new File(dataDirs[0], IoTDBConstant.SEQUENCE_FOLDER_NAME + File.separator + "test.tsfile");
    assertTrue(sequenceFile.createNewFile());
  }

  @After
  public void tearDown() {
    config.setTierDataDirs(originalTierDataDirs);
    tierManager.resetFolders();
  }

  @Test
  public void testConcurrentResetFoldersAndGetNextFolder() throws Exception {
    List<Callable<?>> folderAccessors =
        Arrays.asList(
            () -> tierManager.getNextFolderForTsFile(0, true),
            () -> tierManager.getNextFolderForTsFile(0, false),
            tierManager::getNextFolderForObjectFile,
            tierManager::getNextFolderForCopyToTargetFile,
            () -> tierManager.getFolderManager(0, true),
            () -> tierManager.getFolderManager(0, false),
            () -> {
              int tiersNum = tierManager.getTiersNum();
              assertTrue(tiersNum > 0);
              return tiersNum;
            },
            () -> assertFoldersAvailable(tierManager.getAllFilesFolders()),
            () -> assertFoldersAvailable(tierManager.getAllLocalFilesFolders()),
            () -> assertFoldersAvailable(tierManager.getAllSequenceFileFolders()),
            () -> assertFoldersAvailable(tierManager.getAllLocalSequenceFileFolders()),
            () -> assertFoldersAvailable(tierManager.getAllUnSequenceFileFolders()),
            () -> assertFoldersAvailable(tierManager.getAllLocalUnSequenceFileFolders()),
            () -> tierManager.getFileTierLevel(sequenceFile));
    ExecutorService executorService = Executors.newFixedThreadPool(folderAccessors.size() + 1);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicBoolean resetting = new AtomicBoolean(true);

    Future<?> resetFuture =
        executorService.submit(
            () -> {
              startLatch.await();
              try {
                for (int i = 0; i < RESET_TIMES; i++) {
                  tierManager.resetFolders();
                }
              } finally {
                resetting.set(false);
              }
              return null;
            });
    List<AtomicInteger> accessorCounts = new ArrayList<>();
    List<Future<?>> accessorFutures = new ArrayList<>();
    for (Callable<?> folderAccessor : folderAccessors) {
      AtomicInteger accessorCount = new AtomicInteger();
      accessorCounts.add(accessorCount);
      accessorFutures.add(
          executorService.submit(
              () -> {
                startLatch.await();
                do {
                  assertNotNull(folderAccessor.call());
                  accessorCount.incrementAndGet();
                } while (resetting.get());
                return null;
              }));
    }

    startLatch.countDown();
    try {
      resetFuture.get(30, TimeUnit.SECONDS);
      for (Future<?> accessorFuture : accessorFutures) {
        accessorFuture.get(30, TimeUnit.SECONDS);
      }
      for (AtomicInteger accessorCount : accessorCounts) {
        assertTrue(accessorCount.get() > 0);
      }
    } finally {
      executorService.shutdownNow();
      assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private List<String> assertFoldersAvailable(List<String> folders) {
    assertTrue(folders.size() >= DATA_DIR_NUM);
    return folders;
  }
}
