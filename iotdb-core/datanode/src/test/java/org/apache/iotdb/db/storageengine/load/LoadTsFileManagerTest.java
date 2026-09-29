/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LoadTsFileManagerTest {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[][] originalTierDataDirs;
  private File tempDir;
  private LoadTsFileManager loadTsFileManager;

  @Before
  public void setUp() throws Exception {
    originalTierDataDirs = copyDirs(config.getTierDataDirs());
    tempDir = Files.createTempDirectory("load-tsfile-manager").toFile();
    config.setTierDataDirs(new String[][] {{tempDir.getAbsolutePath()}});
    loadTsFileManager = new LoadTsFileManager();
  }

  @After
  public void tearDown() {
    if (loadTsFileManager != null) {
      loadTsFileManager.stop();
    }
    config.setTierDataDirs(originalTierDataDirs);
    deleteRecursively(tempDir);
  }

  @Test
  public void testRollbackDefersCleanupUntilActiveLoadTaskFinishes() throws Exception {
    final String uuid = "rollback-test";
    final CountDownLatch taskStarted = new CountDownLatch(1);
    final CountDownLatch finishTask = new CountDownLatch(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<String> activeTask =
          executor.submit(
              () ->
                  loadTsFileManager.executeLoadTask(
                      uuid,
                      () -> {
                        taskStarted.countDown();
                        try {
                          if (!finishTask.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("Timed out waiting to finish the load task");
                          }
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                        return "finished";
                      },
                      "rejected"));

      Assert.assertTrue(taskStarted.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(loadTsFileManager.deleteAll(uuid));
      Assert.assertEquals(
          "rejected", loadTsFileManager.executeLoadTask(uuid, () -> "unexpected", "rejected"));

      finishTask.countDown();
      Assert.assertEquals("finished", activeTask.get(10, TimeUnit.SECONDS));
      Assert.assertFalse(loadTsFileManager.deleteAll(uuid));
    } finally {
      finishTask.countDown();
      executor.shutdownNow();
    }
  }

  private static String[][] copyDirs(final String[][] dirs) {
    return Arrays.stream(dirs).map(String[]::clone).toArray(String[][]::new);
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Assert.assertTrue(file.delete());
  }
}
