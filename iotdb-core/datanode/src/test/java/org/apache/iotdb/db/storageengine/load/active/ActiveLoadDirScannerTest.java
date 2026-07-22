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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;

import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ActiveLoadDirScannerTest {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[] originalListeningDirs;
  private String originalPipeDir;
  private boolean originalListeningEnabled;
  private File tempDir;
  private File pendingDir;
  private File pipeDir;

  @Before
  public void setUp() throws Exception {
    originalListeningDirs = config.getLoadActiveListeningDirs();
    originalPipeDir = config.getLoadActiveListeningPipeDir();
    originalListeningEnabled = config.getLoadActiveListeningEnable();

    tempDir = Files.createTempDirectory("active-load-scanner").toFile();
    pendingDir = new File(tempDir, "pending");
    pipeDir = new File(tempDir, "pipe");
    Assert.assertTrue(pendingDir.mkdirs());
    Assert.assertTrue(pipeDir.mkdirs());
    config.setLoadActiveListeningDirs(new String[] {pendingDir.getAbsolutePath()});
    config.setLoadActiveListeningPipeDir(pipeDir.getAbsolutePath());
    config.setLoadActiveListeningEnable(true);
  }

  @After
  public void tearDown() {
    config.setLoadActiveListeningDirs(originalListeningDirs);
    config.setLoadActiveListeningPipeDir(originalPipeDir);
    config.setLoadActiveListeningEnable(originalListeningEnabled);
    LoadUtil.updateLoadDiskSelector();
    deleteRecursively(tempDir);
  }

  @Test
  public void testScanDeduplicatesTsFileAndCompanionFiles() throws Exception {
    // The recursive scanner sees all four paths, but every companion maps back to the same TsFile.
    // Enqueuing each path separately used to consume queue capacity and schedule duplicate loads.
    final File tsFile = createCompletedTsFile(pendingDir, "1-0-0-0.tsfile");
    Assert.assertTrue(
        new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile());
    Assert.assertTrue(
        new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).createNewFile());
    Assert.assertTrue(
        new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).createNewFile());

    final ActiveLoadTsFileLoader loader = mock(ActiveLoadTsFileLoader.class);
    when(loader.getCurrentAllowedPendingSize()).thenReturn(10);
    final ActiveLoadDirScanner scanner = new ActiveLoadDirScanner(loader);
    try {
      final Method scanMethod = ActiveLoadDirScanner.class.getDeclaredMethod("scan");
      scanMethod.setAccessible(true);
      scanMethod.invoke(scanner);
    } finally {
      scanner.stop();
    }

    verify(loader, times(1))
        .tryTriggerTsFileLoad(
            eq(tsFile.getAbsolutePath()), eq(pendingDir.getAbsolutePath()), eq(false), eq(false));
  }

  @Test
  public void testAttributeAndTransferDirectoriesDoNotImplyTableModel() throws Exception {
    // Async tree loads add attribute and per-handoff transfer directories below pending. These are
    // internal directories, not table database names inferred from a user-created subdirectory.
    final Map<String, String> attributes =
        ActiveLoadPathHelper.buildAttributes(null, 2, false, false, null, false, "test-user");
    final File attributeDir = ActiveLoadPathHelper.resolveTargetDir(pendingDir, attributes);
    final File transferDir = new File(attributeDir, "transfer-id");
    Assert.assertTrue(transferDir.mkdirs());
    final File tsFile = createCompletedTsFile(transferDir, "2-0-0-0.tsfile");

    final ActiveLoadTsFileLoader loader = mock(ActiveLoadTsFileLoader.class);
    when(loader.getCurrentAllowedPendingSize()).thenReturn(10);
    final ActiveLoadDirScanner scanner = new ActiveLoadDirScanner(loader);
    try {
      final Method scanMethod = ActiveLoadDirScanner.class.getDeclaredMethod("scan");
      scanMethod.setAccessible(true);
      scanMethod.invoke(scanner);
    } finally {
      scanner.stop();
    }

    verify(loader, times(1))
        .tryTriggerTsFileLoad(
            eq(tsFile.getAbsolutePath()), eq(pendingDir.getAbsolutePath()), eq(false), eq(false));
  }

  private static File createCompletedTsFile(final File dir, final String fileName)
      throws Exception {
    final File tsFile = new File(dir, fileName);
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.endFile();
    }
    return tsFile;
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
