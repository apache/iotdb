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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;

public class ActiveLoadTsFileLoaderTest {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private File tempDir;
  private String originalFailDir;
  private NodeStatus originalNodeStatus;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("active-load-retry").toFile();
    originalFailDir = config.getLoadActiveListeningFailDir();
    originalNodeStatus = CommonDescriptor.getInstance().getConfig().getNodeStatus();
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Running);
    config.setLoadActiveListeningFailDir(new File(tempDir, "failed").getAbsolutePath());
  }

  @After
  public void tearDown() {
    config.setLoadActiveListeningFailDir(originalFailDir);
    CommonDescriptor.getInstance().getConfig().setNodeStatus(originalNodeStatus);
    deleteRecursively(tempDir);
  }

  @Test
  public void testTemporaryUnavailableStatusDoesNotMoveFileToFailDir() throws Exception {
    final ActiveLoadTsFileLoader loader = new ActiveLoadTsFileLoader();
    final File tsFile = createTsFileWithCompanionFiles("retry.tsfile");
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry(
            tsFile.getAbsolutePath(), tempDir.getAbsolutePath(), false, false);

    invokeInitFailDirIfNecessary(loader);
    invokeHandleLoadFailure(
        loader,
        entry,
        new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("load conversion is temporarily unavailable"));

    Assert.assertTrue(tsFile.exists());
    Assert.assertTrue(new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertTrue(new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).exists());
    Assert.assertTrue(new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    Assert.assertFalse(new File(config.getLoadActiveListeningFailDir(), tsFile.getName()).exists());
  }

  @Test
  public void testPermanentFailureStatusMovesFileToFailDir() throws Exception {
    final ActiveLoadTsFileLoader loader = new ActiveLoadTsFileLoader();
    final File tsFile = createTsFileWithCompanionFiles("failed.tsfile");
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry(
            tsFile.getAbsolutePath(), tempDir.getAbsolutePath(), false, false);

    invokeInitFailDirIfNecessary(loader);
    invokeHandleLoadFailure(
        loader,
        entry,
        new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage("permanent error"));

    final File failDir = new File(config.getLoadActiveListeningFailDir());
    Assert.assertFalse(tsFile.exists());
    Assert.assertTrue(new File(failDir, tsFile.getName()).exists());
    Assert.assertTrue(
        new File(failDir, tsFile.getName() + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertTrue(
        new File(failDir, tsFile.getName() + ModificationFileV1.FILE_SUFFIX).exists());
    Assert.assertTrue(new File(failDir, tsFile.getName() + ModificationFile.FILE_SUFFIX).exists());
  }

  @Test
  public void testStopClearsPendingFilesForRestart() throws Exception {
    final ActiveLoadTsFileLoader loader = new ActiveLoadTsFileLoader();
    final Field pendingQueueField = ActiveLoadTsFileLoader.class.getDeclaredField("pendingQueue");
    pendingQueueField.setAccessible(true);
    final ActiveLoadPendingQueue pendingQueue =
        (ActiveLoadPendingQueue) pendingQueueField.get(loader);
    final String tsFilePath = new File(tempDir, "pending.tsfile").getAbsolutePath();
    Assert.assertTrue(pendingQueue.enqueue(tsFilePath, tempDir.getAbsolutePath(), false, false));
    Assert.assertTrue(loader.isFilePendingOrLoading(new File(tsFilePath)));

    // A loader restart reuses this queue. Stale pending membership otherwise makes the scanner
    // believe the on-disk TsFile is already scheduled and it will never enqueue it again.
    loader.stop();

    Assert.assertFalse(loader.isFilePendingOrLoading(new File(tsFilePath)));
    Assert.assertTrue(pendingQueue.isEmpty());
  }

  private File createTsFileWithCompanionFiles(final String fileName) throws Exception {
    final File tsFile = new File(tempDir, fileName);
    Assert.assertTrue(tsFile.createNewFile());
    Assert.assertTrue(
        new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile());
    Assert.assertTrue(
        new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).createNewFile());
    Assert.assertTrue(
        new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).createNewFile());
    return tsFile;
  }

  private void invokeInitFailDirIfNecessary(final ActiveLoadTsFileLoader loader) throws Exception {
    final Method method = ActiveLoadTsFileLoader.class.getDeclaredMethod("initFailDirIfNecessary");
    method.setAccessible(true);
    method.invoke(loader);
  }

  private void invokeHandleLoadFailure(
      final ActiveLoadTsFileLoader loader,
      final ActiveLoadPendingQueue.ActiveLoadEntry entry,
      final TSStatus status)
      throws Exception {
    final Method method =
        ActiveLoadTsFileLoader.class.getDeclaredMethod(
            "handleLoadFailure", ActiveLoadPendingQueue.ActiveLoadEntry.class, TSStatus.class);
    method.setAccessible(true);
    method.invoke(loader, entry, status);
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
