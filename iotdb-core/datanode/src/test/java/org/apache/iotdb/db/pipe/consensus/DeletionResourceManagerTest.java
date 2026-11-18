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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.PageCacheDeletionBuffer;
import org.apache.iotdb.db.utils.constant.TestConstant;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DeletionResourceManagerTest {
  private static final int DATA_REGION_ID = 99;
  private static final String DELETION_BASE_DIR =
      TestConstant.OUTPUT_DATA_DIR.concat("deletionManagerCloseTest");
  private static final long PERSIST_THREAD_IDLE_TIMEOUT_MS = 5000;

  private String previousDeletionFileDir;
  private int previousDataNodeId;

  @Before
  public void setUp() {
    previousDeletionFileDir =
        IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir();
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    IoTDBDescriptor.getInstance().getConfig().setIotConsensusV2DeletionFileDir(DELETION_BASE_DIR);
    DeletionResourceManager.buildForTest();
    deleteDataRegionDALFolder();
  }

  @After
  public void tearDown() {
    try {
      DeletionResourceManager.closeAndRemoveDAL(DATA_REGION_ID);
    } finally {
      deleteDataRegionDALFolder();
      File baseDir = new File(DELETION_BASE_DIR);
      if (baseDir.exists()) {
        FileUtils.deleteFileOrDirectory(baseDir);
      }
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setIotConsensusV2DeletionFileDir(previousDeletionFileDir);
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
    }
  }

  @Test
  public void testDeleteDALFolderDoesNotCreateManager() {
    File dalFolder = dataRegionDALFolder();
    Assert.assertTrue(dalFolder.mkdirs());

    DeletionResourceManager.closeAndRemoveDAL(DATA_REGION_ID);

    Assert.assertNull(DeletionResourceManager.getInstanceIfPresent(DATA_REGION_ID));
    Assert.assertFalse(dalFolder.exists());
  }

  @Test
  public void testCloseExistingManagerRemovesItAndDALFolder() {
    DeletionResourceManager manager = DeletionResourceManager.getInstance(DATA_REGION_ID);
    Assert.assertSame(manager, DeletionResourceManager.getInstanceIfPresent(DATA_REGION_ID));
    File dalFolder = dataRegionDALFolder();
    Assert.assertTrue(dalFolder.exists());

    DeletionResourceManager.closeAndRemoveDAL(DATA_REGION_ID);

    Assert.assertNull(DeletionResourceManager.getInstanceIfPresent(DATA_REGION_ID));
    Assert.assertFalse(dalFolder.exists());
  }

  @Test
  public void testClosingIdleManagerDoesNotWarnOnExpectedInterrupt() throws Exception {
    DeletionResourceManager manager = DeletionResourceManager.getInstance(DATA_REGION_ID);
    waitUntilPersistThreadIsIdle();

    Logger bufferLogger = (Logger) LoggerFactory.getLogger(PageCacheDeletionBuffer.class);
    Level originalLevel = bufferLogger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(bufferLogger.getLoggerContext());
    bufferLogger.setLevel(Level.DEBUG);
    appender.start();
    bufferLogger.addAppender(appender);
    try {
      manager.close();
    } finally {
      bufferLogger.detachAppender(appender);
      appender.stop();
      bufferLogger.setLevel(originalLevel);
    }

    String expectedMessage =
        DataNodePipeMessages.INTERRUPTED_WHEN_WAITING_FOR_TAKING_DELETIONRESOURCE_FROM;
    Assert.assertFalse(
        "closing an idle DeletionResourceManager must not report the expected shutdown interrupt as WARN",
        appender.list.stream()
            .anyMatch(
                event ->
                    event.getLevel() == Level.WARN
                        && expectedMessage.equals(event.getFormattedMessage())));
    Assert.assertTrue(
        "the expected shutdown interrupt should be logged at DEBUG",
        appender.list.stream()
            .anyMatch(
                event ->
                    event.getLevel() == Level.DEBUG
                        && expectedMessage.equals(event.getFormattedMessage())));
  }

  private void waitUntilPersistThreadIsIdle() throws InterruptedException {
    // The thread name is "pool-<n>-IoTDB-DAL-Serialize(group-<id>)-<n>".
    String threadNameSuffix =
        ThreadName.IOT_CONSENSUS_V2_DELETION_SERIALIZE.getName() + "(group-" + DATA_REGION_ID + ")";
    long deadline = System.currentTimeMillis() + PERSIST_THREAD_IDLE_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      for (Thread thread : Thread.getAllStackTraces().keySet()) {
        if (thread.getName().contains(threadNameSuffix)
            && thread.getState() == Thread.State.WAITING) {
          return;
        }
      }
      Thread.sleep(10);
    }
    Assert.fail("the DAL persist thread did not become idle within the timeout");
  }

  private File dataRegionDALFolder() {
    return new File(DELETION_BASE_DIR + File.separator + DATA_REGION_ID);
  }

  private void deleteDataRegionDALFolder() {
    File dalFolder = dataRegionDALFolder();
    if (dalFolder.exists()) {
      FileUtils.deleteFileOrDirectory(dalFolder);
    }
  }
}
