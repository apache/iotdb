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

package org.apache.iotdb.commons.disk;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.disk.FolderManager.FolderState;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.commons.utils.JVMCommonUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class FolderManagerLogTest {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private FolderManager folderManager;
  private List<String> testFolders;
  private NodeStatus previousNodeStatus;
  private String previousStatusReason;
  private double previousDiskSpaceWarningThreshold;

  @Before
  public void setUp() throws Exception {
    previousNodeStatus = COMMON_CONFIG.getNodeStatus();
    previousStatusReason = COMMON_CONFIG.getStatusReason();
    previousDiskSpaceWarningThreshold = COMMON_CONFIG.getDiskSpaceWarningThreshold();
    COMMON_CONFIG.setNodeStatus(NodeStatus.Running);
    COMMON_CONFIG.setStatusReason(null);
    setDiskSpaceWarningThresholdForTest(previousDiskSpaceWarningThreshold);
    FolderManager.resetAllFoldersFullLogTimes();

    File folder1 = tempFolder.newFolder("folder1");
    File folder2 = tempFolder.newFolder("folder2");
    File folder3 = tempFolder.newFolder("folder3");
    testFolders =
        Arrays.asList(
            folder1.getAbsolutePath(), folder2.getAbsolutePath(), folder3.getAbsolutePath());
    folderManager = new FolderManager(testFolders, DirectoryStrategyType.SEQUENCE_STRATEGY);
  }

  @After
  public void tearDown() {
    setDiskSpaceWarningThresholdForTest(previousDiskSpaceWarningThreshold);
    FolderManager.resetAllFoldersFullLogTimes();
    COMMON_CONFIG.setNodeStatus(previousNodeStatus);
    COMMON_CONFIG.setStatusReason(previousStatusReason);
  }

  @Test
  public void allFoldersFullErrorLoggedOnlyOnceUntilFolderRecovers() throws Exception {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FolderManager.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      markAllFolders(FolderState.ABNORMAL);
      assertDiskFullOnNextFolder();
      assertDiskFullOnNextFolder();

      Assert.assertEquals(
          1, countLogEvents(appender, UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY));
      Assert.assertEquals(NodeStatus.ReadOnly, COMMON_CONFIG.getNodeStatus());
      Assert.assertEquals(NodeStatus.DISK_FULL, COMMON_CONFIG.getStatusReason());

      folderManager.updateFolderState(testFolders.get(0), FolderState.HEALTHY);
      Assert.assertNotNull(folderManager.getNextFolder());

      markAllFolders(FolderState.ABNORMAL);
      assertDiskFullOnNextFolder();
      Assert.assertEquals(
          2, countLogEvents(appender, UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  @Test
  public void allFoldersFullErrorLoggedOnlyOnceAcrossFailedConstructions() throws Exception {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FolderManager.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      setDiskSpaceWarningThresholdForTest(1.1);
      assertDiskFullOnNewFolderManager();
      assertDiskFullOnNewFolderManager();

      Assert.assertEquals(
          1, countLogEvents(appender, UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY));

      setDiskSpaceWarningThresholdForTest(-1.0);
      Assert.assertNotNull(new FolderManager(testFolders, DirectoryStrategyType.SEQUENCE_STRATEGY));

      setDiskSpaceWarningThresholdForTest(1.1);
      assertDiskFullOnNewFolderManager();
      Assert.assertEquals(
          2, countLogEvents(appender, UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  @Test
  public void getNextWithRetryLogsAllFoldersFullOnlyOnceUntilFolderRecovers() throws Exception {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FolderManager.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      assertDiskFullAfterFolderProcessingRetriesFail();
      assertDiskFullAfterFolderProcessingRetriesFail();

      Assert.assertEquals(
          1, countLogEvents(appender, UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY));
      Assert.assertEquals(NodeStatus.ReadOnly, COMMON_CONFIG.getNodeStatus());
      Assert.assertEquals(NodeStatus.DISK_FULL, COMMON_CONFIG.getStatusReason());

      folderManager.updateFolderState(testFolders.get(0), FolderState.HEALTHY);
      Assert.assertEquals(
          testFolders.get(0), folderManager.getNextWithRetry(folder -> testFolders.get(0)));

      markAllFolders(FolderState.ABNORMAL);
      assertDiskFullAfterFolderProcessingRetriesFail();
      Assert.assertEquals(
          2, countLogEvents(appender, UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  private void markAllFolders(FolderState state) {
    testFolders.forEach(folder -> folderManager.updateFolderState(folder, state));
  }

  private void assertDiskFullOnNextFolder() {
    try {
      folderManager.getNextFolder();
      Assert.fail("Expected DiskSpaceInsufficientException");
    } catch (DiskSpaceInsufficientException e) {
      Assert.assertTrue(e.getMessage().contains("Can't get next folder"));
    }
  }

  private void assertDiskFullOnNewFolderManager() {
    try {
      new FolderManager(testFolders, DirectoryStrategyType.SEQUENCE_STRATEGY);
      Assert.fail("Expected DiskSpaceInsufficientException");
    } catch (DiskSpaceInsufficientException e) {
      Assert.assertTrue(e.getMessage().contains("Can't get next folder"));
    }
  }

  private void assertDiskFullAfterFolderProcessingRetriesFail() {
    try {
      folderManager.getNextWithRetry(
          folder -> {
            throw new RuntimeException("Failed to process folder");
          });
      Assert.fail("Expected DiskSpaceInsufficientException");
    } catch (DiskSpaceInsufficientException e) {
      Assert.assertTrue(e.getMessage().contains("Can't get next folder"));
    }
  }

  private void setDiskSpaceWarningThresholdForTest(double threshold) {
    COMMON_CONFIG.setDiskSpaceWarningThreshold(threshold);
    JVMCommonUtils.setDiskSpaceWarningThreshold(threshold);
  }

  private long countLogEvents(ListAppender<ILoggingEvent> appender, String messagePattern) {
    return appender.list.stream()
        .filter(event -> messagePattern.equals(event.getMessage()))
        .count();
  }
}
