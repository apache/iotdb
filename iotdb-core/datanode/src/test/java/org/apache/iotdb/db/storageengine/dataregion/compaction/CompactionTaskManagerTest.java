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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.inner.InnerCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

public class CompactionTaskManagerTest extends InnerCompactionTest {
  static final Logger LOGGER = LoggerFactory.getLogger(CompactionTaskManagerTest.class);
  File tempSGDir;
  final long MAX_WAITING_TIME = 120_000;

  private ICompactionPerformer performer;

  @Before
  public void setUp() throws Exception {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (tempSGDir.exists()) {
      FileUtils.deleteDirectory(tempSGDir);
    }
    CompactionTaskManager.getInstance().restart();
    Assert.assertTrue(tempSGDir.mkdirs());
    super.setUp();
    performer = new FastCompactionPerformer(false);
    performer.setSourceFiles(seqResources);
  }

  @After
  public void tearDown() throws StorageEngineException, IOException {
    CompactionTaskManager.getInstance().waitAllCompactionFinish();
    super.tearDown();
  }

  @Test
  public void testRepeatedSubmitBeforeExecution() throws Exception {
    LOGGER.warn("testRepeatedSubmitBeforeExecution");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    InnerSpaceCompactionTask task1 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    seqResources.get(0).readLock();
    CompactionTaskManager manager = CompactionTaskManager.getInstance();
    Future<CompactionTaskSummary> summaryFuture = null;
    try {
      for (TsFileResource resource : seqResources) {
        Assert.assertFalse(resource.isCompactionCandidate());
      }
      Assert.assertTrue(manager.addTaskToWaitingQueue(task1));
      summaryFuture = CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task1);
      Assert.assertEquals(manager.getTotalTaskCount(), 1);
      for (TsFileResource resource : seqResources) {
        Assert.assertTrue(resource.isCompacting());
      }
      // a same task should not be submitted compaction task manager
      Assert.assertFalse(manager.addTaskToWaitingQueue(task2));
      Assert.assertEquals(manager.getTotalTaskCount(), 1);
      for (TsFileResource resource : seqResources) {
        Assert.assertTrue(resource.isCompacting());
      }
    } finally {
      seqResources.get(0).readUnlock();
    }
    manager.waitAllCompactionFinish();
    Assert.assertEquals(0, manager.getTotalTaskCount());
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }
  }

  @Test
  public void testRepeatedSubmitWhenExecuting() throws Exception {
    LOGGER.warn("testRepeatedSubmitWhenExecuting");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    InnerSpaceCompactionTask task1 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    seqResources.get(0).readLock();
    Future<CompactionTaskSummary> summaryFuture = null;
    try {
      CompactionTaskManager manager = CompactionTaskManager.getInstance();
      for (TsFileResource resource : seqResources) {
        Assert.assertFalse(resource.isCompactionCandidate());
      }
      manager.addTaskToWaitingQueue(task1);

      summaryFuture = CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task1);
      for (TsFileResource resource : seqResources) {
        Assert.assertFalse(resource.isCompactionCandidate());
      }
      // When a same compaction task is executing, the compaction task should not be submitted!
      Assert.assertEquals(manager.getExecutingTaskCount(), 1);
      Assert.assertFalse(manager.addTaskToWaitingQueue(task2));
      for (TsFileResource resource : seqResources) {
        Assert.assertFalse(resource.isCompactionCandidate());
      }
    } finally {
      seqResources.get(0).readUnlock();
    }
    if (summaryFuture != null) {
      summaryFuture.get();
    }
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }
  }

  @Test
  public void testRepeatedSubmitAfterExecution() throws Exception {
    LOGGER.warn("testRepeatedSubmitAfterExecution");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    InnerSpaceCompactionTask task1 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    CompactionTaskManager manager = CompactionTaskManager.getInstance();
    seqResources.get(0).readLock();
    Assert.assertTrue(manager.addTaskToWaitingQueue(task1));
    Future future = CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task1);
    seqResources.get(0).readUnlock();
    CompactionTaskManager.getInstance().waitAllCompactionFinish();

    // an invalid task cannot be submitted to waiting queue and cannot be submitted to thread pool
    try {
      Assert.assertFalse(manager.addTaskToWaitingQueue(task2));
      Assert.assertEquals(manager.getExecutingTaskCount(), 0);
    } finally {
      CompactionTaskManager.getInstance().waitAllCompactionFinish();
    }
  }

  @Test
  public void testRemoveSelfFromRunningList() throws Exception {
    LOGGER.warn("testRemoveSelfFromRunningList");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    InnerSpaceCompactionTask task1 =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    CompactionTaskManager manager = CompactionTaskManager.getInstance();
    manager.restart();
    seqResources.get(0).readLock();
    Future future = null;
    try {
      manager.addTaskToWaitingQueue(task1);
      future = CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task1);
      List<AbstractCompactionTask> runningList = manager.getRunningCompactionTaskList();
      // compaction task should add itself to running list
      Assert.assertEquals(1, runningList.size());
      Assert.assertTrue(runningList.contains(task1));
    } finally {
      seqResources.get(0).readUnlock();
    }
    // after execution, task should remove itself from running list
    future.get();
    Thread.sleep(10);
    List<AbstractCompactionTask> runningList = manager.getRunningCompactionTaskList();
    Assert.assertEquals(0, runningList.size());
    manager.waitAllCompactionFinish();
  }

  @Test
  public void testSizeTieredCompactionStatus() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, performer, 0);
    seqResources.get(0).readLock();
    CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.isCompactionCandidate() || resource.isCompacting());
    }
    Future future = CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task);
    seqResources.get(0).readUnlock();
    future.get();

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }
  }

  @Test
  public void testRewriteCrossCompactionFileStatus() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    seqResources = seqResources.subList(1, 5);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }

    CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
    seqResources.get(0).readLock();
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.isCompactionCandidate() || resource.isCompacting());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.isCompactionCandidate() || resource.isCompacting());
    }

    Future future = CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task);
    seqResources.get(0).readUnlock();
    future.get();
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.isCompactionCandidate());
    }
  }
}
