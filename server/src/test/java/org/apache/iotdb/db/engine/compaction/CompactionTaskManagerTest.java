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
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.inner.InnerCompactionTest;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionTaskManagerTest extends InnerCompactionTest {
  static final Logger logger = LoggerFactory.getLogger(CompactionTaskManagerTest.class);
  File tempSGDir;
  final long MAX_WAITING_TIME = 120_000;

  @Before
  public void setUp() throws Exception {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (tempSGDir.exists()) {
      FileUtils.deleteDirectory(tempSGDir);
    }
    Assert.assertTrue(tempSGDir.mkdirs());
    super.setUp();
  }

  @Test
  public void testRepeatedSubmitBeforeExecution() throws Exception {
    logger.warn("testRepeatedSubmitBeforeExecution");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    SizeTieredCompactionTask task1 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    SizeTieredCompactionTask task2 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    tsFileManager.writeLock("test");
    CompactionTaskManager manager = CompactionTaskManager.getInstance();
    try {
      Assert.assertTrue(manager.addTaskToWaitingQueue(task1));
      Assert.assertEquals(manager.getTotalTaskCount(), 1);
      // a same task should not be submitted compaction task manager
      Assert.assertFalse(manager.addTaskToWaitingQueue(task2));
      Assert.assertEquals(manager.getTotalTaskCount(), 1);
      manager.submitTaskFromTaskQueue();
    } finally {
      tsFileManager.writeUnlock();
    }
    Thread.sleep(5000);
    Assert.assertEquals(0, manager.getTotalTaskCount());
    long waitingTime = 0;
    while (manager.getRunningCompactionTaskList().size() > 0) {
      Thread.sleep(100);
      waitingTime += 100;
      if (waitingTime % 10000 == 0) {
        logger.warn("{}", manager.getRunningCompactionTaskList());
      }
      if (waitingTime > MAX_WAITING_TIME) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testRepeatedSubmitWhenExecuting() throws Exception {
    logger.warn("testRepeatedSubmitWhenExecuting");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    SizeTieredCompactionTask task1 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    SizeTieredCompactionTask task2 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    tsFileManager.writeLock("test");
    try {
      CompactionTaskManager manager = CompactionTaskManager.getInstance();
      manager.addTaskToWaitingQueue(task1);
      manager.submitTaskFromTaskQueue();
      Thread.sleep(2000);
      // When a same compaction task is executing, the compaction task should not be submitted!
      Assert.assertEquals(manager.getExecutingTaskCount(), 1);
      Assert.assertFalse(manager.addTaskToWaitingQueue(task2));
    } finally {
      tsFileManager.writeUnlock();
    }
    long waitingTime = 0;
    while (CompactionTaskManager.getInstance().getRunningCompactionTaskList().size() > 0) {
      Thread.sleep(100);
      waitingTime += 100;
      if (waitingTime % 10000 == 0) {
        logger.warn("{}", CompactionTaskManager.getInstance().getRunningCompactionTaskList());
      }
      if (waitingTime > MAX_WAITING_TIME) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testRepeatedSubmitAfterExecution() throws Exception {
    logger.warn("testRepeatedSubmitAfterExecution");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    SizeTieredCompactionTask task1 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    SizeTieredCompactionTask task2 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    CompactionTaskManager manager = CompactionTaskManager.getInstance();
    manager.addTaskToWaitingQueue(task1);
    manager.submitTaskFromTaskQueue();
    while (manager.getTotalTaskCount() > 0) {
      Thread.sleep(10);
    }
    tsFileManager.writeLock("test");
    // an invalid task can be submitted to waiting queue, but should not be submitted to thread pool
    Assert.assertTrue(manager.addTaskToWaitingQueue(task2));
    manager.submitTaskFromTaskQueue();
    Assert.assertEquals(manager.getExecutingTaskCount(), 0);
    long waitingTime = 0;
    while (manager.getRunningCompactionTaskList().size() > 0) {
      Thread.sleep(100);
      waitingTime += 100;
      if (waitingTime % 10000 == 0) {
        logger.warn("{}", manager.getRunningCompactionTaskList());
      }
      if (waitingTime > MAX_WAITING_TIME) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testRemoveSelfFromRunningList() throws Exception {
    logger.warn("testRemoveSelfFromRunningList");
    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    SizeTieredCompactionTask task1 =
        new SizeTieredCompactionTask(
            "root.compactionTest",
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    CompactionTaskManager manager = CompactionTaskManager.getInstance();
    tsFileManager.writeLock("test");
    try {
      manager.addTaskToWaitingQueue(task1);
      manager.submitTaskFromTaskQueue();
      Thread.sleep(5000);
      List<AbstractCompactionTask> runningList = manager.getRunningCompactionTaskList();
      // compaction task should add itself to running list
      Assert.assertEquals(1, runningList.size());
      Assert.assertTrue(runningList.contains(task1));
    } finally {
      tsFileManager.writeUnlock();
    }
    // after execution, task should remove itself from running list
    Thread.sleep(5000);
    List<AbstractCompactionTask> runningList = manager.getRunningCompactionTaskList();
    Assert.assertEquals(0, runningList.size());
    long waitingTime = 0;
    while (manager.getRunningCompactionTaskList().size() > 0) {
      Thread.sleep(100);
      waitingTime += 100;
      if (waitingTime % 10000 == 0) {
        logger.warn("{}", manager.getRunningCompactionTaskList());
      }
      if (waitingTime > MAX_WAITING_TIME) {
        Assert.fail();
      }
    }
  }
}
