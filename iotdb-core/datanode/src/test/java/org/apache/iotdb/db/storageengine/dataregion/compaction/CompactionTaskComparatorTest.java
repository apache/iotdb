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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import com.google.common.collect.MinMaxPriorityQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompactionTaskComparatorTest {
  private final Logger LOGGER = LoggerFactory.getLogger(CompactionTaskComparatorTest.class);
  private FixedPriorityBlockingQueue<AbstractCompactionTask> compactionTaskQueue =
      new FixedPriorityBlockingQueue<>(1024, new DefaultCompactionTaskComparatorImpl());
  private TsFileManager tsFileManager = new TsFileManager("fakeSg", "0", "/");

  @Before
  public void setUp() {
    compactionTaskQueue.clear();
  }

  @After
  public void tearDown() {
    new CompactionConfigRestorer().restoreCompactionConfig();
  }

  /** Test comparation of tasks with different file num */
  @Test
  public void testFileNumCompare() throws InterruptedException {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      compactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, tsFileManager, true, resources, 0);
      compactionTaskQueue.put(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test comparation of task with same file num and different file size */
  @Test
  public void testFileSizeCompare() throws InterruptedException {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j - i + 101));
      }
      compactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, tsFileManager, true, resources, 0);
      compactionTaskQueue.put(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test comparation of task with same file num and file size, different compaction count */
  @Test
  public void testFileCompactCountCompare() throws InterruptedException {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 10; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-%d-0.tsfile", i + j, i + j, j - i + 101)), 1));
      }
      compactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, tsFileManager, true, resources, 0);
      compactionTaskQueue.put(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  @Test
  public void testPriorityQueueSizeLimit() {
    MinMaxPriorityQueue<AbstractCompactionTask> limitQueue =
        MinMaxPriorityQueue.orderedBy(new DefaultCompactionTaskComparatorImpl())
            .maximumSize(50)
            .create();
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 10; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-%d-0.tsfile", i + j, i + j, j - i + 101)), 1));
      }
      compactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, tsFileManager, true, resources, 0);
      limitQueue.add(compactionTasks[i]);
    }

    for (int i = 0; i < 100 && limitQueue.size() > 0; ++i) {
      AbstractCompactionTask currentTask = limitQueue.poll();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test comparation with same file num, file size, compaction count and different file version */
  @Test
  public void testFileVersionCompare() throws InterruptedException {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-0-0.tsfile", i + j, i + j, j - i + 101)), 1));
      }
      compactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, tsFileManager, true, resources, 0);
      compactionTaskQueue.put(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test the comparation of different type of compaction task */
  @Test
  public void testComparationOfDifferentTaskType() throws InterruptedException {
    IoTDBDescriptor.getInstance().getConfig().setCompactionPriority(CompactionPriority.INNER_CROSS);
    AbstractCompactionTask[] innerCompactionTasks = new AbstractCompactionTask[100];
    AbstractCompactionTask[] crossCompactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      innerCompactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, tsFileManager, true, resources, 0);
    }

    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> sequenceResources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        sequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      List<TsFileResource> unsequenceResources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        unsequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      crossCompactionTasks[i] =
          new FakeCrossSpaceCompactionTask(
              "fakeSg", 0, tsFileManager, sequenceResources, unsequenceResources, 0);
    }

    for (int i = 0; i < 100; i++) {
      compactionTaskQueue.put(innerCompactionTasks[i]);
      compactionTaskQueue.put(crossCompactionTasks[i]);
    }

    for (int i = 0; i < 100; i++) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == innerCompactionTasks[99 - i]);
    }

    for (int i = 0; i < 100; i++) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == crossCompactionTasks[99 - i]);
    }
  }

  /** Test the comparation of cross space compaction task */
  @Test
  public void testComparationOfCrossSpaceTask() throws InterruptedException {
    // the priority of the tasks in this array are created from highest to lowest
    AbstractCompactionTask[] crossCompactionTasks = new AbstractCompactionTask[200];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> sequenceResources = new ArrayList<>();
      for (int j = 0; j < i + 1; ++j) {
        sequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      List<TsFileResource> unsequenceResources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        unsequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      crossCompactionTasks[i] =
          new FakeCrossSpaceCompactionTask(
              "fakeSg", 0, tsFileManager, sequenceResources, unsequenceResources, 0);
      compactionTaskQueue.put(crossCompactionTasks[i]);
    }
    for (int i = 100; i < 200; ++i) {
      List<TsFileResource> sequenceResources = new ArrayList<>();
      for (int j = 0; j < 101; ++j) {
        sequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      List<TsFileResource> unsequenceResources = new ArrayList<>();
      for (int j = 199; j >= i; --j) {
        unsequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      crossCompactionTasks[i] =
          new FakeCrossSpaceCompactionTask(
              "fakeSg", 0, tsFileManager, sequenceResources, unsequenceResources, 0);
      compactionTaskQueue.put(crossCompactionTasks[i]);
    }

    for (int i = 0; i < 200; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.take();
      assertTrue(currentTask == crossCompactionTasks[i]);
    }
  }

  @Test
  public void testSerialId() throws InterruptedException {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    TsFileManager[] tsFileManagers = new TsFileManager[10];
    for (int i = 0; i < 10; ++i) {
      tsFileManagers[i] = new TsFileManager("fakeSg" + i, "0", "/");
      for (int j = 0; j < 10; ++j) {
        List<TsFileResource> resources = new ArrayList<>();
        // the j th compaction task for i th sg
        for (int k = 0; k < 10; ++k) {
          resources.add(
              new FakedTsFileResource(
                  new File(String.format("%d-%d-0-0.tsfile", j * 10 + k, j * 10 + k)), 10));
        }
        compactionTaskQueue.put(
            new FakedInnerSpaceCompactionTask(
                "fakeSg" + i, 0, tsFileManagers[i], true, resources, j));
      }
    }
    Map<String, AtomicInteger> taskCount = new HashMap<>();
    for (int i = 0; i < 10; ++i) {
      taskCount.put("fakeSg" + i + "-0", new AtomicInteger(0));
    }
    long cnt = 0;
    while (compactionTaskQueue.size() > 0) {
      for (int i = 0; i < 10; ++i) {
        AbstractCompactionTask task = compactionTaskQueue.take();
        String id =
            CompactionTaskManager.getSgWithRegionId(
                task.getStorageGroupName(), task.getDataRegionId());
        taskCount.get(id).incrementAndGet();
      }
      cnt++;
      for (int i = 0; i < 10; ++i) {
        assertEquals(cnt, taskCount.get("fakeSg" + i + "-0").get());
      }
    }
  }

  @Test
  public void testCompareByMaxModsFileSize()
      throws InterruptedException, IllegalPathException, IOException {
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      SettleCompactionTask settleTask =
          new SettleCompactionTask(
              0,
              tsFileManager,
              Collections.emptyList(),
              resources,
              true,
              new FastCompactionPerformer(false),
              0);
      compactionTaskQueue.put(settleTask);
    }

    String targetFileName = "101-101-0-0.tsfile";
    FakedTsFileResource fakedTsFileResource =
        new FakedTsFileResource(new File(targetFileName), 100);
    fakedTsFileResource.getModFile().write(new Deletion(new MeasurementPath("root.test.d1"), 1, 1));
    compactionTaskQueue.put(
        new SettleCompactionTask(
            0,
            tsFileManager,
            Collections.emptyList(),
            Collections.singletonList(fakedTsFileResource),
            true,
            new FastCompactionPerformer(false),
            0));
    SettleCompactionTask task = (SettleCompactionTask) compactionTaskQueue.take();
    Assert.assertEquals(targetFileName, task.getPartiallyDirtyFiles().get(0).getTsFile().getName());
    fakedTsFileResource.getModFile().remove();
  }

  @Test
  public void testCompareByTimePartitionWithInnerSpaceCompaction() throws InterruptedException {
    List<TsFileResource> resources1 = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      resources1.add(
          new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i, i)), 10));
    }
    FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
        new FixedPriorityBlockingQueue<>(
            IoTDBDescriptor.getInstance().getConfig().getCandidateCompactionTaskQueueSize(),
            new DefaultCompactionTaskComparatorImpl());
    for (int i = 0; i < 10; i++) {
      FakedInnerSpaceCompactionTask task =
          new FakedInnerSpaceCompactionTask("fakeSg", i, tsFileManager, true, resources1, 0);
      candidateCompactionTaskQueue.put(task);
    }

    for (int i = 9; i >= 0; i--) {
      Assert.assertEquals(candidateCompactionTaskQueue.take().getTimePartition(), i);
    }
  }

  @Test
  public void testCompareByTimePartitionWithCrossSpaceCompaction() throws InterruptedException {
    List<TsFileResource> seqResources = new ArrayList<>();
    List<TsFileResource> unseqResources = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      seqResources.add(
          new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i, i)), 10));
    }
    for (int i = 10; i < 20; i++) {
      unseqResources.add(
          new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i, i)), 10));
    }
    FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
        new FixedPriorityBlockingQueue<>(
            IoTDBDescriptor.getInstance().getConfig().getCandidateCompactionTaskQueueSize(),
            new DefaultCompactionTaskComparatorImpl());
    for (int i = 0; i < 10; i++) {
      CrossSpaceCompactionTask task =
          new CrossSpaceCompactionTask(
              i,
              tsFileManager,
              seqResources,
              unseqResources,
              new FastCompactionPerformer(true),
              0,
              0);
      candidateCompactionTaskQueue.put(task);
    }

    for (int i = 9; i >= 0; i--) {
      Assert.assertEquals(candidateCompactionTaskQueue.take().getTimePartition(), i);
    }
  }

  @Test
  public void testCompareByCompactionTaskType() throws InterruptedException {
    FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
        new FixedPriorityBlockingQueue<>(
            IoTDBDescriptor.getInstance().getConfig().getCandidateCompactionTaskQueueSize(),
            new DefaultCompactionTaskComparatorImpl());

    candidateCompactionTaskQueue.put(
        new SettleCompactionTask(
            1,
            tsFileManager,
            Collections.emptyList(),
            Collections.singletonList(
                new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", 1, 1)), 1)),
            true,
            new FastCompactionPerformer(false),
            0));
    candidateCompactionTaskQueue.put(
        new FakedInnerSpaceCompactionTask(
            "fakeSg",
            1,
            tsFileManager,
            true,
            Collections.singletonList(
                new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", 1, 2)), 1)),
            0));
    candidateCompactionTaskQueue.put(
        new SettleCompactionTask(
            1,
            tsFileManager,
            Collections.emptyList(),
            Collections.singletonList(
                new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", 1, 3)), 1)),
            true,
            new FastCompactionPerformer(false),
            0));

    Assert.assertEquals(
        candidateCompactionTaskQueue.take().getCompactionTaskType(), CompactionTaskType.SETTLE);
    Assert.assertEquals(
        candidateCompactionTaskQueue.take().getCompactionTaskType(), CompactionTaskType.SETTLE);
    Assert.assertEquals(
        candidateCompactionTaskQueue.take().getCompactionTaskType(), CompactionTaskType.INNER_SEQ);
  }

  private static class FakedInnerSpaceCompactionTask extends InnerSpaceCompactionTask {

    public FakedInnerSpaceCompactionTask(
        String storageGroupName,
        long timePartition,
        TsFileManager tsFileManager,
        boolean sequence,
        List<TsFileResource> selectedTsFileResourceList,
        long serialId) {
      super(
          timePartition,
          tsFileManager,
          selectedTsFileResourceList,
          sequence,
          new FastCompactionPerformer(false),
          serialId);
    }

    @Override
    protected boolean doCompaction() {
      return true;
    }

    @Override
    public boolean equalsOtherTask(AbstractCompactionTask other) {
      return false;
    }
  }

  private static class FakeCrossSpaceCompactionTask extends CrossSpaceCompactionTask {

    public FakeCrossSpaceCompactionTask(
        String fullStorageGroupName,
        long timePartition,
        TsFileManager tsFileManager,
        List<TsFileResource> selectedSequenceFiles,
        List<TsFileResource> selectedUnsequenceFiles,
        long serialId) {
      super(
          timePartition,
          tsFileManager,
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          new ReadPointCompactionPerformer(),
          0,
          serialId);
    }

    @Override
    public boolean doCompaction() {
      return true;
    }

    @Override
    public boolean equalsOtherTask(AbstractCompactionTask other) {
      return false;
    }
  }

  private static class FakedTsFileResource extends TsFileResource {
    long tsfileSize = 0;

    public FakedTsFileResource(File tsfile, long tsfileSize) {
      super(tsfile);
      this.tsfileSize = tsfileSize;
    }

    @Override
    public long getTsFileSize() {
      return tsfileSize;
    }
  }
}
