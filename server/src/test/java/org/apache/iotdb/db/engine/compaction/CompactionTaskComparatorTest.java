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

import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import com.google.common.collect.MinMaxPriorityQueue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class CompactionTaskComparatorTest {
  private final Logger LOGGER = LoggerFactory.getLogger(CompactionTaskComparatorTest.class);
  private final AtomicInteger taskNum = new AtomicInteger(0);
  private MinMaxPriorityQueue<AbstractCompactionTask> compactionTaskQueue =
      MinMaxPriorityQueue.orderedBy(new CompactionTaskComparator()).create();

  @Before
  public void setUp() {
    compactionTaskQueue.clear();
  }

  /** Test comparation of tasks with different file num */
  @Test
  public void testFileNumCompare() {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      compactionTasks[i] = new FakedInnerSpaceCompactionTask("fakeSg", 0, taskNum, true, resources);
      compactionTaskQueue.add(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == compactionTasks[i]);
    }
  }

  /** Test comparation of task with same file num and different file size */
  @Test
  public void testFileSizeCompare() {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j - i + 101));
      }
      compactionTasks[i] = new FakedInnerSpaceCompactionTask("fakeSg", 0, taskNum, true, resources);
      compactionTaskQueue.add(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test comparation of task with same file num and file size, different compaction count */
  @Test
  public void testFileCompactCountCompare() {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 10; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-%d-0.tsfile", i + j, i + j, j - i + 101)), 1));
      }
      compactionTasks[i] = new FakedInnerSpaceCompactionTask("fakeSg", 0, taskNum, true, resources);
      compactionTaskQueue.add(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  @Test
  public void testPriorityQueueSizeLimit() {
    MinMaxPriorityQueue<AbstractCompactionTask> limitQueue =
        MinMaxPriorityQueue.orderedBy(new CompactionTaskComparator()).maximumSize(50).create();
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 10; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-%d-0.tsfile", i + j, i + j, j - i + 101)), 1));
      }
      compactionTasks[i] = new FakedInnerSpaceCompactionTask("fakeSg", 0, taskNum, true, resources);
      limitQueue.add(compactionTasks[i]);
    }

    for (int i = 0; i < 100 && limitQueue.size() > 0; ++i) {
      AbstractCompactionTask currentTask = limitQueue.poll();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test comparation with same file num, file size, compaction count and different file version */
  @Test
  public void testFileVersionCompare() {
    AbstractCompactionTask[] compactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = 0; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(
                new File(String.format("%d-%d-0-0.tsfile", i + j, i + j, j - i + 101)), 1));
      }
      compactionTasks[i] = new FakedInnerSpaceCompactionTask("fakeSg", 0, taskNum, true, resources);
      compactionTaskQueue.add(compactionTasks[i]);
    }

    for (int i = 0; i < 100; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == compactionTasks[99 - i]);
    }
  }

  /** Test the comparation of different type of compaction task */
  @Test
  public void testComparationOfDifferentTaskType() {
    AbstractCompactionTask[] innerCompactionTasks = new AbstractCompactionTask[100];
    AbstractCompactionTask[] crossCompactionTasks = new AbstractCompactionTask[100];
    for (int i = 0; i < 100; ++i) {
      List<TsFileResource> resources = new ArrayList<>();
      for (int j = i; j < 100; ++j) {
        resources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      innerCompactionTasks[i] =
          new FakedInnerSpaceCompactionTask("fakeSg", 0, taskNum, true, resources);
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
              "fakeSg", 0, taskNum, sequenceResources, unsequenceResources);
    }

    for (int i = 0; i < 100; i++) {
      compactionTaskQueue.add(innerCompactionTasks[i]);
      compactionTaskQueue.add(crossCompactionTasks[i]);
    }

    for (int i = 0; i < 100; i++) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == innerCompactionTasks[i]);
    }

    for (int i = 0; i < 100; i++) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == crossCompactionTasks[99 - i]);
    }
  }

  /** Test the comparation of cross space compaction task */
  @Test
  public void testComparationOfCrossSpaceTask() {
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
              "fakeSg", 0, taskNum, sequenceResources, unsequenceResources);
      compactionTaskQueue.add(crossCompactionTasks[i]);
    }
    for (int i = 100; i < 200; ++i) {
      List<TsFileResource> sequenceResources = new ArrayList<>();
      for (int j = 0; j < 101; ++j) {
        sequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      List<TsFileResource> unsequenceResources = new ArrayList<>();
      for (int j = 100; j < i + 1; ++j) {
        unsequenceResources.add(
            new FakedTsFileResource(new File(String.format("%d-%d-0-0.tsfile", i + j, i + j)), j));
      }
      crossCompactionTasks[i] =
          new FakeCrossSpaceCompactionTask(
              "fakeSg", 0, taskNum, sequenceResources, unsequenceResources);
      compactionTaskQueue.add(crossCompactionTasks[i]);
    }

    for (int i = 0; i < 200; ++i) {
      AbstractCompactionTask currentTask = compactionTaskQueue.pollFirst();
      assertTrue(currentTask == crossCompactionTasks[i]);
    }
  }

  private static class FakedInnerSpaceCompactionTask extends AbstractInnerSpaceCompactionTask {

    public FakedInnerSpaceCompactionTask(
        String storageGroupName,
        long timePartition,
        AtomicInteger currentTaskNum,
        boolean sequence,
        List<TsFileResource> selectedTsFileResourceList) {
      super(storageGroupName, timePartition, currentTaskNum, sequence, selectedTsFileResourceList);
    }

    @Override
    protected void doCompaction() throws Exception {}

    @Override
    public boolean equalsOtherTask(AbstractCompactionTask other) {
      return false;
    }

    @Override
    public boolean checkValidAndSetMerging() {
      return true;
    }
  }

  private static class FakeCrossSpaceCompactionTask extends AbstractCrossSpaceCompactionTask {

    public FakeCrossSpaceCompactionTask(
        String fullStorageGroupName,
        long timePartition,
        AtomicInteger currentTaskNum,
        List<TsFileResource> selectedSequenceFiles,
        List<TsFileResource> selectedUnsequenceFiles) {
      super(
          fullStorageGroupName,
          timePartition,
          currentTaskNum,
          selectedSequenceFiles,
          selectedUnsequenceFiles);
    }

    @Override
    protected void doCompaction() throws Exception {}

    @Override
    public boolean equalsOtherTask(AbstractCompactionTask other) {
      return false;
    }

    @Override
    public boolean checkValidAndSetMerging() {
      return true;
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
