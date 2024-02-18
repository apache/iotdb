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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskQueue;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionTaskQueueTest {

  private final long originalMemorySizeForCompaction =
      SystemInfo.getInstance().getMemorySizeForCompaction();
  private final int originalFileNumLimitForCompaction =
      SystemInfo.getInstance().getTotalFileLimitForCompaction();

  @Before
  public void setup() {
    SystemInfo.getInstance().getCompactionMemoryCost().set(0);
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().setMemorySizeForCompaction(2000);
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(50);
  }

  @After
  public void teardown() {
    SystemInfo.getInstance().getCompactionMemoryCost().set(0);
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().setMemorySizeForCompaction(originalMemorySizeForCompaction);
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(originalFileNumLimitForCompaction);
  }

  @Test
  public void testPutAndTake() throws InterruptedException {
    AbstractCompactionTask mockTask = prepareTask(1000, 10);
    CompactionTaskQueue queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(mockTask);
    AbstractCompactionTask task = queue.take();
    Assert.assertNotNull(task);
    releaseTaskOccupiedResources(task);
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
  }

  @Test
  public void testPutAndTakeWithTaskBlockedByMemoryLimit() throws InterruptedException {
    AbstractCompactionTask mockTask1 = prepareTask(1500, 10);
    AbstractCompactionTask mockTask2 = prepareTask(200, 10);
    AbstractCompactionTask mockTask3 = prepareTask(600, 10);
    CompactionTaskQueue queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(mockTask1);
    queue.put(mockTask2);
    queue.put(mockTask3);
    AtomicInteger outTaskNum = new AtomicInteger(0);
    for (int i = 0; i < 10; i++) {
      CompletableFuture.supplyAsync(
          () -> {
            AbstractCompactionTask task = null;
            try {
              task = queue.take();
              if (task != null) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                releaseTaskOccupiedResources(task);
                outTaskNum.incrementAndGet();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return null;
          });
    }
    while (outTaskNum.get() != 3) {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    }
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
  }

  @Test
  public void testPutAndTakeWithTaskBlockedByFileNumLimit() throws InterruptedException {
    AbstractCompactionTask mockTask1 = prepareTask(500, 3);
    AbstractCompactionTask mockTask2 = prepareTask(200, 40);
    AbstractCompactionTask mockTask3 = prepareTask(600, 10);
    CompactionTaskQueue queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(mockTask1);
    queue.put(mockTask2);
    queue.put(mockTask3);
    AtomicInteger outTaskNum = new AtomicInteger(0);
    for (int i = 0; i < 10; i++) {
      CompletableFuture.supplyAsync(
          () -> {
            AbstractCompactionTask task = null;
            try {
              task = queue.take();
              if (task != null) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                releaseTaskOccupiedResources(task);
                outTaskNum.incrementAndGet();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return null;
          });
    }
    while (outTaskNum.get() != 3) {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    }
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
  }

  @Test
  public void testIncrementPriority() throws InterruptedException {
    AbstractCompactionTask mockTask1 = prepareTask(200, 10, 1);
    AbstractCompactionTask mockTask2 = prepareTask(1600, 10, 2);
    Mockito.when(mockTask2.getRetryAllocateResourcesTimes()).thenReturn(Integer.MAX_VALUE);
    AbstractCompactionTask mockTask3 = prepareTask(600, 10, 3);
    CompactionTaskQueue queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(mockTask1);
    queue.put(mockTask3);
    CountDownLatch latch = new CountDownLatch(3);
    new Thread(
            () -> {
              while (!queue.isEmpty()) {
                try {
                  AbstractCompactionTask task = queue.take();
                  Assert.assertEquals(mockTask3, task);

                  queue.put(mockTask2);
                  latch.countDown();

                  task = queue.take();
                  Assert.assertEquals(mockTask2, task);
                  latch.countDown();

                  task = queue.take();
                  Assert.assertEquals(mockTask1, task);

                  latch.countDown();
                } catch (InterruptedException ignored) {
                  Assert.fail();
                }
              }
            })
        .start();
    while (latch.getCount() != 2) {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    }
    releaseTaskOccupiedResources(mockTask3);
    latch.await();
    releaseTaskOccupiedResources(mockTask2);
    releaseTaskOccupiedResources(mockTask1);
  }

  private AbstractCompactionTask prepareTask(long memCost, int fileNum, long timePartition) {
    InnerSpaceCompactionTask mockTask = Mockito.mock(InnerSpaceCompactionTask.class);
    Mockito.when(mockTask.getEstimatedMemoryCost()).thenReturn(memCost);
    Mockito.when(mockTask.getProcessedFileNum()).thenReturn(fileNum);
    Mockito.when(mockTask.getAllSourceTsFiles()).thenReturn(Arrays.asList());
    Mockito.when(mockTask.isDiskSpaceCheckPassed()).thenReturn(true);
    Mockito.when(mockTask.getSumOfCompactionCount()).thenReturn(1);
    Mockito.when(mockTask.getSelectedTsFileResourceList())
        .thenReturn(Collections.singletonList(new TsFileResource()));
    Mockito.when(mockTask.getCompactionTaskType()).thenReturn(CompactionTaskType.INNER_SEQ);
    Mockito.when(mockTask.getTimePartition()).thenReturn(timePartition);
    Mockito.when(mockTask.isCompactionAllowed()).thenReturn(true);
    return mockTask;
  }

  private AbstractCompactionTask prepareTask(long memCost, int fileNum) {
    return prepareTask(memCost, fileNum, 0);
  }

  private void releaseTaskOccupiedResources(AbstractCompactionTask task) {
    SystemInfo.getInstance()
        .resetCompactionMemoryCost(task.getCompactionTaskType(), task.getEstimatedMemoryCost());
    SystemInfo.getInstance().decreaseCompactionFileNumCost(task.getProcessedFileNum());
  }
}
