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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskQueue;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionTaskQueueTest extends AbstractCompactionTest {

  private final long originalMemorySizeForCompaction =
      SystemInfo.getInstance().getMemorySizeForCompaction();
  private final int originalFileNumLimitForCompaction =
      SystemInfo.getInstance().getTotalFileLimitForCompaction();

  @Before
  public void setup()
      throws IOException, InterruptedException, MetadataException, WriteProcessException {
    SystemInfo.getInstance().getCompactionMemoryCost().set(0);
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().setMemorySizeForCompaction(2000);
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(50);
    super.setUp();
  }

  @After
  public void teardown() throws StorageEngineException, IOException {
    SystemInfo.getInstance().getCompactionMemoryCost().set(0);
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().setMemorySizeForCompaction(originalMemorySizeForCompaction);
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(originalFileNumLimitForCompaction);
    super.tearDown();
  }

  @Test
  public void testPutAndTake()
      throws InterruptedException, IOException, MetadataException, WriteProcessException {
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
  public void testPutAndTakeWithTaskBlockedByMemoryLimit()
      throws InterruptedException, IOException, MetadataException, WriteProcessException {
    AbstractCompactionTask mockTask1 = prepareTask(1500, 10);
    AbstractCompactionTask mockTask2 = prepareTask(200, 10);
    AbstractCompactionTask mockTask3 = prepareTask(600, 10);
    CompactionTaskQueue queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(mockTask1);
    queue.put(mockTask2);
    queue.put(mockTask3);
    AtomicInteger outTaskNum = new AtomicInteger(0);
    List<Thread> threadList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Thread thread =
          new Thread(
              () -> {
                AbstractCompactionTask task = null;
                try {
                  task = queue.take();
                  if (task != null) {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                    releaseTaskOccupiedResources(task);
                    outTaskNum.incrementAndGet();
                  }
                } catch (InterruptedException ignored) {
                }
              });
      threadList.add(thread);
      thread.start();
    }
    while (outTaskNum.get() != 3) {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    }
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
    for (Thread thread : threadList) {
      thread.interrupt();
      thread.join();
    }
  }

  @Test
  public void testPutAndTakeWithTaskBlockedByFileNumLimit()
      throws InterruptedException, IOException, MetadataException, WriteProcessException {
    AbstractCompactionTask mockTask1 = prepareTask(500, 3);
    AbstractCompactionTask mockTask2 = prepareTask(200, 40);
    AbstractCompactionTask mockTask3 = prepareTask(600, 10);
    CompactionTaskQueue queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(mockTask1);
    queue.put(mockTask2);
    queue.put(mockTask3);
    AtomicInteger outTaskNum = new AtomicInteger(0);
    List<Thread> threadList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Thread thread =
          new Thread(
              () -> {
                AbstractCompactionTask task = null;
                try {
                  task = queue.take();
                  if (task != null) {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                    releaseTaskOccupiedResources(task);
                    outTaskNum.incrementAndGet();
                  }
                } catch (InterruptedException ignored) {
                }
              });
      threadList.add(thread);
      thread.start();
    }

    while (outTaskNum.get() != 3) {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    }
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
    for (Thread thread : threadList) {
      thread.interrupt();
      thread.join();
    }
  }

  private AbstractCompactionTask prepareTask(long memCost, int fileNum, long timePartition)
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 1, 1, 1, 1, 1, 1, 1, true, true);
    seqResources
        .get(seqResources.size() - 1)
        .setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            timePartition,
            tsFileManager,
            Collections.singletonList(seqResources.get(seqResources.size() - 1)),
            true,
            new ReadChunkCompactionPerformer(),
            0);
    InnerSpaceCompactionTask mockTask = Mockito.spy(task);
    Mockito.doReturn(memCost).when(mockTask).getEstimatedMemoryCost();
    Mockito.doReturn(fileNum).when(mockTask).getProcessedFileNum();
    Mockito.doReturn(true).when(mockTask).isDiskSpaceCheckPassed();
    Mockito.doReturn(1).when(mockTask).getSumOfCompactionCount();
    Mockito.doReturn(true).when(mockTask).isCompactionAllowed();
    return mockTask;
  }

  private AbstractCompactionTask prepareTask(long memCost, int fileNum)
      throws IOException, MetadataException, WriteProcessException {
    return prepareTask(memCost, fileNum, 0);
  }

  private void releaseTaskOccupiedResources(AbstractCompactionTask task) {
    SystemInfo.getInstance()
        .resetCompactionMemoryCost(task.getCompactionTaskType(), task.getEstimatedMemoryCost());
    SystemInfo.getInstance().decreaseCompactionFileNumCost(task.getProcessedFileNum());
  }
}
