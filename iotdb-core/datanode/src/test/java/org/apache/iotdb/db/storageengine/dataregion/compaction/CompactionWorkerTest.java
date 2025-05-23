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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskQueue;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionWorker;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionWorkerTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().getCompactionMemoryBlock().setUsedMemoryInBytes(0);
  }

  @After
  public void teardown() {
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().getCompactionMemoryBlock().setUsedMemoryInBytes(0);
  }

  @Test
  public void testFailedToAllocateMemoryInCrossTask() throws Exception {
    List<TsFileResource> sequenceFiles = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      sequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)),
              TsFileResourceStatus.COMPACTION_CANDIDATE));
    }
    List<TsFileResource> unsequenceFiles = new ArrayList<>();
    for (int i = 11; i <= 20; i++) {
      unsequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)),
              TsFileResourceStatus.COMPACTION_CANDIDATE));
    }
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(tsFileManager.getDatabaseName()).thenReturn("root.sg");
    Mockito.when(tsFileManager.getDataRegionId()).thenReturn("1");
    Mockito.when(tsFileManager.isAllowCompaction()).thenReturn(true);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0L,
            tsFileManager,
            sequenceFiles,
            unsequenceFiles,
            null,
            1024L * 1024L * 1024L * 50L,
            0);
    CrossSpaceCompactionTask taskMock = Mockito.spy(task);
    Mockito.doReturn(true).when(taskMock).start();
    FixedPriorityBlockingQueue<AbstractCompactionTask> queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(taskMock);
    Thread thread =
        new Thread(
            () -> {
              try {
                queue.take();
                Assert.fail();
              } catch (InterruptedException ignored) {
              }
            });
    thread.start();
    thread.join(TimeUnit.SECONDS.toMillis(2));
    Assert.assertEquals(
        0, SystemInfo.getInstance().getCompactionMemoryBlock().getUsedMemoryInBytes());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
    for (TsFileResource tsFileResource : sequenceFiles) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
      Assert.assertTrue(tsFileResource.tryWriteLock());
    }
    for (TsFileResource tsFileResource : unsequenceFiles) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
      Assert.assertTrue(tsFileResource.tryWriteLock());
    }
    thread.interrupt();
    thread.join();
  }

  @Test
  public void testFailedToAllocateFileNumInCrossTask() throws InterruptedException {
    int oldMaxCrossCompactionCandidateFileNum =
        SystemInfo.getInstance().getTotalFileLimitForCompaction();
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(2);
    try {
      List<TsFileResource> sequenceFiles = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        sequenceFiles.add(
            new TsFileResource(
                new File(String.format("%d-%d-0-0.tsfile", i, i)),
                TsFileResourceStatus.COMPACTION_CANDIDATE));
      }
      List<TsFileResource> unsequenceFiles = new ArrayList<>();
      for (int i = 11; i <= 30; i++) {
        unsequenceFiles.add(
            new TsFileResource(
                new File(String.format("%d-%d-0-0.tsfile", i, i)),
                TsFileResourceStatus.COMPACTION_CANDIDATE));
      }

      TsFileManager tsFileManager = new TsFileManager("root.testsg", "0", "");
      tsFileManager.addAll(sequenceFiles, true);
      tsFileManager.addAll(unsequenceFiles, false);
      CrossSpaceCompactionTask task =
          new CrossSpaceCompactionTask(
              0L, tsFileManager, sequenceFiles, unsequenceFiles, null, 1000, 0);
      CrossSpaceCompactionTask taskMock = Mockito.spy(task);
      Mockito.doReturn(true).when(taskMock).start();
      FixedPriorityBlockingQueue<AbstractCompactionTask> queue =
          new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
      queue.put(taskMock);
      Thread thread =
          new Thread(
              () -> {
                try {
                  queue.take();
                } catch (InterruptedException ignored) {
                }
              });
      thread.start();
      thread.join(TimeUnit.SECONDS.toMillis(2));
      Assert.assertEquals(
          0, SystemInfo.getInstance().getCompactionMemoryBlock().getUsedMemoryInBytes());
      Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
      for (TsFileResource tsFileResource : sequenceFiles) {
        Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
        Assert.assertTrue(tsFileResource.tryWriteLock());
      }
      for (TsFileResource tsFileResource : unsequenceFiles) {
        Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
        Assert.assertTrue(tsFileResource.tryWriteLock());
      }
      thread.interrupt();
      thread.join();
    } finally {
      SystemInfo.getInstance()
          .setTotalFileLimitForCompactionTask(oldMaxCrossCompactionCandidateFileNum);
    }
  }

  /** AllowCompaction is false. */
  @Test
  public void testFailedToCheckValidInCrossTask() throws InterruptedException {
    List<TsFileResource> sequenceFiles = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      sequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)),
              TsFileResourceStatus.COMPACTION_CANDIDATE));
    }
    List<TsFileResource> unsequenceFiles = new ArrayList<>();
    for (int i = 11; i <= 20; i++) {
      unsequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)),
              TsFileResourceStatus.COMPACTION_CANDIDATE));
    }
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(tsFileManager.isAllowCompaction()).thenReturn(false);
    // fail to check valid when tsfile manager is not allowed to compaction in cross task
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0L, tsFileManager, sequenceFiles, unsequenceFiles, null, 1000, 0);
    FixedPriorityBlockingQueue<AbstractCompactionTask> queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(task);
    Thread thread =
        new Thread(
            () -> {
              try {
                queue.take();
              } catch (InterruptedException ignored) {
              }
            });
    thread.start();
    thread.join(TimeUnit.SECONDS.toMillis(2));
    Assert.assertEquals(
        0, SystemInfo.getInstance().getCompactionMemoryBlock().getUsedMemoryInBytes());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
    for (TsFileResource tsFileResource : sequenceFiles) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
      Assert.assertTrue(tsFileResource.tryWriteLock());
    }
    thread.interrupt();
    thread.join();
  }

  /** AllowCompaction is false. */
  @Test
  public void testFailedToCheckValidInInnerTask() throws InterruptedException {
    List<TsFileResource> sequenceFiles = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      sequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)),
              TsFileResourceStatus.COMPACTION_CANDIDATE));
    }
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(tsFileManager.isAllowCompaction()).thenReturn(false);

    // fail to check valid when tsfile manager is not allowed to compaction in inner task
    InnerSpaceCompactionTask innerTask =
        new InnerSpaceCompactionTask(0L, tsFileManager, sequenceFiles, true, null, 0L);
    FixedPriorityBlockingQueue<AbstractCompactionTask> queue =
        new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
    queue.put(innerTask);
    Thread thread =
        new Thread(
            () -> {
              try {
                queue.take();
              } catch (InterruptedException ignored) {
              }
            });
    thread.start();
    thread.join(TimeUnit.SECONDS.toMillis(2));
    Assert.assertEquals(
        0, SystemInfo.getInstance().getCompactionMemoryBlock().getUsedMemoryInBytes());
    Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
    for (TsFileResource tsFileResource : sequenceFiles) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
      Assert.assertTrue(tsFileResource.tryWriteLock());
    }
    thread.interrupt();
    thread.join();
  }

  @Test
  public void testAbortCompactionTask() {
    AtomicReference<CompactionWorker.CompactionTaskFuture> compactionTaskSummary =
        new AtomicReference<>();
    AtomicBoolean isInterrupted = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    Phaser phaser = new Phaser(2);
    Thread t =
        new Thread(
            () -> {
              compactionTaskSummary.set(
                  new CompactionWorker.CompactionTaskFuture(new CompactionTaskSummary()));
              phaser.arriveAndAwaitAdvance();
              try {
                latch.await();
              } catch (InterruptedException ignored) {
                isInterrupted.set(true);
              }
              phaser.arriveAndAwaitAdvance();
            });
    t.start();
    phaser.arriveAndAwaitAdvance();
    compactionTaskSummary.get().cancel(true);
    phaser.arriveAndAwaitAdvance();
    Assert.assertTrue(isInterrupted.get());
  }
}
