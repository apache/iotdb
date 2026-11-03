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
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ActiveLoadTsFileLoaderTest {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private File tempDir;
  private String originalFailDir;
  private NodeStatus originalNodeStatus;
  private int originalDataNodeId;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("active-load-retry").toFile();
    originalFailDir = config.getLoadActiveListeningFailDir();
    originalNodeStatus = CommonDescriptor.getInstance().getConfig().getNodeStatus();
    originalDataNodeId = config.getDataNodeId();
    config.setDataNodeId(1);
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Running);
    config.setLoadActiveListeningFailDir(new File(tempDir, "failed").getAbsolutePath());
  }

  @After
  public void tearDown() {
    config.setLoadActiveListeningFailDir(originalFailDir);
    CommonDescriptor.getInstance().getConfig().setNodeStatus(originalNodeStatus);
    config.setDataNodeId(originalDataNodeId);
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

  @Test
  public void testPendingEntryRetainsConversionTaskId() {
    final ActiveLoadPendingQueue pendingQueue = new ActiveLoadPendingQueue();
    final String tsFilePath = new File(tempDir, "task.tsfile").getAbsolutePath();
    Assert.assertTrue(
        pendingQueue.enqueue(
            tsFilePath, tempDir.getAbsolutePath(), true, false, "conversion-task"));

    final ActiveLoadPendingQueue.ActiveLoadEntry entry = pendingQueue.dequeueFromPending();
    Assert.assertNotNull(entry);
    Assert.assertEquals("conversion-task", entry.getConversionTaskId());
    pendingQueue.removeFromLoading(tsFilePath);
  }

  @Test
  public void testFileQueuedDuringLastEmptyPollIsProcessed() throws Exception {
    assertLastEmptyPollHandoff(false);
  }

  @Test
  public void testStopDoesNotRestartRetiringWorker() throws Exception {
    assertLastEmptyPollHandoff(true);
  }

  private void assertLastEmptyPollHandoff(final boolean stop) throws Exception {
    final long originalInterval = config.getLoadActiveListeningCheckIntervalSeconds();
    final int originalMaxThreads = config.getLoadActiveListeningMaxThreadNum();
    final ActiveLoadTsFileLoader loader = new ActiveLoadTsFileLoader();
    final CountDownLatch lastEmptyPoll = new CountDownLatch(1);
    final CountDownLatch releasePoll = new CountDownLatch(1);
    final AtomicInteger emptyPolls = new AtomicInteger();
    final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    final WrappedThreadPoolExecutor executor =
        new WrappedThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory("active-load-retirement-test"),
            "active-load-retirement-test") {
          @Override
          protected void afterExecute(final Runnable task, final Throwable failure) {
            if (failure != null) {
              workerFailure.compareAndSet(null, failure);
            }
            super.afterExecute(task, failure);
          }
        };
    executorReference(loader).set(executor);
    final ActiveLoadPendingQueue pendingQueue =
        new ActiveLoadPendingQueue() {
          @Override
          public ActiveLoadEntry dequeueFromPending() {
            final ActiveLoadEntry entry = super.dequeueFromPending();
            // Interval zero gives two empty polls. Pause after the last dequeue has released
            // the queue lock, so enqueue races with retirement rather than an ordinary poll.
            if (entry == null && emptyPolls.incrementAndGet() == 2) {
              lastEmptyPoll.countDown();
              try {
                Assert.assertTrue(releasePoll.await(30, TimeUnit.SECONDS));
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            return entry;
          }
        };
    final Field queueField = ActiveLoadTsFileLoader.class.getDeclaredField("pendingQueue");
    queueField.setAccessible(true);
    queueField.set(loader, pendingQueue);
    config.setLoadActiveListeningCheckIntervalSeconds(0);
    config.setLoadActiveListeningMaxThreadNum(1);
    try {
      // Missing files exercise scheduling and failure cleanup without starting a database.
      loader.tryTriggerTsFileLoad(
          new File(tempDir, "first.tsfile").getAbsolutePath(),
          tempDir.getAbsolutePath(),
          false,
          false);
      Assert.assertTrue(lastEmptyPoll.await(30, TimeUnit.SECONDS));
      Assert.assertEquals(1, executor.getActiveCount());
      final File next = new File(tempDir, "next.tsfile");
      loader.tryTriggerTsFileLoad(next.getAbsolutePath(), tempDir.getAbsolutePath(), false, false);
      Assert.assertTrue(loader.isFilePendingOrLoading(next));
      if (stop) {
        loader.stop();
        Assert.assertNull(executorReference(loader).get());
        Assert.assertTrue(executor.isTerminated());
        Assert.assertTrue(pendingQueue.isEmpty());
      } else {
        releasePoll.countDown();
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> Assert.assertFalse(loader.isFilePendingOrLoading(next)));
        Assert.assertTrue(pendingQueue.isEmpty());
      }
    } finally {
      releasePoll.countDown();
      loader.stop();
      config.setLoadActiveListeningCheckIntervalSeconds(originalInterval);
      config.setLoadActiveListeningMaxThreadNum(originalMaxThreads);
      Assert.assertNull(String.valueOf(workerFailure.get()), workerFailure.get());
    }
  }

  @Test
  public void testConcurrentEnqueuesCountTasksThatHaveNotStarted() throws Exception {
    final int originalMaxThreads = config.getLoadActiveListeningMaxThreadNum();
    final ActiveLoadTsFileLoader loader = new ActiveLoadTsFileLoader();
    final WrappedThreadPoolExecutor executor = mock(WrappedThreadPoolExecutor.class);
    // Submitted tasks remain queued: getActiveCount() alone would schedule too many workers.
    when(executor.getActiveCount()).thenReturn(0);
    when(executor.getCorePoolSize()).thenReturn(2);
    when(executor.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);
    executorReference(loader).set(executor);
    config.setLoadActiveListeningMaxThreadNum(2);
    final ExecutorService producers = Executors.newFixedThreadPool(4);
    try {
      final List<Future<?>> enqueues = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        final File source = new File(tempDir, "queued-" + i + ".tsfile");
        enqueues.add(
            producers.submit(
                () ->
                    loader.tryTriggerTsFileLoad(
                        source.getAbsolutePath(), tempDir.getAbsolutePath(), false, false)));
      }
      for (final Future<?> enqueue : enqueues) {
        enqueue.get(10, TimeUnit.SECONDS);
      }
      verify(executor, times(2)).execute(any(Runnable.class));
    } finally {
      producers.shutdownNow();
      loader.stop();
      config.setLoadActiveListeningMaxThreadNum(originalMaxThreads);
      Assert.assertTrue(producers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @SuppressWarnings("unchecked")
  private AtomicReference<WrappedThreadPoolExecutor> executorReference(
      final ActiveLoadTsFileLoader loader) throws Exception {
    final Field field = ActiveLoadTsFileLoader.class.getDeclaredField("activeLoadExecutor");
    field.setAccessible(true);
    return (AtomicReference<WrappedThreadPoolExecutor>) field.get(loader);
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
