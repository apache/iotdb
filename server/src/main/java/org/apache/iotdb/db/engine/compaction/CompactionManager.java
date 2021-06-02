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

package org.apache.iotdb.db.engine.compaction;

import static org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionLogger.COMPACTION_LOG_NAME;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CompactionTaskManager provides a ThreadPool to queue and run all compaction tasks. */
public class CompactionManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
  private static final CompactionManager INSTANCE = new CompactionManager();
  private ScheduledExecutorService pool;
  private Map<String, Set<Future<Void>>> storageGroupTasks = new ConcurrentHashMap<>();
  private final Set<StorageGroupProcessor> storageGroupProcessorSet = new ConcurrentSkipListSet<>();

  public static CompactionManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    if (pool == null) {
      this.pool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getCompactionThreadNum(),
              ThreadName.COMPACTION_SERVICE.getName());
    }
    logger.info("Compaction task manager started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      pool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (pool != null) {
      awaitTermination(pool, milliseconds);
      logger.info("Waiting for task pool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @TestOnly
  public void waitAllCompactionFinish() {
    if (pool != null) {
      File sgDir =
          FSFactoryProducer.getFSFactory()
              .getFile(
                  FilePathUtils.regularizePath(
                          IoTDBDescriptor.getInstance().getConfig().getSystemDir())
                      + "storage_groups");
      File[] subDirList = sgDir.listFiles();
      if (subDirList != null) {
        for (File subDir : subDirList) {
          while (FSFactoryProducer.getFSFactory()
              .getFile(
                  subDir.getAbsoluteFile()
                      + File.separator
                      + subDir.getName()
                      + COMPACTION_LOG_NAME)
              .exists()) {
            // wait
          }
        }
      }
      storageGroupTasks.clear();
      logger.info("All compaction task finish");
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    while (!pool.isTerminated()) {
      int timeMillis = 0;
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error(
            "CompactionMergeTaskPoolManager {} shutdown",
            ThreadName.COMPACTION_SERVICE.getName(),
            e);
        Thread.currentThread().interrupt();
      }
      timeMillis += 200;
      long time = System.currentTimeMillis() - startTime;
      if (timeMillis % 60_000 == 0) {
        logger.warn("CompactionManager has wait for {} seconds to stop", time / 1000);
      }
    }
    pool = null;
    storageGroupTasks.clear();
    logger.info("CompactionManager stopped");
  }

  private void awaitTermination(ExecutorService service, long milliseconds) {
    try {
      service.shutdown();
      service.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("CompactionThreadPool can not be closed in {} ms", milliseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.COMPACTION_SERVICE;
  }

  public void init(
      String storageGroupName,
      TsFileManagement tsFileManagement,
      StorageGroupProcessor storageGroupProcessor) {
    logger.info("{} submit a compaction recover task", storageGroupName);
    try {
      Callable<Void> compactionRecoverTask =
          new CompactionRecoverTask(tsFileManagement, this::closeCompactionMergeCallBack);
      Future<Void> future = pool.submit(compactionRecoverTask);
      storageGroupTasks
          .computeIfAbsent(storageGroupName, k -> new ConcurrentSkipListSet<>())
          .add(future);
    } catch (RejectedExecutionException e) {
      this.closeCompactionMergeCallBack(storageGroupName);
      logger.info("{} compaction submit task failed", storageGroupName, e);
    }
    storageGroupProcessorSet.add(storageGroupProcessor);
    pool.scheduleWithFixedDelay(
        () -> mergeAll(IoTDBDescriptor.getInstance().getConfig().isFullMerge()),
        30,
        0,
        TimeUnit.SECONDS);
  }

  public void mergeAll(boolean isFullMerge) {
    for (StorageGroupProcessor storageGroupProcessor : storageGroupProcessorSet) {
      storageGroupProcessor.writeLock();
      try {
        for (long timePartitionId :
            storageGroupProcessor.partitionLatestFlushedTimeForEachDevice.keySet()) {
          CompactionManager.getInstance()
              .startCompaction(
                  storageGroupProcessor.getLogicalStorageGroupName(),
                  timePartitionId,
                  isFullMerge,
                  storageGroupProcessor.tsFileManagement);
        }
      } finally {
        storageGroupProcessor.writeUnlock();
      }
    }
  }

  public void startCompaction(
      String storageGroupName,
      long timePartition,
      boolean fullMerge,
      TsFileManagement tsFileManagement) {
    if (!storageGroupTasks.containsKey(storageGroupName)
        || storageGroupTasks.get(storageGroupName).isEmpty()) {
      logger.info("{} submit a compaction task", storageGroupName);
      try {
        Callable<Void> compactionTask =
            new CompactionTask(
                tsFileManagement, timePartition, fullMerge, this::closeCompactionMergeCallBack);
        Future<Void> future = pool.submit(compactionTask);
        storageGroupTasks
            .computeIfAbsent(storageGroupName, k -> new ConcurrentSkipListSet<>())
            .add(future);
      } catch (RejectedExecutionException e) {
        this.closeCompactionMergeCallBack(storageGroupName);
        logger.info("{} compaction submit task failed", storageGroupName, e);
      }
    } else {
      logger.info("{} last compaction merge task is working, skip current merge", storageGroupName);
    }
  }

  /** close compaction merge callback, to release some locks */
  private void closeCompactionMergeCallBack(String storageGroupName) {
    this.storageGroupTasks.remove(storageGroupName);
  }

  /**
   * Abort all compactions of a storage group. The caller must acquire the write lock of the
   * corresponding storage group.
   */
  public void abortCompaction(String storageGroup) {
    Set<Future<Void>> subTasks =
        storageGroupTasks.getOrDefault(storageGroup, Collections.emptySet());
    Iterator<Future<Void>> subIterator = subTasks.iterator();
    while (subIterator.hasNext()) {
      Future<Void> next = subIterator.next();
      if (!next.isDone() && !next.isCancelled()) {
        next.cancel(true);
      }
      subIterator.remove();
    }
  }

  public boolean isTerminated() {
    return pool == null || pool.isTerminated();
  }
}
