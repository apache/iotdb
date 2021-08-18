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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** CompactionMergeTaskPoolManager provides a ThreadPool to queue and run all compaction tasks. */
public class CompactionTaskManager implements IService {
  private static final Logger logger = LoggerFactory.getLogger(CompactionTaskManager.class);
  private static final CompactionTaskManager INSTANCE = new CompactionTaskManager();
  private ScheduledThreadPoolExecutor pool;
  public static volatile AtomicInteger currentTaskNum = new AtomicInteger(0);
  // TODO: record the task in time partition
  private Map<String, Set<Future<Void>>> storageGroupTasks = new ConcurrentHashMap<>();
  private Map<String, Map<Long, Set<Future<Void>>>> compactionTaskFutures =
      new ConcurrentHashMap<>();

  public static CompactionTaskManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    if (pool == null) {
      this.pool =
          (ScheduledThreadPoolExecutor)
              IoTDBThreadPoolFactory.newScheduledThreadPool(
                  IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread(),
                  ThreadName.COMPACTION_SERVICE.getName());
      currentTaskNum = new AtomicInteger(0);
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
      while (pool.getActiveCount() > 0 || pool.getQueue().size() > 0) {
        // wait
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          logger.error("thread interrupted while waiting for compaction to end", e);
          return;
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

  public synchronized void submitTask(
      String fullStorageGroupName, long timePartition, Callable<Void> compactionMergeTask)
      throws RejectedExecutionException {
    if (pool != null && !pool.isTerminated()) {
      logger.info("submitted a compaction task, currentTaskNum={}", getTaskCount());
      Future<Void> future = pool.submit(compactionMergeTask);
      CompactionScheduler.addPartitionCompaction(fullStorageGroupName, timePartition);
      compactionTaskFutures
          .computeIfAbsent(fullStorageGroupName, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(timePartition, k -> new HashSet<>())
          .add(future);
      logger.warn("A CompactionTask is submitted to CompactionTaskManager");
      return;
    }
    logger.warn(
        "A CompactionTask failed to be submitted to CompactionTaskManager because {}",
        pool == null ? "pool is null" : "pool is terminated");
  }

  /**
   * Abort all compactions of a storage group. The caller must acquire the write lock of the
   * corresponding storage group.
   */
  public void abortCompaction(String fullStorageGroupName) {
    Set<Future<Void>> subTasks =
        storageGroupTasks.getOrDefault(fullStorageGroupName, Collections.emptySet());
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

  public int getTaskCount() {
    return pool.getActiveCount() + pool.getQueue().size();
  }
}
