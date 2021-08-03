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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;

/** CompactionMergeTaskPoolManager provides a ThreadPool to queue and run all compaction tasks. */
public class CompactionMergeTaskPoolManager implements IService {

  private static final Logger logger =
      LoggerFactory.getLogger(CompactionMergeTaskPoolManager.class);
  private static final CompactionMergeTaskPoolManager INSTANCE =
      new CompactionMergeTaskPoolManager();
  private ScheduledExecutorService scheduledPool;
  private ExecutorService pool;
  private Map<String, List<Future<Void>>> storageGroupTasks = new ConcurrentHashMap<>();
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static ConcurrentHashMap<String, Boolean> sgCompactionStatus = new ConcurrentHashMap<>();

  public static CompactionMergeTaskPoolManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    if (pool == null) {
      this.pool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getCompactionThreadNum(),
              ThreadName.COMPACTION_SERVICE.getName());
      this.scheduledPool =
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
      scheduledPool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (pool != null) {
      awaitTermination(scheduledPool, milliseconds);
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
    scheduledPool = null;
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

  /**
   * Abort all compactions of a storage group. The caller must acquire the write lock of the
   * corresponding storage group.
   */
  public void abortCompaction(String storageGroup) {
    List<Future<Void>> subTasks =
        storageGroupTasks.getOrDefault(storageGroup, Collections.emptyList());
    for (Future<Void> next : subTasks) {
      if (!next.isDone() && !next.isCancelled()) {
        next.cancel(true);
        sgCompactionStatus.put(storageGroup, false);
      }
    }
    subTasks.clear();
  }

  public synchronized void clearCompactionStatus(String storageGroupName) {
    // for test
    if (sgCompactionStatus == null) {
      sgCompactionStatus = new ConcurrentHashMap<>();
    }
    sgCompactionStatus.put(storageGroupName, false);
  }

  public void init(Runnable function) {
    scheduledPool.scheduleWithFixedDelay(
        function, 1000, config.getCompactionInterval(), TimeUnit.MILLISECONDS);
  }

  public synchronized void submitTask(StorageGroupCompactionTask storageGroupCompactionTask)
      throws RejectedExecutionException {
    if (pool != null && !pool.isTerminated()) {
      String storageGroup = storageGroupCompactionTask.getStorageGroupName();
      boolean isCompacting = sgCompactionStatus.computeIfAbsent(storageGroup, k -> false);
      if (isCompacting) {
        return;
      }
      storageGroupCompactionTask.setSgCompactionStatus(sgCompactionStatus);
      sgCompactionStatus.put(storageGroup, true);
      Future<Void> future = pool.submit(storageGroupCompactionTask);
      storageGroupTasks
          .computeIfAbsent(storageGroup, k -> new CopyOnWriteArrayList<>())
          .add(future);
    }
  }

  public boolean isTerminated() {
    return pool == null || pool.isTerminated();
  }
}
