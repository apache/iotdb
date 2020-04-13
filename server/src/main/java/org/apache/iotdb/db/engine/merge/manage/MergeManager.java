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

package org.apache.iotdb.db.engine.merge.manage;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeManager provides a ThreadPool to queue and run all merge tasks to restrain the total
 * resources occupied by merge and manages a Timer to periodically issue a global merge.
 */
public class MergeManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(MergeManager.class);
  private static final MergeManager INSTANCE = new MergeManager();

  private AtomicInteger threadCnt = new AtomicInteger();
  private ThreadPoolExecutor mergeTaskPool;
  private ThreadPoolExecutor mergeChunkSubTaskPool;
  private ScheduledExecutorService timedMergeThreadPool;

  private MergeManager() {
  }

  public static MergeManager getINSTANCE() {
    return INSTANCE;
  }

  public void submitMainTask(MergeTask mergeTask) {
    mergeTaskPool.submit(mergeTask);
  }

  public Future submitChunkSubTask(Callable callable) {
    return mergeChunkSubTaskPool.submit(callable);
  }

  @Override
  public void start() {
    if (mergeTaskPool == null) {
      int threadNum = IoTDBDescriptor.getInstance().getConfig().getMergeThreadNum();
      if (threadNum <= 0) {
        threadNum = 1;
      }

      int chunkSubThreadNum = IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
      if (chunkSubThreadNum <= 0) {
        chunkSubThreadNum = 1;
      }

      mergeTaskPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum,
              r -> new Thread(r, "MergeThread-" + threadCnt.getAndIncrement()));
      mergeChunkSubTaskPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum * chunkSubThreadNum,
              r -> new Thread(r, "MergeChunkSubThread-" + threadCnt.getAndIncrement()));
      long mergeInterval = IoTDBDescriptor.getInstance().getConfig().getMergeIntervalSec();
      if (mergeInterval > 0) {
        timedMergeThreadPool = Executors.newSingleThreadScheduledExecutor( r -> new Thread(r,
            "TimedMergeThread"));
        timedMergeThreadPool.scheduleAtFixedRate(this::mergeAll, mergeInterval,
            mergeInterval, TimeUnit.SECONDS);
      }
      logger.info("MergeManager started");
    }
  }

  @Override
  public void stop() {
    if (mergeTaskPool != null) {
      if (timedMergeThreadPool != null) {
        timedMergeThreadPool.shutdownNow();
        timedMergeThreadPool = null;
      }
      mergeTaskPool.shutdownNow();
      mergeChunkSubTaskPool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      long startTime = System.currentTimeMillis();
      while (!mergeTaskPool.isTerminated() || !mergeChunkSubTaskPool.isTerminated() ) {
        // wait
        long time = System.currentTimeMillis() - startTime;
        if (time % 60_000 == 0) {
          logger.warn("MergeManager has wait for {} seconds to stop", time/1000);
        }
      }
      mergeTaskPool = null;
      logger.info("MergeManager stopped");
    }
  }

  @Override
  public void waitAndStop(long millseconds) {
    if (mergeTaskPool != null) {
      if (timedMergeThreadPool != null) {
        awaitTermination(timedMergeThreadPool, millseconds);
        timedMergeThreadPool = null;
      }
      awaitTermination(mergeTaskPool, millseconds);
      awaitTermination(mergeChunkSubTaskPool, millseconds);
      logger.info("Waiting for task pool to shut down");
      long startTime = System.currentTimeMillis();
      while (!mergeTaskPool.isTerminated() || !mergeChunkSubTaskPool.isTerminated() ) {
        // wait
        long time = System.currentTimeMillis() - startTime;
        if (time % 60_000 == 0) {
          logger.warn("MergeManager has wait for {} seconds to stop", time/1000);
        }
      }
      mergeTaskPool = null;
      logger.info("MergeManager stopped");
    }
  }

  private void awaitTermination(ExecutorService service, long millseconds) {
    try {
      service.shutdown();
      service.awaitTermination(millseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("MergeThreadPool can not be closed in {} ms", millseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MERGE_SERVICE;
  }

  private void mergeAll() {
    try {
      StorageEngine.getInstance().mergeAll(IoTDBDescriptor.getInstance().getConfig().isForceFullMerge());
    } catch (StorageEngineException e) {
      logger.error("Cannot perform a global merge because", e);
    }
  }
}
