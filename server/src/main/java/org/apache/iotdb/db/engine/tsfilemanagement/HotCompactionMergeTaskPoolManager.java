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

package org.apache.iotdb.db.engine.tsfilemanagement;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.tsfilemanagement.TsFileManagement.HotCompactionMergeTask;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HotCompactionMergeTaskPoolManager provides a ThreadPool to queue and run all hot compaction
 * tasks.
 */
public class HotCompactionMergeTaskPoolManager implements IService {

  private static final Logger logger = LoggerFactory
      .getLogger(HotCompactionMergeTaskPoolManager.class);
  private static final HotCompactionMergeTaskPoolManager INSTANCE = new HotCompactionMergeTaskPoolManager();
  private ExecutorService pool;

  public static HotCompactionMergeTaskPoolManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    if (pool == null) {
      this.pool = IoTDBThreadPoolFactory
          .newScheduledThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getHotCompactionThreadNum(),
              ThreadName.HOT_COMPACTION_SERVICE.getName());
    }
    logger.info("Hot compaction merge task manager started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      pool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      waitTermination();
    }
  }

  @Override
  public void waitAndStop(long millseconds) {
    if (pool != null) {
      awaitTermination(pool, millseconds);
      logger.info("Waiting for task pool to shut down");
      waitTermination();
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    while (!pool.isTerminated()) {
      // wait
      long time = System.currentTimeMillis() - startTime;
      if (time % 60_000 == 0) {
        logger.warn("HotCompactionManager has wait for {} seconds to stop", time / 1000);
      }
    }
    pool = null;
    logger.info("HotCompactionManager stopped");
  }

  private void awaitTermination(ExecutorService service, long millseconds) {
    try {
      service.shutdown();
      service.awaitTermination(millseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("HotCompactionThreadPool can not be closed in {} ms", millseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.HOT_COMPACTION_SERVICE;
  }

  public void submitTask(HotCompactionMergeTask hotCompactionMergeTask)
      throws RejectedExecutionException {
    if (pool != null && !pool.isTerminated()) {
      pool.submit(hotCompactionMergeTask);
    }
  }

  public boolean isTerminated() {
    return pool == null || pool.isTerminated();
  }

}
