/**
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

package org.apache.iotdb.db.engine.merge;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(MergeManager.class);
  private static final MergeManager INSTANCE = new MergeManager();

  private AtomicInteger threadNum = new AtomicInteger();
  private ThreadPoolExecutor mergeTaskPool;
  private ScheduledExecutorService timedMergeThreadPool;

  private MergeManager() {
    start();
  }

  public static MergeManager getINSTANCE() {
    return INSTANCE;
  }

  public void submit(MergeTask mergeTask) {
    mergeTaskPool.submit(mergeTask);
  }

  @Override
  public void start() {
    if (mergeTaskPool == null) {
      mergeTaskPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getMergeConcurrentThreads(),
              r -> new Thread(r, "mergeThread-" + threadNum.getAndIncrement()));
      timedMergeThreadPool = (ScheduledExecutorService) Executors.newSingleThreadExecutor();
      timedMergeThreadPool.scheduleAtFixedRate(this::flushAll, 0,
          IoTDBDescriptor.getInstance().getConfig().getMergeIntervalSec(), TimeUnit.SECONDS);
    }
    logger.info("MergeManager started");
  }

  @Override
  public void stop() {
    if (mergeTaskPool != null) {
      timedMergeThreadPool.shutdownNow();
      mergeTaskPool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      while (!mergeTaskPool.isShutdown()) {
        // wait
      }
      mergeTaskPool = null;
      timedMergeThreadPool = null;
    }
    logger.info("MergeManager stopped");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MERGE_SERVICE;
  }

  private void flushAll() {
    try {
      StorageEngine.getInstance().mergeAll();
    } catch (StorageEngineException e) {
      logger.error("Cannot perform a global merge because", e);
    }
  }
}
