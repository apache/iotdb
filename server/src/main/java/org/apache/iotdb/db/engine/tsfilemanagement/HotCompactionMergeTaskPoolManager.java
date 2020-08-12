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
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.tsfilemanagement.TsFileManagement.HotCompactionMergeTask;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HotCompactionMergeTaskPoolManager provides a ThreadPool to queue and run all hot compaction
 * tasks.
 */
public class HotCompactionMergeTaskPoolManager implements IService {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(HotCompactionMergeTaskPoolManager.class);
  private static final HotCompactionMergeTaskPoolManager INSTANCE = new HotCompactionMergeTaskPoolManager();
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private ExecutorService pool;

  public static HotCompactionMergeTaskPoolManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    JMXService.registerMBean(this, mbeanName);
    if (pool == null) {
      this.pool = IoTDBThreadPoolFactory
          .newCachedThreadPool(ThreadName.FLUSH_VM_SERVICE.getName());
    }
    LOGGER.info("Hot compaction merge task manager started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      pool.shutdownNow();
      LOGGER.info("Waiting for task pool to shut down");
      long startTime = System.currentTimeMillis();
      while (!pool.isTerminated()) {
        // wait
        long time = System.currentTimeMillis() - startTime;
        if (time % 60_000 == 0) {
          LOGGER.warn("HotCompactionManager has wait for {} seconds to stop", time / 1000);
        }
      }
      pool = null;
      LOGGER.info("HotCompactionManager stopped");
    }
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public void waitAndStop(long millseconds) {
    if (pool != null) {
      awaitTermination(pool, millseconds);
      LOGGER.info("Waiting for task pool to shut down");
      long startTime = System.currentTimeMillis();
      while (!pool.isTerminated()) {
        // wait
        long time = System.currentTimeMillis() - startTime;
        if (time % 60_000 == 0) {
          LOGGER.warn("HotCompactionManager has wait for {} seconds to stop", time / 1000);
        }
      }
      pool = null;
      LOGGER.info("HotCompactionManager stopped");
    }
  }

  private void awaitTermination(ExecutorService service, long millseconds) {
    try {
      service.shutdown();
      service.awaitTermination(millseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn("HotCompactionThreadPool can not be closed in {} ms", millseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.HOT_COMPACTION_SERVICE;
  }

  public void submitTask(HotCompactionMergeTask hotCompactionMergeTask) {
    pool.submit(hotCompactionMergeTask);
  }

}
