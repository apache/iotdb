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
package org.apache.iotdb.db.metadata.mtree.store.disk;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class MTreeLoadTaskManager {

  private static final Logger logger = LoggerFactory.getLogger(MTreeLoadTaskManager.class);
  private static final String MTREE_LOAD_THREAD_POOL_NAME = "MTree-load-task";

  private volatile ExecutorService loadTaskExecutor;

  private MTreeLoadTaskManager() {}

  private static class MTreeLoadTaskManagerHolder {
    private static final MTreeLoadTaskManager INSTANCE = new MTreeLoadTaskManager();

    private MTreeLoadTaskManagerHolder() {}
  }

  public static MTreeLoadTaskManager getInstance() {
    return MTreeLoadTaskManagerHolder.INSTANCE;
  }

  public void init() {
    loadTaskExecutor = IoTDBThreadPoolFactory.newCachedThreadPool(MTREE_LOAD_THREAD_POOL_NAME);
  }

  public void clear() {
    if (loadTaskExecutor != null) {
      try {
        loadTaskExecutor.shutdown();
        while (!loadTaskExecutor.isTerminated()) ;
        loadTaskExecutor = null;
      } catch (RuntimeException e) {
        logger.error("Something wrong happened during MTree recovery: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }

  public void submit(Runnable task) {
    loadTaskExecutor.submit(task);
  }
}
