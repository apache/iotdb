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
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.util.concurrent.ExecutorService;

public class MTreeFlushTaskManager {

  private static final String MTREE_FLUSH_THREAD_POOL_NAME = "MTree-flush-task";

  private volatile ExecutorService flushTaskExecutor;

  private MTreeFlushTaskManager() {}

  private static class MTreeFlushTaskManagerHolder {
    private static final MTreeFlushTaskManager INSTANCE = new MTreeFlushTaskManager();

    private MTreeFlushTaskManagerHolder() {}
  }

  public static MTreeFlushTaskManager getInstance() {
    return MTreeFlushTaskManagerHolder.INSTANCE;
  }

  public void init() {
    flushTaskExecutor =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            MTREE_FLUSH_THREAD_POOL_NAME,
            IoTDBDescriptor.getInstance().getConfig().getMaxSchemaFlushThreadNum());
  }

  public void clear() {
    if (flushTaskExecutor != null) {
      flushTaskExecutor.shutdownNow();
      while (!flushTaskExecutor.isTerminated()) ;
      flushTaskExecutor = null;
    }
  }

  public void submit(Runnable task) {
    flushTaskExecutor.submit(task);
  }
}
