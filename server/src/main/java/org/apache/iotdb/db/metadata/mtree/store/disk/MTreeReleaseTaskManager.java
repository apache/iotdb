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

import java.util.concurrent.ExecutorService;

public class MTreeReleaseTaskManager {

  private static final String MTREE_RELEASE_THREAD_POOL_NAME = "MTree-release-task";

  private ExecutorService releaseTaskExecutor;

  private MTreeReleaseTaskManager() {}

  private static class MTreeReleaseTaskManagerHolder {
    private static final MTreeReleaseTaskManager INSTANCE = new MTreeReleaseTaskManager();

    private MTreeReleaseTaskManagerHolder() {}
  }

  public static MTreeReleaseTaskManager getInstance() {
    return MTreeReleaseTaskManager.MTreeReleaseTaskManagerHolder.INSTANCE;
  }

  public void init() {
    releaseTaskExecutor =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            MTREE_RELEASE_THREAD_POOL_NAME, Runtime.getRuntime().availableProcessors());
  }

  public void clear() {
    if (releaseTaskExecutor != null) {
      releaseTaskExecutor.shutdown();
      while (!releaseTaskExecutor.isTerminated()) ;
      releaseTaskExecutor = null;
    }
  }

  public void submit(Runnable task) {
    releaseTaskExecutor.submit(task);
  }
}
