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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.CachedMTreeStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class Scheduler {

  /** configuration */
  private int BATCH_FLUSH_SUBTREE = 50;
  private int FLUSH_WORKER_NUM = 10;

  /** data structure */
  private final Map<Integer, CachedMTreeStore> regionToStore;

  private final ExecutorService workerPool;

  public Scheduler(Map<Integer, CachedMTreeStore> regionToStore){
    this.regionToStore = regionToStore;
    workerPool =
            IoTDBThreadPoolFactory.newFixedThreadPool(
                    FLUSH_WORKER_NUM, ThreadName.PBTREE_FLUSH_PROCESSOR.getName(), new ThreadPoolExecutor.AbortPolicy());
  }

  public void forceFlushAll() {
    // TODO
  }

  public void scheduleFlush(List<Integer> regionIds) {
    for(int regionId:regionIds){
      workerPool.submit(() -> {
        CachedMTreeStore store = regionToStore.get(regionId);
        store.flushVolatileNodes();
        // todo: executeMemoryRelease
//        while (isExceedReleaseThreshold()) {
//          // store try to release memory if not exceed release threshold
//          if (store.executeMemoryRelease()) {
//            // if store can not release memory, break
//            break;
//          }
//        }
      });
    }
  }

  public int getFlushThreadNum(){
    return ((WrappedThreadPoolExecutor) workerPool).getActiveCount();
  }
}
