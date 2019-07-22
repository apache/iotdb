/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.iotdb.db.engine.pool.FlushPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlushManager {

  private static final Logger logger = LoggerFactory.getLogger(FlushManager.class);

  private ConcurrentLinkedDeque<TsFileProcessor> tsFileProcessorQueue = new ConcurrentLinkedDeque<>();

  private FlushPoolManager flushPool = FlushPoolManager.getInstance();

  class FlushThread implements Runnable {

    @Override
    public void run() {
      TsFileProcessor tsFileProcessor = tsFileProcessorQueue.poll();
      tsFileProcessor.flushOneMemTable();
      tsFileProcessor.setManagedByFlushManager(false);
      registerTsFileProcessor(tsFileProcessor);
    }
  }

  /**
   * Add BufferWriteProcessor to asyncTryToFlush manager
   */
  @SuppressWarnings("squid:S2445")
  void registerTsFileProcessor(TsFileProcessor tsFileProcessor) {
    synchronized (tsFileProcessor) {
      if (!tsFileProcessor.isManagedByFlushManager() && tsFileProcessor.getFlushingMemTableSize() > 0) {
        logger.info("storage group {} begin to submit a flush thread, flushing memtable size: {}",
            tsFileProcessor.getStorageGroupName(), tsFileProcessor.getFlushingMemTableSize());
        tsFileProcessorQueue.add(tsFileProcessor);
        tsFileProcessor.setManagedByFlushManager(true);
        flushPool.submit(new FlushThread());
      }
    }
  }

  private FlushManager() {
  }

  public static FlushManager getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static FlushManager instance = new FlushManager();
  }

}
