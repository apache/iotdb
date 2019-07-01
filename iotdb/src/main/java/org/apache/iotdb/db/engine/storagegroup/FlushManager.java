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

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.iotdb.db.engine.pool.FlushPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlushManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlushManager.class);

  private ConcurrentLinkedDeque<UnsealedTsFileProcessor> unsealedTsFileProcessorQueue = new ConcurrentLinkedDeque<>();

  private FlushPoolManager flushPool = FlushPoolManager.getInstance();

  class FlushThread implements Runnable {

    @Override
    public void run() {
      UnsealedTsFileProcessor unsealedTsFileProcessor = unsealedTsFileProcessorQueue.poll();
      try {
        unsealedTsFileProcessor.flushOneMemTable();
      } catch (IOException e) {
        LOGGER.error("storage group {} flush one memtable meet error",
            unsealedTsFileProcessor.getStorageGroupName(), e);
        // TODO do sth
      }
      unsealedTsFileProcessor.setManagedByFlushManager(false);
      registerUnsealedTsFileProcessor(unsealedTsFileProcessor);
    }
  }

  /**
   * Add BufferWriteProcessor to asyncFlush manager
   */
  void registerUnsealedTsFileProcessor(UnsealedTsFileProcessor unsealedTsFileProcessor) {
    synchronized (unsealedTsFileProcessor) {
      if (!unsealedTsFileProcessor.isManagedByFlushManager() && unsealedTsFileProcessor.getFlushingMemTableSize() > 0) {
        LOGGER.info("storage group {} begin to submit a flush thread, flushing memtable size: {}",
            unsealedTsFileProcessor.getStorageGroupName(), unsealedTsFileProcessor.getFlushingMemTableSize());
        unsealedTsFileProcessorQueue.add(unsealedTsFileProcessor);
        unsealedTsFileProcessor.setManagedByFlushManager(true);
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
