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
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import org.apache.iotdb.db.engine.pool.FlushPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlushManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlushManager.class);

  private ConcurrentLinkedDeque<UnsealedTsFileProcessorV2> unsealedTsFileProcessorQueue = new ConcurrentLinkedDeque<>();

  private FlushPoolManager flushPool = FlushPoolManager.getInstance();

  class FlushThread implements Runnable {

    @Override
    public void run() {
      UnsealedTsFileProcessorV2 unsealedTsFileProcessor = unsealedTsFileProcessorQueue.poll();
      long startTime = System.currentTimeMillis();
      try {
        unsealedTsFileProcessor.flushOneMemTable();
      } catch (IOException e) {
        LOGGER.error("flush one memtable meet error", e);
        // TODO do sth
      }
      unsealedTsFileProcessor.setManagedByFlushManager(false);
      LOGGER.info("flush process consume {} ms", System.currentTimeMillis() - startTime);
      registerUnsealedTsFileProcessor(unsealedTsFileProcessor);
    }
  }

  /**
   * Add BufferWriteProcessor to asyncFlush manager
   */
  public Future registerUnsealedTsFileProcessor(UnsealedTsFileProcessorV2 unsealedTsFileProcessor) {
    synchronized (unsealedTsFileProcessor) {
      if (!unsealedTsFileProcessor.isManagedByFlushManager() && unsealedTsFileProcessor.getFlushingMemTableSize() > 0) {
        LOGGER.info("begin to submit a flush thread, flushing memtable getTotalDataNumber: {}", unsealedTsFileProcessor.getFlushingMemTableSize());
        unsealedTsFileProcessorQueue.add(unsealedTsFileProcessor);
        unsealedTsFileProcessor.setManagedByFlushManager(true);
        return flushPool.submit(new FlushThread());
      }
      return null;
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
