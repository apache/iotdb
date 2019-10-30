/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.rescon.MemTablePool;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each flush task allocates new {@linkplain ChunkBuffer} which might be very large and lead to
 * high-cost GC. In new design, we try to reuse ChunkBuffer objects by ChunkBufferPool, referring to
 * {@linkplain MemTablePool}.
 */
@Deprecated
public class ChunkBufferPool {

  private static final Logger logger = LoggerFactory.getLogger(ChunkBufferPool.class);

  private static final Deque<ChunkBuffer> availableChunkBuffer = new ArrayDeque<>();

  private long size = 0;

  private static final int WAIT_TIME = 2000;

  private ChunkBufferPool() {
  }

  public ChunkBuffer getEmptyChunkBuffer(Object applier, MeasurementSchema schema) {
    if (!IoTDBDescriptor.getInstance().getConfig()
        .isChunkBufferPoolEnable()) {
     return new ChunkBuffer(schema);
    }

    synchronized (availableChunkBuffer) {
      //we use the memtable number * maximal series number in one StroageGroup * 2 as the capacity
      long capacity  =
          2 * MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups() * IoTDBDescriptor
              .getInstance().getConfig().getMaxMemtableNumber() + 100000;
      if (availableChunkBuffer.isEmpty() && size < capacity) {
        size++;
        return new ChunkBuffer(schema);
      } else if (!availableChunkBuffer.isEmpty()) {
        ChunkBuffer chunkBuffer = availableChunkBuffer.pop();
        chunkBuffer.reInit(schema);
        return chunkBuffer;
      }

      // wait until some one has released a ChunkBuffer
      int waitCount = 1;
      while (true) {
        if (!availableChunkBuffer.isEmpty()) {
          return availableChunkBuffer.pop();
        }
        try {
          availableChunkBuffer.wait(WAIT_TIME);
        } catch (InterruptedException e) {
          logger.error("{} fails to wait fot ReusableChunkBuffer {}, continue to wait", applier, e);
          Thread.currentThread().interrupt();
        }
        logger.info("{} has waited for a ReusableChunkBuffer for {}ms", applier,
            waitCount++ * WAIT_TIME);
      }
    }
  }

  public void putBack(ChunkBuffer chunkBuffer) {
    if (!IoTDBDescriptor.getInstance().getConfig()
        .isChunkBufferPoolEnable()) {
      return;
    }

    synchronized (availableChunkBuffer) {
      chunkBuffer.reset();
      //we use the memtable number * maximal series number in one StroageGroup as the capacity
      long capacity  =
          MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups() * IoTDBDescriptor
              .getInstance().getConfig().getMaxMemtableNumber();
      if (size > capacity) {
        size --;
      } else {
        availableChunkBuffer.push(chunkBuffer);
      }
      availableChunkBuffer.notify();
    }
  }

  public static ChunkBufferPool getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final ChunkBufferPool INSTANCE = new ChunkBufferPool();
  }
}
