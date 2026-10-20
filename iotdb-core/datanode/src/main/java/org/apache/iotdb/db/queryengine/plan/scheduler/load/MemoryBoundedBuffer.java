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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileAbstractMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;

/**
 * LOAD memory budget: the memory budget of the pieces of one source TsFile. It is a sub-block of
 * the shared {@link LoadTsFileDataCacheMemoryBlock} the manager handed out, so every byte accounted
 * here is accounted on the shared block too, and {@link #clear()} is the last-chance cleanup that
 * gives the leftover accounting back to it.
 *
 * <p>The cap is {@code thriftMaxFrameSize >> 2}: a piece has to stay transmittable in one frame,
 * whatever the shared block has left. {@link #isMemoryEnough()} is the signal the pipeline polls
 * after every chunk, and when it turns false {@link PieceDispatcher} evicts the largest buffered
 * piece first.
 */
class MemoryBoundedBuffer {

  private static final long MAX_MEMORY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize() >> 2;

  private final LoadTsFileAbstractMemoryBlock subBlock;

  MemoryBoundedBuffer(final LoadTsFileDataCacheMemoryBlock block) {
    this.subBlock = block.createSubBlock(MAX_MEMORY_SIZE);
  }

  boolean isMemoryEnough() {
    return subBlock.hasEnoughMemory();
  }

  void add(final long memorySize) {
    subBlock.addMemoryUsage(memorySize);
  }

  void release(final long memorySize) {
    subBlock.reduceMemoryUsage(memorySize);
  }

  /** Returns all buffered accounting to the shared memory block; safe to call multiple times. */
  void clear() {
    subBlock.close();
  }
}
