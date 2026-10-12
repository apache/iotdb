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
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;

/**
 * LOAD memory budget: memory-pool accounting for the pieces of one source TsFile. It keeps two
 * views in sync - its own {@code dataSize} and the shared {@link LoadTsFileDataCacheMemoryBlock} -
 * so the cluster-wide LOAD cache is aware of this file's footprint.
 *
 * <p>The budget is {@code thriftMaxFrameSize >> 2}; {@link #isMemoryEnough()} is the signal the
 * pipeline polls after every chunk, and when it turns false {@link PieceDispatcher} evicts the
 * largest buffered piece first. {@link #add(long)} / {@link #release(long)} keep both views
 * consistent, and {@link #clear()} returns any leftover accounting to the shared block.
 *
 * <p>Not a sub-block of the shared block: the manager hands out the block itself, and the shared
 * limit answers a different question than the per-file budget that keeps pieces sliceable.
 */
class MemoryBoundedBuffer {

  private static final long MAX_MEMORY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize() >> 2;

  private final LoadTsFileDataCacheMemoryBlock block;
  private long dataSize = 0;

  MemoryBoundedBuffer(LoadTsFileDataCacheMemoryBlock block) {
    this.block = block;
  }

  boolean isMemoryEnough() {
    return dataSize <= MAX_MEMORY_SIZE && block.hasEnoughMemory();
  }

  void add(long memorySize) {
    dataSize += memorySize;
    block.addMemoryUsage(memorySize);
  }

  void release(long memorySize) {
    dataSize -= memorySize;
    block.reduceMemoryUsage(memorySize);
  }

  long getDataSize() {
    return dataSize;
  }

  /** Returns all buffered accounting to the shared memory block; safe to call multiple times. */
  void clear() {
    if (dataSize > 0) {
      block.reduceMemoryUsage(dataSize);
      dataSize = 0;
    }
  }
}
