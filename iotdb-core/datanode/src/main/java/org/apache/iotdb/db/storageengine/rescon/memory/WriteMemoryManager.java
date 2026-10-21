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
package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns a write-memory accounting scope (a DataRegion or flush reservations). This is not a new
 * budget: SystemInfo retains the existing reporting, flush and rejection policy.
 */
public final class WriteMemoryManager {
  private final IMemoryBlock memoryBlock;

  public WriteMemoryManager() {
    this("WriteMemory");
  }

  public WriteMemoryManager(String scope) {
    memoryBlock = new AtomicLongMemoryBlock(scope, null, Long.MAX_VALUE);
  }

  private final Set<WriteMemoryBlock> blocks = ConcurrentHashMap.newKeySet();

  public WriteMemoryBlock createBlock(String name, WriteMemoryBlock.Category category) {
    WriteMemoryBlock block = new WriteMemoryBlock(name, category);
    block.attachTo(this);
    return block;
  }

  void register(WriteMemoryBlock block) {
    blocks.add(block);
  }

  void unregister(WriteMemoryBlock block) {
    blocks.remove(block);
  }

  void addMemory(long bytes) {
    memoryBlock.forceAllocateWithoutLimitation(bytes);
  }

  void releaseMemory(long bytes) {
    memoryBlock.release(bytes);
  }

  /** Includes pending reservations, just like the previous DataRegion memory counter. */
  public long getMemoryUsageInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public List<WriteMemoryBlock> getMemoryBlocks() {
    return new ArrayList<>(blocks);
  }
}
