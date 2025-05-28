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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.db.pipe.resource.memory.strategy.DynamicMemoryAllocationStrategy;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class PipeModelFixedMemoryBlock extends PipeFixedMemoryBlock {

  private final Set<PipeDynamicMemoryBlock> memoryBlocks =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final DynamicMemoryAllocationStrategy allocationStrategy;

  private volatile long memoryAllocatedInBytes;

  public PipeModelFixedMemoryBlock(
      final long memoryUsageInBytes, final DynamicMemoryAllocationStrategy allocationStrategy) {
    super(memoryUsageInBytes);
    this.memoryAllocatedInBytes = 0;
    this.allocationStrategy = allocationStrategy;
  }

  public synchronized PipeDynamicMemoryBlock registerPipeBatchMemoryBlock(
      final long memorySizeInBytes) {
    final PipeDynamicMemoryBlock memoryBlock = new PipeDynamicMemoryBlock(this, 0);
    memoryBlocks.add(memoryBlock);
    if (memorySizeInBytes != 0) {
      resetMemoryBlockSize(memoryBlock, memorySizeInBytes);
      double e = (double) getMemoryUsageInBytes() / memorySizeInBytes;
      memoryBlock.updateMemoryEfficiency(e, e);
      return memoryBlock;
    }

    memoryBlock.updateMemoryEfficiency(0.0, 0.0);
    return memoryBlock;
  }

  @Override
  public synchronized boolean expand() {
    // Ensure that the memory block that gets most of the memory is released first, which can reduce
    // the jitter of memory allocationIf the memory block is not expanded, it will not be expanded
    // again.This function not only completes the expansion but also the reduction.
    memoryBlocks.stream()
        .sorted((a, b) -> Long.compare(b.getMemoryUsageInBytes(), a.getMemoryUsageInBytes()))
        .forEach(PipeDynamicMemoryBlock::doExpand);
    return false;
  }

  public long getMemoryAllocatedInBytes() {
    return memoryAllocatedInBytes;
  }

  public synchronized Set<PipeDynamicMemoryBlock> getMemoryBlocks() {
    return memoryBlocks;
  }

  synchronized void releaseMemory(final PipeDynamicMemoryBlock memoryBlock) {
    resetMemoryBlockSize(memoryBlock, 0);
    memoryBlocks.remove(memoryBlock);
  }

  synchronized void dynamicallyAdjustMemory(final PipeDynamicMemoryBlock block) {
    if (this.isReleased() || block.isReleased() || !memoryBlocks.contains(block)) {
      throw new IllegalStateException("The memory block has been released");
    }
    allocationStrategy.dynamicallyAdjustMemory(block);
  }

  synchronized void resetMemoryBlockSize(
      final PipeDynamicMemoryBlock block, final long memorySizeInBytes) {
    if (this.isReleased() || block.isReleased() || !memoryBlocks.contains(block)) {
      throw new IllegalStateException("The memory block has been released");
    }

    final long diff = memorySizeInBytes - block.getMemoryUsageInBytes();

    // If the capacity is expanded, determine whether it will exceed the maximum value of the fixed
    // module
    if (getMemoryUsageInBytes() - memoryAllocatedInBytes < diff) {
      // Pay attention to the order of calls, otherwise it will cause resource leakage
      block.setMemoryUsageInBytes(
          block.getMemoryUsageInBytes() + getMemoryUsageInBytes() - memoryAllocatedInBytes);
      memoryAllocatedInBytes = getMemoryUsageInBytes();
      return;
    }

    memoryAllocatedInBytes = memoryAllocatedInBytes + diff;
    block.setMemoryUsageInBytes(memorySizeInBytes);
  }

  Stream<PipeDynamicMemoryBlock> getMemoryBlocksStream() {
    if (isReleased()) {
      throw new IllegalStateException("The memory block has been released");
    }
    return memoryBlocks.stream();
  }

  @Override
  public synchronized void close() {
    memoryBlocks.forEach(PipeDynamicMemoryBlock::close);
    super.close();
  }
}
