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

import org.apache.tsfile.utils.Pair;

import javax.validation.constraints.NotNull;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class PipeDynamicMemoryBlock {

  private final PipeModelFixedMemoryBlock fixedMemoryBlock;

  private boolean isExpandable = true;

  private Consumer<PipeDynamicMemoryBlock> expand = null;

  private volatile boolean released = false;

  private volatile long memoryUsageInBytes;

  private volatile double historyMemoryEfficiency;

  private volatile double currentMemoryEfficiency;

  public PipeDynamicMemoryBlock(
      final @NotNull PipeModelFixedMemoryBlock fixedMemoryBlock, final long memoryUsageInBytes) {
    this.memoryUsageInBytes = Math.max(memoryUsageInBytes, 0);
    this.fixedMemoryBlock = fixedMemoryBlock;
    historyMemoryEfficiency = 1.0;
    currentMemoryEfficiency = 1.0;
  }

  public PipeDynamicMemoryBlock(
      final @NotNull PipeModelFixedMemoryBlock fixedMemoryBlock,
      final long memoryUsageInBytes,
      final double historyMemoryEfficiency,
      final double currentMemoryEfficiency) {
    this.memoryUsageInBytes = Math.max(memoryUsageInBytes, 0);
    this.fixedMemoryBlock = fixedMemoryBlock;
    this.historyMemoryEfficiency = historyMemoryEfficiency;
    this.currentMemoryEfficiency = currentMemoryEfficiency;
  }

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes;
  }

  public void setMemoryUsageInBytes(final long memoryUsageInBytes) {
    this.memoryUsageInBytes = memoryUsageInBytes;
  }

  public Pair<Double, Double> getMemoryEfficiency() {
    synchronized (fixedMemoryBlock) {
      return new Pair<>(historyMemoryEfficiency, currentMemoryEfficiency);
    }
  }

  public void setExpandable(boolean expandable) {
    isExpandable = expandable;
  }

  public void setExpand(Consumer<PipeDynamicMemoryBlock> expand) {
    this.expand = expand;
  }

  public void updateCurrentMemoryEfficiencyAdjustMem(final double currentMemoryEfficiency) {
    synchronized (fixedMemoryBlock) {
      this.historyMemoryEfficiency = this.currentMemoryEfficiency;
      this.currentMemoryEfficiency = currentMemoryEfficiency;
      fixedMemoryBlock.dynamicallyAdjustMemory(this);
    }
  }

  public void updateMemoryEfficiency(
      final double currentMemoryEfficiency, final double historyMemoryEfficiency) {
    synchronized (fixedMemoryBlock) {
      this.historyMemoryEfficiency = historyMemoryEfficiency;
      this.currentMemoryEfficiency = currentMemoryEfficiency;
    }
  }

  public Stream<PipeDynamicMemoryBlock> getMemoryBlocks() {
    return fixedMemoryBlock.getMemoryBlocks();
  }

  public void applyForDynamicMemory(final long memoryUsageInBytes) {
    fixedMemoryBlock.resetMemoryBlockSize(this, memoryUsageInBytes);
  }

  public boolean isReleased() {
    return released;
  }

  public void close() {
    synchronized (fixedMemoryBlock) {
      fixedMemoryBlock.releaseMemory(this);
      released = true;
    }
  }

  void doExpand() {
    if (isExpandable && expand != null) {
      expand.accept(this);
    }
  }
}
