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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.memory.strategy.ThresholdAllocationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public class PipeMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryManager.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private static final boolean PIPE_MEMORY_MANAGEMENT_ENABLED =
      PipeConfig.getInstance().getPipeMemoryManagementEnabled();

  // TODO @spricoder: consider combine memory block and used MemorySizeInBytes
  private final IMemoryBlock memoryBlock =
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .getPipeMemoryManager()
          .exactAllocate("Stream", MemoryBlockType.DYNAMIC);

  private static final double EXCEED_PROTECT_THRESHOLD = 0.95;

  private volatile long usedMemorySizeInBytesOfTablets;

  private volatile long usedMemorySizeInBytesOfTsFiles;

  // Only non-zero memory blocks will be added to this set.
  private final Set<PipeMemoryBlock> allocatedBlocks = new HashSet<>();

  public PipeMemoryManager() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeMemoryManager#tryExpandAll()",
            this::tryExpandAllAndCheckConsistency,
            PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds());
  }

  // NOTE: Here we unify the memory threshold judgment for tablet and tsfile memory block, because
  // introducing too many heuristic rules not conducive to flexible dynamic adjustment of memory
  // configuration:
  // 1. Proportion of memory occupied by tablet memory block: [TABLET_MEMORY_REJECT_THRESHOLD / 2,
  // TABLET_MEMORY_REJECT_THRESHOLD + TS_FILE_MEMORY_REJECT_THRESHOLD / 2]
  // 2. Proportion of memory occupied by tsfile memory block: [TS_FILE_MEMORY_REJECT_THRESHOLD / 2,
  // TS_FILE_MEMORY_REJECT_THRESHOLD + TABLET_MEMORY_REJECT_THRESHOLD / 2]
  // 3. The sum of the memory proportion occupied by the tablet memory block and the tsfile memory
  // block does not exceed TABLET_MEMORY_REJECT_THRESHOLD + TS_FILE_MEMORY_REJECT_THRESHOLD

  private double allowedMaxMemorySizeInBytesOfTabletsAndTsFiles() {
    return (PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold())
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private double allowedMaxMemorySizeInBytesOfTablets() {
    return (PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold() / 2)
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private double allowedMaxMemorySizeInBytesOfTsTiles() {
    return (PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold() / 2)
        * getTotalNonFloatingMemorySizeInBytes();
  }

  public boolean isEnough4TabletParsing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTablets
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTablets();
  }

  private boolean isHardEnough4TabletParsing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTablets < allowedMaxMemorySizeInBytesOfTablets();
  }

  public boolean isEnough4TsFileSlicing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTsTiles();
  }

  private boolean isHardEnough4TsFileSlicing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTsFiles < allowedMaxMemorySizeInBytesOfTsTiles();
  }

  public synchronized PipeMemoryBlock forceAllocate(long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      // No need to calculate the tablet size, skip it to save time
      return new PipeMemoryBlock(0);
    }

    if (sizeInBytes == 0) {
      return registerMemoryBlock(0);
    }

    return forceAllocateWithRetry(sizeInBytes, PipeMemoryBlockType.NORMAL);
  }

  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(long tabletSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      // No need to calculate the tablet size, skip it to save time
      return new PipeTabletMemoryBlock(0);
    }

    if (tabletSizeInBytes == 0) {
      return (PipeTabletMemoryBlock) registerMemoryBlock(0, PipeMemoryBlockType.TABLET);
    }

    for (int i = 1, size = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries(); i <= size; i++) {
      if (isHardEnough4TabletParsing()) {
        break;
      }

      try {
        Thread.sleep(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocateWithRetry: interrupted while waiting for available memory", ex);
      }
    }

    if (!isHardEnough4TabletParsing()) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              "forceAllocateForTablet: failed to allocate because there's too much memory for tablets, "
                  + "total memory size %d bytes, used memory for tablet size %d bytes, requested memory size %d bytes",
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytesOfTablets,
              tabletSizeInBytes));
    }

    synchronized (this) {
      final PipeTabletMemoryBlock block =
          (PipeTabletMemoryBlock)
              forceAllocateWithRetry(tabletSizeInBytes, PipeMemoryBlockType.TABLET);
      usedMemorySizeInBytesOfTablets += block.getMemoryUsageInBytes();
      return block;
    }
  }

  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(long tsFileSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeTsFileMemoryBlock(0);
    }

    if (tsFileSizeInBytes == 0) {
      return (PipeTsFileMemoryBlock) registerMemoryBlock(0, PipeMemoryBlockType.TS_FILE);
    }

    for (int i = 1, size = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries(); i <= size; i++) {
      if (isHardEnough4TsFileSlicing()) {
        break;
      }

      try {
        Thread.sleep(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocateWithRetry: interrupted while waiting for available memory", ex);
      }
    }

    if (!isHardEnough4TsFileSlicing()) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              "forceAllocateForTsFile: failed to allocate because there's too much memory for tsfiles, "
                  + "total memory size %d bytes, used memory for tsfile size %d bytes, requested memory size %d bytes",
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytesOfTsFiles,
              tsFileSizeInBytes));
    }

    synchronized (this) {
      final PipeTsFileMemoryBlock block =
          (PipeTsFileMemoryBlock)
              forceAllocateWithRetry(tsFileSizeInBytes, PipeMemoryBlockType.TS_FILE);
      usedMemorySizeInBytesOfTsFiles += block.getMemoryUsageInBytes();
      return block;
    }
  }

  public PipeModelFixedMemoryBlock forceAllocateForModelFixedMemoryBlock(
      long fixedSizeInBytes, PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeModelFixedMemoryBlock(Long.MAX_VALUE, new ThresholdAllocationStrategy());
    }

    if (fixedSizeInBytes == 0) {
      return (PipeModelFixedMemoryBlock) registerMemoryBlock(0, type);
    }

    for (int i = 1, size = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries(); i <= size; i++) {
      if (getFreeMemorySizeInBytes() >= fixedSizeInBytes) {
        break;
      }

      try {
        Thread.sleep(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocateWithRetry: interrupted while waiting for available memory", ex);
      }
    }

    synchronized (this) {
      if (getFreeMemorySizeInBytes() < fixedSizeInBytes) {
        return (PipeModelFixedMemoryBlock) forceAllocateWithRetry(getFreeMemorySizeInBytes(), type);
      }

      return (PipeModelFixedMemoryBlock) forceAllocateWithRetry(fixedSizeInBytes, type);
    }
  }

  private PipeMemoryBlock forceAllocateWithRetry(long sizeInBytes, PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      switch (type) {
        case TABLET:
          return new PipeTabletMemoryBlock(sizeInBytes);
        case TS_FILE:
          return new PipeTsFileMemoryBlock(sizeInBytes);
        case BATCH:
        case WAL:
          return new PipeModelFixedMemoryBlock(sizeInBytes, new ThresholdAllocationStrategy());
        default:
          return new PipeMemoryBlock(sizeInBytes);
      }
    }

    final int memoryAllocateMaxRetries = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
          >= sizeInBytes) {
        return registerMemoryBlock(sizeInBytes, type);
      }

      try {
        tryShrinkUntilFreeMemorySatisfy(sizeInBytes);
        this.wait(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new PipeRuntimeOutOfMemoryCriticalException(
        String.format(
            "forceAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            memoryAllocateMaxRetries,
            getTotalNonFloatingMemorySizeInBytes(),
            memoryBlock.getUsedMemoryInBytes(),
            sizeInBytes));
  }

  public void forceResize(final PipeMemoryBlock block, final long targetSize) {
    resize(block, targetSize, true);
  }

  public synchronized void resize(
      final PipeMemoryBlock block, final long targetSize, final boolean force) {
    if (block == null || block.isReleased()) {
      LOGGER.warn("forceResize: cannot resize a null or released memory block");
      return;
    }

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      block.setMemoryUsageInBytes(targetSize);
      return;
    }

    final long oldSize = block.getMemoryUsageInBytes();
    if (oldSize == 0) {
      // If the memory block is not registered, we need to register it first.
      // Otherwise, the memory usage will be inconsistent.
      // See registerMemoryBlock for more details.
      allocatedBlocks.add(block);
    }

    if (oldSize >= targetSize) {
      memoryBlock.release(oldSize - targetSize);
      if (block instanceof PipeTabletMemoryBlock) {
        usedMemorySizeInBytesOfTablets -= oldSize - targetSize;
      }
      if (block instanceof PipeTsFileMemoryBlock) {
        usedMemorySizeInBytesOfTsFiles -= oldSize - targetSize;
      }
      block.setMemoryUsageInBytes(targetSize);

      // If no memory is used in the block, we can remove it from the allocated blocks.
      if (targetSize == 0) {
        allocatedBlocks.remove(block);
      }
      return;
    }

    long sizeInBytes = targetSize - oldSize;
    final int memoryAllocateMaxRetries = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
          >= sizeInBytes) {
        memoryBlock.forceAllocateWithoutLimitation(sizeInBytes);
        if (block instanceof PipeTabletMemoryBlock) {
          usedMemorySizeInBytesOfTablets += sizeInBytes;
        }
        if (block instanceof PipeTsFileMemoryBlock) {
          usedMemorySizeInBytesOfTsFiles += sizeInBytes;
        }
        block.setMemoryUsageInBytes(targetSize);
        return;
      }

      try {
        tryShrinkUntilFreeMemorySatisfy(sizeInBytes);
        this.wait(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceResize: interrupted while waiting for available memory", e);
      }
    }

    if (force) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              "forceResize: failed to allocate memory after %d retries, "
                  + "total memory size %d bytes, used memory size %d bytes, "
                  + "requested memory size %d bytes",
              memoryAllocateMaxRetries,
              getTotalNonFloatingMemorySizeInBytes(),
              memoryBlock.getUsedMemoryInBytes(),
              sizeInBytes));
    }
  }

  /**
   * Allocate a {@link PipeMemoryBlock} for pipe only if memory used after allocation is less than
   * the specified threshold.
   *
   * @param sizeInBytes size of memory needed to allocate
   * @param usedThreshold proportion of memory used, ranged from 0.0 to 1.0
   * @return {@code null} if the proportion of memory used after allocation exceeds {@code
   *     usedThreshold}. Will return a memory block otherwise.
   */
  public synchronized PipeMemoryBlock forceAllocateIfSufficient(
      long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    if (sizeInBytes == 0) {
      return registerMemoryBlock(0);
    }

    if ((float) (memoryBlock.getUsedMemoryInBytes() + sizeInBytes)
        <= getTotalNonFloatingMemorySizeInBytes() * usedThreshold) {
      return forceAllocate(sizeInBytes);
    }

    return null;
  }

  public synchronized PipeMemoryBlock tryAllocate(long sizeInBytes) {
    return tryAllocate(sizeInBytes, currentSize -> currentSize * 2 / 3);
  }

  public synchronized PipeMemoryBlock tryAllocate(
      long sizeInBytes, LongUnaryOperator customAllocateStrategy) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    if (sizeInBytes == 0
        || getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
            >= sizeInBytes) {
      return registerMemoryBlock(sizeInBytes);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    final long memoryAllocateMinSizeInBytes = PIPE_CONFIG.getPipeMemoryAllocateMinSizeInBytes();

    while (sizeToAllocateInBytes > memoryAllocateMinSizeInBytes) {
      if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
          >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes, "
                + "actual requested memory size {} bytes",
            getTotalNonFloatingMemorySizeInBytes(),
            memoryBlock.getUsedMemoryInBytes(),
            sizeInBytes,
            sizeToAllocateInBytes);
        return registerMemoryBlock(sizeToAllocateInBytes);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              memoryAllocateMinSizeInBytes);
    }

    if (tryShrinkUntilFreeMemorySatisfy(sizeToAllocateInBytes)) {
      LOGGER.info(
          "tryAllocate: allocated memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "original requested memory size {} bytes, "
              + "actual requested memory size {} bytes",
          getTotalNonFloatingMemorySizeInBytes(),
          memoryBlock.getUsedMemoryInBytes(),
          sizeInBytes,
          sizeToAllocateInBytes);
      return registerMemoryBlock(sizeToAllocateInBytes);
    } else {
      LOGGER.warn(
          "tryAllocate: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes",
          getTotalNonFloatingMemorySizeInBytes(),
          memoryBlock.getUsedMemoryInBytes(),
          sizeInBytes);
      return registerMemoryBlock(0);
    }
  }

  public synchronized boolean tryAllocate(
      PipeMemoryBlock block, long memoryInBytesNeededToBeAllocated) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
        >= memoryInBytesNeededToBeAllocated) {
      memoryBlock.forceAllocateWithoutLimitation(memoryInBytesNeededToBeAllocated);
      if (block instanceof PipeTabletMemoryBlock) {
        usedMemorySizeInBytesOfTablets += memoryInBytesNeededToBeAllocated;
      }
      if (block instanceof PipeTsFileMemoryBlock) {
        usedMemorySizeInBytesOfTsFiles += memoryInBytesNeededToBeAllocated;
      }
      block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() + memoryInBytesNeededToBeAllocated);
      return true;
    }

    return false;
  }

  private PipeMemoryBlock registerMemoryBlock(long sizeInBytes) {
    return registerMemoryBlock(sizeInBytes, PipeMemoryBlockType.NORMAL);
  }

  private PipeMemoryBlock registerMemoryBlock(long sizeInBytes, PipeMemoryBlockType type) {
    final PipeMemoryBlock returnedMemoryBlock;
    switch (type) {
      case TABLET:
        returnedMemoryBlock = new PipeTabletMemoryBlock(sizeInBytes);
        break;
      case TS_FILE:
        returnedMemoryBlock = new PipeTsFileMemoryBlock(sizeInBytes);
        break;
      case BATCH:
      case WAL:
        returnedMemoryBlock =
            new PipeModelFixedMemoryBlock(sizeInBytes, new ThresholdAllocationStrategy());
        break;
      default:
        returnedMemoryBlock = new PipeMemoryBlock(sizeInBytes);
        break;
    }

    // For memory block whose size is 0, we do not need to add it to the allocated blocks now.
    // It's good for performance and will not trigger concurrent issues.
    // If forceResize is called on it, we will add it to the allocated blocks.
    if (sizeInBytes > 0) {
      memoryBlock.forceAllocateWithoutLimitation(sizeInBytes);
      allocatedBlocks.add(returnedMemoryBlock);
    }

    return returnedMemoryBlock;
  }

  private boolean tryShrinkUntilFreeMemorySatisfy(long sizeInBytes) {
    final List<PipeMemoryBlock> shuffledBlocks = new ArrayList<>(allocatedBlocks);
    Collections.shuffle(shuffledBlocks);

    while (true) {
      boolean hasAtLeastOneBlockShrinkable = false;
      for (final PipeMemoryBlock block : shuffledBlocks) {
        if (block.shrink()) {
          hasAtLeastOneBlockShrinkable = true;
          if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
              >= sizeInBytes) {
            return true;
          }
        }
      }
      if (!hasAtLeastOneBlockShrinkable) {
        return false;
      }
    }
  }

  public synchronized void tryExpandAllAndCheckConsistency() {
    allocatedBlocks.forEach(PipeMemoryBlock::expand);

    long blockSum =
        allocatedBlocks.stream().mapToLong(PipeMemoryBlock::getMemoryUsageInBytes).sum();
    if (blockSum != memoryBlock.getUsedMemoryInBytes()) {
      LOGGER.warn(
          "tryExpandAllAndCheckConsistency: memory usage is not consistent with allocated blocks,"
              + " usedMemorySizeInBytes is {} but sum of all blocks is {}",
          memoryBlock.getUsedMemoryInBytes(),
          blockSum);
    }

    long tabletBlockSum =
        allocatedBlocks.stream()
            .filter(PipeTabletMemoryBlock.class::isInstance)
            .mapToLong(PipeMemoryBlock::getMemoryUsageInBytes)
            .sum();
    if (tabletBlockSum != usedMemorySizeInBytesOfTablets) {
      LOGGER.warn(
          "tryExpandAllAndCheckConsistency: memory usage of tablets is not consistent with allocated blocks,"
              + " usedMemorySizeInBytesOfTablets is {} but sum of all tablet blocks is {}",
          usedMemorySizeInBytesOfTablets,
          tabletBlockSum);
    }

    long tsFileBlockSum =
        allocatedBlocks.stream()
            .filter(PipeTsFileMemoryBlock.class::isInstance)
            .mapToLong(PipeMemoryBlock::getMemoryUsageInBytes)
            .sum();
    if (tsFileBlockSum != usedMemorySizeInBytesOfTsFiles) {
      LOGGER.warn(
          "tryExpandAllAndCheckConsistency: memory usage of tsfiles is not consistent with allocated blocks,"
              + " usedMemorySizeInBytesOfTsFiles is {} but sum of all tsfile blocks is {}",
          usedMemorySizeInBytesOfTsFiles,
          tsFileBlockSum);
    }
  }

  public synchronized void release(PipeMemoryBlock block) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return;
    }

    allocatedBlocks.remove(block);
    memoryBlock.release(block.getMemoryUsageInBytes());
    if (block instanceof PipeTabletMemoryBlock) {
      usedMemorySizeInBytesOfTablets -= block.getMemoryUsageInBytes();
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      usedMemorySizeInBytesOfTsFiles -= block.getMemoryUsageInBytes();
    }
    block.markAsReleased();

    this.notifyAll();
  }

  public synchronized boolean release(PipeMemoryBlock block, long sizeInBytes) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    memoryBlock.release(sizeInBytes);
    if (block instanceof PipeTabletMemoryBlock) {
      usedMemorySizeInBytesOfTablets -= sizeInBytes;
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      usedMemorySizeInBytesOfTsFiles -= sizeInBytes;
    }
    block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() - sizeInBytes);

    this.notifyAll();

    return true;
  }

  public long getUsedMemorySizeInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public long getUsedMemorySizeInBytesOfTablets() {
    return usedMemorySizeInBytesOfTablets;
  }

  public long getUsedMemorySizeInBytesOfTsFiles() {
    return usedMemorySizeInBytesOfTsFiles;
  }

  public long getFreeMemorySizeInBytes() {
    return memoryBlock.getFreeMemoryInBytes();
  }

  public long getTotalNonFloatingMemorySizeInBytes() {
    return (long)
        (memoryBlock.getTotalMemorySizeInBytes()
            * (1 - PipeConfig.getInstance().getPipeTotalFloatingMemoryProportion()));
  }

  public long getTotalFloatingMemorySizeInBytes() {
    return (long)
        (memoryBlock.getTotalMemorySizeInBytes()
            * PipeConfig.getInstance().getPipeTotalFloatingMemoryProportion());
  }

  public long getTotalMemorySizeInBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }
}
