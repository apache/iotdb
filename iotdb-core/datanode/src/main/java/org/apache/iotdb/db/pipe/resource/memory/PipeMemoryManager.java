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
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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

  private static final boolean PIPE_MEMORY_MANAGEMENT_ENABLED =
      PipeConfig.getInstance().getPipeMemoryManagementEnabled();

  private static final int MEMORY_ALLOCATE_MAX_RETRIES =
      PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs();

  private static final long TOTAL_MEMORY_SIZE_IN_BYTES =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForPipe();
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES =
      PipeConfig.getInstance().getPipeMemoryAllocateMinSizeInBytes();

  private long usedMemorySizeInBytes;

  private final Set<PipeMemoryBlock> allocatedBlocks = new HashSet<>();

  public PipeMemoryManager() {
    PipeAgent.runtime()
        .registerPeriodicalJob(
            "PipeMemoryManager#tryExpandAll()",
            this::tryExpandAll,
            PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds());
  }

  public synchronized PipeMemoryBlock forceAllocate(long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    for (int i = 1; i <= MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
        return registeredMemoryBlock(sizeInBytes);
      }

      try {
        tryShrink4Allocate(sizeInBytes);
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
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
            MEMORY_ALLOCATE_MAX_RETRIES,
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  public synchronized PipeMemoryBlock forceAllocate(Tablet tablet)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(calculateTabletSizeInBytes(tablet));
  }

  /**
   * Allocate a memory block for pipe only if memory already used is less than specified threshold.
   *
   * @param sizeInBytes size of memory needed to allocate
   * @param usedThreshold proportion of memory used, ranged from 0.0 to 1.0
   * @return {@code null} if the proportion of memory already used exceeds {@code usedThreshold}.
   *     Will return a memory block otherwise.
   */
  public synchronized PipeMemoryBlock forceAllocateIfSufficient(
      long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }
    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes
        && (float) usedMemorySizeInBytes / TOTAL_MEMORY_SIZE_IN_BYTES < usedThreshold) {
      return forceAllocate(sizeInBytes);
    } else {
      long memoryToShrink =
          Math.max(
              usedMemorySizeInBytes - (long) (TOTAL_MEMORY_SIZE_IN_BYTES * usedThreshold),
              sizeInBytes);
      if (tryShrink4Allocate(memoryToShrink)) {
        return forceAllocate(sizeInBytes);
      }
    }
    return null;
  }

  private long calculateTabletSizeInBytes(Tablet tablet) {
    long totalSizeInBytes = 0;

    if (tablet == null) {
      return totalSizeInBytes;
    }

    // timestamps
    if (tablet.timestamps != null) {
      totalSizeInBytes += tablet.timestamps.length * 8L;
    }

    // values
    final List<MeasurementSchema> timeseries = tablet.getSchemas();
    if (timeseries != null) {
      for (int column = 0; column < timeseries.size(); column++) {
        final MeasurementSchema measurementSchema = timeseries.get(column);
        if (measurementSchema == null) {
          continue;
        }

        final TSDataType tsDataType = measurementSchema.getType();
        if (tsDataType == null) {
          continue;
        }

        if (tsDataType == TSDataType.TEXT) {
          if (tablet.values == null || tablet.values.length <= column) {
            continue;
          }
          final Binary[] values = ((Binary[]) tablet.values[column]);
          if (values == null) {
            continue;
          }
          for (Binary value : values) {
            totalSizeInBytes +=
                value == null ? 0 : (value.getLength() == -1 ? 0 : value.getLength());
          }
        } else {
          totalSizeInBytes += (long) tablet.timestamps.length * tsDataType.getDataTypeSize();
        }
      }
    }

    // bitMaps
    if (tablet.bitMaps != null) {
      for (int i = 0; i < tablet.bitMaps.length; i++) {
        totalSizeInBytes += tablet.bitMaps[i] == null ? 0 : tablet.bitMaps[i].getSize();
      }
    }

    // estimate other dataStructures size
    totalSizeInBytes += 100;

    return totalSizeInBytes;
  }

  public synchronized PipeMemoryBlock tryAllocate(long sizeInBytes) {
    return tryAllocate(sizeInBytes, currentSize -> currentSize * 2 / 3);
  }

  public synchronized PipeMemoryBlock tryAllocate(
      long sizeInBytes, LongUnaryOperator customAllocateStrategy) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
      return registeredMemoryBlock(sizeInBytes);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes,"
                + "actual requested memory size {} bytes",
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes,
            sizeToAllocateInBytes);
        return registeredMemoryBlock(sizeToAllocateInBytes);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    if (tryShrink4Allocate(sizeToAllocateInBytes)) {
      LOGGER.info(
          "tryAllocate: allocated memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "original requested memory size {} bytes,"
              + "actual requested memory size {} bytes",
          TOTAL_MEMORY_SIZE_IN_BYTES,
          usedMemorySizeInBytes,
          sizeInBytes,
          sizeToAllocateInBytes);
      return registeredMemoryBlock(sizeToAllocateInBytes);
    } else {
      LOGGER.warn(
          "tryAllocate: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes",
          TOTAL_MEMORY_SIZE_IN_BYTES,
          usedMemorySizeInBytes,
          sizeInBytes);
      return registeredMemoryBlock(0);
    }
  }

  public synchronized boolean tryAllocate(
      PipeMemoryBlock block, long memoryInBytesNeededToBeAllocated) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= memoryInBytesNeededToBeAllocated) {
      usedMemorySizeInBytes += memoryInBytesNeededToBeAllocated;
      block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() + memoryInBytesNeededToBeAllocated);
      return true;
    }

    return false;
  }

  private PipeMemoryBlock registeredMemoryBlock(long sizeInBytes) {
    usedMemorySizeInBytes += sizeInBytes;

    final PipeMemoryBlock returnedMemoryBlock = new PipeMemoryBlock(sizeInBytes);
    allocatedBlocks.add(returnedMemoryBlock);
    return returnedMemoryBlock;
  }

  private boolean tryShrink4Allocate(long sizeInBytes) {
    final List<PipeMemoryBlock> shuffledBlocks = new ArrayList<>(allocatedBlocks);
    Collections.shuffle(shuffledBlocks);

    while (true) {
      boolean hasAtLeastOneBlockShrinkable = false;
      for (final PipeMemoryBlock block : shuffledBlocks) {
        if (block.shrink()) {
          hasAtLeastOneBlockShrinkable = true;
          if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
            return true;
          }
        }
      }
      if (!hasAtLeastOneBlockShrinkable) {
        return false;
      }
    }
  }

  public synchronized void tryExpandAll() {
    allocatedBlocks.forEach(PipeMemoryBlock::expand);
  }

  public synchronized void release(PipeMemoryBlock block) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return;
    }

    allocatedBlocks.remove(block);
    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
  }

  public synchronized boolean release(PipeMemoryBlock block, long sizeInBytes) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    usedMemorySizeInBytes -= sizeInBytes;
    block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() - sizeInBytes);

    this.notifyAll();

    return true;
  }

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return TOTAL_MEMORY_SIZE_IN_BYTES;
  }
}
