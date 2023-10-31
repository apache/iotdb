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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PipeMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryManager.class);

  private final boolean memoryAllocateEnabled =
      PipeConfig.getInstance().getPipeMemoryManagementEnabled();

  private static final int MEMORY_ALLOCATE_MAX_RETRIES =
      PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs();

  private static final long TOTAL_MEMORY_SIZE_IN_BYTES =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForPipe();
  private final long allocateMinSize =
      PipeConfig.getInstance().getPipeMemoryAllocateMinSizeInBytes();
  private long usedMemorySizeInBytes = 0;

  public synchronized PipeMemoryBlock forceAllocate(long sizeInBytes) throws PipeRuntimeException {
    if (!memoryAllocateEnabled) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    for (int i = 1; i <= MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
        usedMemorySizeInBytes += sizeInBytes;
        return new PipeMemoryBlock(sizeInBytes);
      }

      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("allocate: interrupted while waiting for available memory", e);
      }
    }

    throw new PipeRuntimeOutOfMemoryCriticalException(
        String.format(
            "failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  public synchronized PipeMemoryBlock tryAllocate(long sizeInBytes) {
    if (!memoryAllocateEnabled) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    long allocateSize = sizeInBytes;
    while (allocateSize > allocateMinSize) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= allocateSize) {
        usedMemorySizeInBytes += allocateSize;
        return new PipeMemoryBlock(allocateSize);
      }

      allocateSize = Math.max(allocateSize * 2 / 3, allocateMinSize);
      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("allocate: interrupted while waiting for available memory", e);
      }
    }

    return new PipeMemoryBlock(0);
  }

  public synchronized PipeMemoryBlock forceAllocateForTablet(Tablet tablet)
      throws PipeRuntimeException {
    return forceAllocate(calculateTabletSizeInBytes(tablet));
  }

  public synchronized void release(PipeMemoryBlock block) {
    if (!memoryAllocateEnabled || block == null || block.isReleased()) {
      return;
    }

    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
  }

  public long calculateTabletSizeInBytes(Tablet tablet) {
    long totalSizeInBytes = 0;

    // timestamps
    totalSizeInBytes += tablet.timestamps.length * 8L;

    // values
    List<MeasurementSchema> timeseries = tablet.getSchemas();
    for (int column = 0; column < timeseries.size(); column++) {
      TSDataType tsDataType = timeseries.get(column).getType();
      for (int row = 0; row < tablet.rowSize; row++) {
        // check isNull in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[column] != null
            && tablet.bitMaps[column].isMarked(row)) {
          continue;
        }

        // value
        totalSizeInBytes +=
            tsDataType == TSDataType.TEXT
                ? (((Binary[]) tablet.values[column])[row]).getLength()
                : tsDataType.getDataTypeSize();
      }
    }

    // bitMaps
    for (int i = 0; i < tablet.bitMaps.length; i++) {
      totalSizeInBytes += tablet.bitMaps[i] == null ? 0 : tablet.bitMaps[i].getSize();
    }

    // rowSize, maxRowNumber
    totalSizeInBytes += TSDataType.INT32.getDataTypeSize() * 2L;

    // estimate other dataStructures size
    totalSizeInBytes += 100;

    return totalSizeInBytes;
  }

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return TOTAL_MEMORY_SIZE_IN_BYTES;
  }

  public double getMemoryUsage() {
    return (double) usedMemorySizeInBytes / TOTAL_MEMORY_SIZE_IN_BYTES;
  }
}
