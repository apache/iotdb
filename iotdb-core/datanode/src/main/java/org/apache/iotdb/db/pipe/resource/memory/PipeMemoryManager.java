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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

  private long usedMemorySizeInBytes = 0;

  public synchronized PipeMemoryBlock forceAllocate(long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
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
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
      usedMemorySizeInBytes += sizeInBytes;
      return new PipeMemoryBlock(sizeInBytes);
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
        usedMemorySizeInBytes += sizeToAllocateInBytes;
        return new PipeMemoryBlock(sizeToAllocateInBytes);
      }

      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("tryAllocate: interrupted while waiting for available memory", e);
      }

      sizeToAllocateInBytes =
          Math.max(sizeToAllocateInBytes * 2 / 3, MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    LOGGER.warn(
        "tryAllocate: failed to allocate memory, "
            + "total memory size {} bytes, used memory size {} bytes, "
            + "requested memory size {} bytes",
        TOTAL_MEMORY_SIZE_IN_BYTES,
        usedMemorySizeInBytes,
        sizeInBytes);

    return new PipeMemoryBlock(0);
  }

  public synchronized void release(PipeMemoryBlock block) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return;
    }

    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
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
