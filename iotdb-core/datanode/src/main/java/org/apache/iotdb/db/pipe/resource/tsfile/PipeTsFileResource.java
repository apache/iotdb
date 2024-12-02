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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTsFileResource implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResource.class);

  public static final long TSFILE_MIN_TIME_TO_LIVE_IN_MS = 1000L * 20;
  private static final float MEMORY_SUFFICIENT_THRESHOLD = 0.5f;

  private final File hardlinkOrCopiedFile;
  private final boolean isTsFile;

  /** this TsFileResource is used to track the {@link TsFileResourceStatus} of original TsFile. * */
  private final TsFileResource tsFileResource;

  private volatile long fileSize = -1L;

  private final AtomicInteger referenceCount;
  private final AtomicLong lastUnpinToZeroTime;
  private PipeMemoryBlock allocatedMemoryBlock;
  private Map<IDeviceID, List<String>> deviceMeasurementsMap = null;
  private Map<IDeviceID, Boolean> deviceIsAlignedMap = null;
  private Map<String, TSDataType> measurementDataTypeMap = null;

  public PipeTsFileResource(
      final File hardlinkOrCopiedFile,
      final boolean isTsFile,
      final TsFileResource tsFileResource) {
    this.hardlinkOrCopiedFile = hardlinkOrCopiedFile;
    this.isTsFile = isTsFile;
    this.tsFileResource = tsFileResource;

    referenceCount = new AtomicInteger(1);
    lastUnpinToZeroTime = new AtomicLong(Long.MAX_VALUE);
  }

  public File getFile() {
    return hardlinkOrCopiedFile;
  }

  public boolean isOriginalTsFileDeleted() {
    return isTsFile && Objects.nonNull(tsFileResource) && tsFileResource.isDeleted();
  }

  public long getFileSize() {
    if (fileSize == -1L) {
      synchronized (this) {
        if (fileSize == -1L) {
          fileSize = hardlinkOrCopiedFile.length();
        }
      }
    }
    return fileSize;
  }

  public long getTsFileResourceSize() {
    return Objects.nonNull(tsFileResource) ? tsFileResource.calculateRamSize() : 0;
  }

  ///////////////////// Reference Count /////////////////////

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public int increaseAndGetReference() {
    return referenceCount.addAndGet(1);
  }

  public int decreaseAndGetReference() {
    final int finalReferenceCount = referenceCount.addAndGet(-1);
    if (finalReferenceCount == 0) {
      lastUnpinToZeroTime.set(System.currentTimeMillis());
    }
    if (finalReferenceCount < 0) {
      LOGGER.warn("PipeTsFileResource's reference count is decreased to below 0.");
    }
    return finalReferenceCount;
  }

  public synchronized boolean closeIfOutOfTimeToLive() throws IOException {
    if (referenceCount.get() <= 0
        && (deviceMeasurementsMap == null // Not cached yet.
            || System.currentTimeMillis() - lastUnpinToZeroTime.get()
                > TSFILE_MIN_TIME_TO_LIVE_IN_MS)) {
      close();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (deviceMeasurementsMap != null) {
      deviceMeasurementsMap = null;
    }

    if (deviceIsAlignedMap != null) {
      deviceIsAlignedMap = null;
    }

    if (measurementDataTypeMap != null) {
      measurementDataTypeMap = null;
    }

    if (allocatedMemoryBlock != null) {
      allocatedMemoryBlock.close();
      allocatedMemoryBlock = null;
    }

    Files.deleteIfExists(hardlinkOrCopiedFile.toPath());

    LOGGER.info("PipeTsFileResource: Closed tsfile {} and cleaned up.", hardlinkOrCopiedFile);
  }

  //////////////////////////// Cache Getter ////////////////////////////

  public synchronized Map<IDeviceID, List<String>> tryGetDeviceMeasurementsMap()
      throws IOException {
    if (deviceMeasurementsMap == null && isTsFile) {
      cacheObjectsIfAbsent();
    }
    return deviceMeasurementsMap;
  }

  public synchronized Map<IDeviceID, Boolean> tryGetDeviceIsAlignedMap(
      final boolean cacheOtherMetadata) throws IOException {
    if (deviceIsAlignedMap == null && isTsFile) {
      if (cacheOtherMetadata) {
        cacheObjectsIfAbsent();
      } else {
        cacheDeviceIsAlignedMapIfAbsent();
      }
    }
    return deviceIsAlignedMap;
  }

  public synchronized Map<String, TSDataType> tryGetMeasurementDataTypeMap() throws IOException {
    if (measurementDataTypeMap == null && isTsFile) {
      cacheObjectsIfAbsent();
    }
    return measurementDataTypeMap;
  }

  synchronized boolean cacheDeviceIsAlignedMapIfAbsent() throws IOException {
    if (!isTsFile) {
      return false;
    }

    if (allocatedMemoryBlock != null) {
      // This means objects are already cached.
      return true;
    }

    // See if pipe memory is sufficient to be allocated for TsFileSequenceReader.
    // Only allocate when pipe memory used is less than 50%, because memory here
    // is hard to shrink and may consume too much memory.
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(
                PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes(),
                MEMORY_SUFFICIENT_THRESHOLD);
    if (allocatedMemoryBlock == null) {
      LOGGER.info(
          "PipeTsFileResource: Failed to create TsFileSequenceReader for tsfile {} in cache, because memory usage is high",
          hardlinkOrCopiedFile.getPath());
      return false;
    }

    long memoryRequiredInBytes = 0L;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(hardlinkOrCopiedFile.getPath(), true, false)) {
      deviceIsAlignedMap = new HashMap<>();
      final TsFileDeviceIterator deviceIsAlignedIterator =
          sequenceReader.getAllDevicesIteratorWithIsAligned();
      while (deviceIsAlignedIterator.hasNext()) {
        final Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
        deviceIsAlignedMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
      }
      memoryRequiredInBytes += PipeMemoryWeightUtil.memoryOfIDeviceId2Bool(deviceIsAlignedMap);
    }
    // Release memory of TsFileSequenceReader.
    allocatedMemoryBlock.close();
    allocatedMemoryBlock = null;

    // Allocate again for the cached objects.
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(memoryRequiredInBytes, MEMORY_SUFFICIENT_THRESHOLD);
    if (allocatedMemoryBlock == null) {
      LOGGER.info(
          "PipeTsFileResource: Failed to cache objects for tsfile {} in cache, because memory usage is high",
          hardlinkOrCopiedFile.getPath());
      deviceIsAlignedMap = null;
      return false;
    }

    LOGGER.info(
        "PipeTsFileResource: Cached deviceIsAlignedMap for tsfile {}.",
        hardlinkOrCopiedFile.getPath());
    return true;
  }

  synchronized boolean cacheObjectsIfAbsent() throws IOException {
    if (!isTsFile) {
      return false;
    }

    if (allocatedMemoryBlock != null) {
      if (deviceMeasurementsMap != null) {
        return true;
      } else {
        // Recalculate it again because only deviceIsAligned map is cached
        allocatedMemoryBlock.close();
        allocatedMemoryBlock = null;
      }
    }

    // See if pipe memory is sufficient to be allocated for TsFileSequenceReader.
    // Only allocate when pipe memory used is less than 50%, because memory here
    // is hard to shrink and may consume too much memory.
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(
                PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes(),
                MEMORY_SUFFICIENT_THRESHOLD);
    if (allocatedMemoryBlock == null) {
      LOGGER.info(
          "PipeTsFileResource: Failed to create TsFileSequenceReader for tsfile {} in cache, because memory usage is high",
          hardlinkOrCopiedFile.getPath());
      return false;
    }

    long memoryRequiredInBytes = 0L;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(hardlinkOrCopiedFile.getPath(), true, true)) {
      deviceMeasurementsMap = sequenceReader.getDeviceMeasurementsMap();
      memoryRequiredInBytes +=
          PipeMemoryWeightUtil.memoryOfIDeviceID2StrList(deviceMeasurementsMap);

      if (Objects.isNull(deviceIsAlignedMap)) {
        deviceIsAlignedMap = new HashMap<>();
        final TsFileDeviceIterator deviceIsAlignedIterator =
            sequenceReader.getAllDevicesIteratorWithIsAligned();
        while (deviceIsAlignedIterator.hasNext()) {
          final Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
          deviceIsAlignedMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
        }
      }
      memoryRequiredInBytes += PipeMemoryWeightUtil.memoryOfIDeviceId2Bool(deviceIsAlignedMap);

      measurementDataTypeMap = sequenceReader.getFullPathDataTypeMap();
      memoryRequiredInBytes += PipeMemoryWeightUtil.memoryOfStr2TSDataType(measurementDataTypeMap);
    }
    // Release memory of TsFileSequenceReader.
    allocatedMemoryBlock.close();
    allocatedMemoryBlock = null;

    // Allocate again for the cached objects.
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(memoryRequiredInBytes, MEMORY_SUFFICIENT_THRESHOLD);
    if (allocatedMemoryBlock == null) {
      LOGGER.info(
          "PipeTsFileResource: Failed to cache objects for tsfile {} in cache, because memory usage is high",
          hardlinkOrCopiedFile.getPath());
      deviceIsAlignedMap = null;
      deviceMeasurementsMap = null;
      measurementDataTypeMap = null;
      return false;
    }

    LOGGER.info(
        "PipeTsFileResource: Cached objects for tsfile {}.", hardlinkOrCopiedFile.getPath());
    return true;
  }
}
