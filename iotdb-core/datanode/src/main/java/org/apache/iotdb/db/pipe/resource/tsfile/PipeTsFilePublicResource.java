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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeTsFilePublicResource extends PipeTsFileResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFilePublicResource.class);
  public static final float MEMORY_SUFFICIENT_THRESHOLD = 0.7f;
  private PipeMemoryBlock allocatedMemoryBlock;
  private Map<IDeviceID, List<String>> deviceMeasurementsMap = null;
  private Map<IDeviceID, Boolean> deviceIsAlignedMap = null;
  private Map<String, TSDataType> measurementDataTypeMap = null;

  public PipeTsFilePublicResource(File hardlinkOrCopiedFile) {
    super(hardlinkOrCopiedFile);
  }

  @Override
  public void close() {
    super.close();

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
  }

  //////////////////////////// Cache Getter ////////////////////////////

  public synchronized Map<IDeviceID, List<String>> tryGetDeviceMeasurementsMap(final File tsFile)
      throws IOException {
    if (deviceMeasurementsMap == null) {
      cacheObjectsIfAbsent(tsFile);
    }
    return deviceMeasurementsMap;
  }

  public synchronized Map<IDeviceID, Boolean> tryGetDeviceIsAlignedMap(
      final boolean cacheOtherMetadata, final File tsFile) throws IOException {
    if (deviceIsAlignedMap == null) {
      if (cacheOtherMetadata) {
        cacheObjectsIfAbsent(tsFile);
      } else {
        cacheDeviceIsAlignedMapIfAbsent(tsFile);
      }
    }
    return deviceIsAlignedMap;
  }

  public synchronized Map<String, TSDataType> tryGetMeasurementDataTypeMap(final File tsFile)
      throws IOException {
    if (measurementDataTypeMap == null) {
      cacheObjectsIfAbsent(tsFile);
    }
    return measurementDataTypeMap;
  }

  synchronized boolean cacheDeviceIsAlignedMapIfAbsent(final File tsFile) throws IOException {

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
          "Failed to cacheDeviceIsAlignedMapIfAbsent for tsfile {}, because memory usage is high",
          tsFile.getPath());
      return false;
    }

    long memoryRequiredInBytes = 0L;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(tsFile.getPath(), true, false)) {
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
          tsFile.getPath());
      deviceIsAlignedMap = null;
      return false;
    }

    LOGGER.info("PipeTsFileResource: Cached deviceIsAlignedMap for tsfile {}.", tsFile.getPath());
    return true;
  }

  synchronized boolean cacheObjectsIfAbsent(final File tsFile) throws IOException {
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
          "Failed to cacheObjectsIfAbsent for tsfile {}, because memory usage is high",
          tsFile.getPath());
      return false;
    }

    long memoryRequiredInBytes = 0L;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(tsFile.getPath(), true, true)) {
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
          tsFile.getPath());
      deviceIsAlignedMap = null;
      deviceMeasurementsMap = null;
      measurementDataTypeMap = null;
      return false;
    }

    LOGGER.info("PipeTsFileResource: Cached objects for tsfile {}.", tsFile.getPath());
    return true;
  }
}
