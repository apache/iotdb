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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
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
  public synchronized void close() {
    super.close();
    deviceMeasurementsMap = null;
    deviceIsAlignedMap = null;
    measurementDataTypeMap = null;
    final PipeMemoryBlock block = allocatedMemoryBlock;
    allocatedMemoryBlock = null;
    if (block != null) {
      block.close();
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
    final PipeMemoryBlock readerMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(
                PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes(),
                MEMORY_SUFFICIENT_THRESHOLD);
    if (readerMemoryBlock == null) {
      LOGGER.info(
          DataNodePipeMessages.FAILED_TO_CACHEDEVICEISALIGNEDMAPIFABSENT_FOR_TSFILE_BECAUSE_MEMORY,
          tsFile.getPath());
      return false;
    }

    final Map<IDeviceID, Boolean> cachedDeviceIsAlignedMap = new HashMap<>();
    long memoryRequiredInBytes = 0L;
    try {
      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(tsFile.getPath(), true, false)) {
        final TsFileDeviceIterator deviceIsAlignedIterator =
            sequenceReader.getAllDevicesIteratorWithIsAligned();
        while (deviceIsAlignedIterator.hasNext()) {
          final Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
          cachedDeviceIsAlignedMap.put(
              deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
        }
      }
      memoryRequiredInBytes +=
          PipeMemoryWeightUtil.memoryOfIDeviceId2Bool(cachedDeviceIsAlignedMap);
    } finally {
      // The reader block is temporary and must never become the persistent metadata block.
      readerMemoryBlock.close();
    }

    // Allocate again for the cached objects.
    final PipeMemoryBlock cachedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(memoryRequiredInBytes, MEMORY_SUFFICIENT_THRESHOLD);
    if (cachedMemoryBlock == null) {
      LOGGER.info(
          DataNodePipeMessages.PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE,
          tsFile.getPath());
      return false;
    }

    // Publish the map only after its accounting block has been acquired.  Readers never observe
    // a partially built map or a map without a corresponding memory reservation.
    deviceIsAlignedMap = cachedDeviceIsAlignedMap;
    allocatedMemoryBlock = cachedMemoryBlock;
    LOGGER.info(
        DataNodePipeMessages.PIPETSFILERESOURCE_CACHED_DEVICEISALIGNEDMAP_FOR_TSFILE,
        tsFile.getPath());
    return true;
  }

  synchronized boolean cacheObjectsIfAbsent(final File tsFile) throws IOException {
    if (allocatedMemoryBlock != null) {
      if (deviceMeasurementsMap != null) {
        return true;
      } else {
        // Recalculate it again because only deviceIsAligned map is cached
        final PipeMemoryBlock oldMemoryBlock = allocatedMemoryBlock;
        allocatedMemoryBlock = null;
        deviceIsAlignedMap = null;
        oldMemoryBlock.close();
      }
    }

    // See if pipe memory is sufficient to be allocated for TsFileSequenceReader.
    // Only allocate when pipe memory used is less than 50%, because memory here
    // is hard to shrink and may consume too much memory.
    final PipeMemoryBlock readerMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(
                PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes(),
                MEMORY_SUFFICIENT_THRESHOLD);
    if (readerMemoryBlock == null) {
      LOGGER.info(
          DataNodePipeMessages.FAILED_TO_CACHEOBJECTSIFABSENT_FOR_TSFILE_BECAUSE_MEMORY,
          tsFile.getPath());
      return false;
    }

    Map<IDeviceID, List<String>> cachedDeviceMeasurementsMap = null;
    Map<IDeviceID, Boolean> cachedDeviceIsAlignedMap = null;
    Map<String, TSDataType> cachedMeasurementDataTypeMap = null;
    long memoryRequiredInBytes = 0L;
    try {
      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(tsFile.getPath(), true, true)) {
        cachedDeviceMeasurementsMap = sequenceReader.getDeviceMeasurementsMap();
        memoryRequiredInBytes +=
            PipeMemoryWeightUtil.memoryOfIDeviceID2StrList(cachedDeviceMeasurementsMap);

        cachedDeviceIsAlignedMap = new HashMap<>();
        final TsFileDeviceIterator deviceIsAlignedIterator =
            sequenceReader.getAllDevicesIteratorWithIsAligned();
        while (deviceIsAlignedIterator.hasNext()) {
          final Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
          cachedDeviceIsAlignedMap.put(
              deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
        }
        memoryRequiredInBytes +=
            PipeMemoryWeightUtil.memoryOfIDeviceId2Bool(cachedDeviceIsAlignedMap);

        cachedMeasurementDataTypeMap = sequenceReader.getFullPathDataTypeMap();
        memoryRequiredInBytes +=
            PipeMemoryWeightUtil.memoryOfStr2TSDataType(cachedMeasurementDataTypeMap);
      }
    } finally {
      // The reader block is temporary and must be released even when metadata traversal fails.
      readerMemoryBlock.close();
    }

    // Allocate again for the cached objects.
    final PipeMemoryBlock cachedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateIfSufficient(memoryRequiredInBytes, MEMORY_SUFFICIENT_THRESHOLD);
    if (cachedMemoryBlock == null) {
      LOGGER.info(
          DataNodePipeMessages.PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE,
          tsFile.getPath());
      return false;
    }

    // Publish all metadata only after the persistent accounting block is ready.
    deviceMeasurementsMap = cachedDeviceMeasurementsMap;
    deviceIsAlignedMap = cachedDeviceIsAlignedMap;
    measurementDataTypeMap = cachedMeasurementDataTypeMap;
    allocatedMemoryBlock = cachedMemoryBlock;
    LOGGER.info(
        DataNodePipeMessages.PIPETSFILERESOURCE_CACHED_OBJECTS_FOR_TSFILE, tsFile.getPath());
    return true;
  }
}
