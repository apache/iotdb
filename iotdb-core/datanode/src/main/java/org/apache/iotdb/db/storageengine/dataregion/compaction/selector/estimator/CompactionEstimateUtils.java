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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionSourceFileDeletedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.statistics.BinaryStatistics;
import org.apache.tsfile.file.metadata.statistics.StringStatistics;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompactionEstimateUtils {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  /**
   * Get the details of the tsfile, the returned array contains the following elements in sequence:
   *
   * <p>total chunk num in this tsfile
   *
   * <p>max chunk num of one timeseries in this tsfile
   *
   * <p>max aligned series num in one device. If there is no aligned series in this file, then it
   * turns to be -1.
   *
   * <p>max chunk num of one device in this tsfile
   *
   * @throws IOException if io errors occurred
   */
  static FileInfo calculateFileInfo(TsFileSequenceReader reader) throws IOException {
    int totalChunkNum = 0;
    int maxChunkNum = 0;
    int maxAlignedSeriesNumInDevice = -1;
    int maxDeviceChunkNum = 0;
    long maxMemCostToReadAlignedSeriesMetadata = 0;
    long maxMemCostToReadNonAlignedSeriesMetadata = 0;
    TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
    long totalMetadataSize = 0;
    while (deviceIterator.hasNext()) {
      int deviceChunkNum = 0;
      int alignedSeriesNumInDevice = 0;

      Pair<IDeviceID, Boolean> deviceWithIsAlignedPair = deviceIterator.next();
      IDeviceID device = deviceWithIsAlignedPair.left;
      boolean isAlignedDevice = deviceWithIsAlignedPair.right;
      long memCostToReadMetadata = 0;

      Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
          reader.getMeasurementChunkMetadataListMapIterator(device);
      while (measurementChunkMetadataListMapIterator.hasNext()) {
        Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap =
            measurementChunkMetadataListMapIterator.next();
        if (isAlignedDevice) {
          alignedSeriesNumInDevice += measurementChunkMetadataListMap.size();
        }

        for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataList :
            measurementChunkMetadataListMap.entrySet()) {
          int currentChunkMetadataListSize = measurementChunkMetadataList.getValue().size();
          long measurementNameRamSize =
              RamUsageEstimator.sizeOf(measurementChunkMetadataList.getKey());
          long chunkMetadataMemCost = 0;
          long currentSeriesRamSize = measurementNameRamSize;
          for (ChunkMetadata chunkMetadata : measurementChunkMetadataList.getValue()) {
            // chunkMetadata should not be a null value
            if (chunkMetadata != null) {
              TSDataType dataType = chunkMetadata.getDataType();
              chunkMetadataMemCost =
                  chunkMetadataMemCost != 0
                      ? chunkMetadataMemCost
                      : (ChunkMetadata.calculateRamSize(chunkMetadata.getMeasurementUid(), dataType)
                          - measurementNameRamSize);
              if (dataType == TSDataType.TEXT) {
                // add ram size for first value and last value
                currentSeriesRamSize +=
                    chunkMetadata.getStatistics().getRetainedSizeInBytes()
                        - BinaryStatistics.INSTANCE_SIZE;
              } else if (dataType == TSDataType.STRING) {
                currentSeriesRamSize +=
                    chunkMetadata.getStatistics().getRetainedSizeInBytes()
                        - StringStatistics.INSTANCE_SIZE;
              } else {
                break;
              }
            } else {
              LOGGER.warn(
                  "{} has null chunk metadata, file is {}",
                  device.toString() + "." + measurementChunkMetadataList.getKey(),
                  reader.getFileName());
            }
          }
          currentSeriesRamSize += chunkMetadataMemCost * currentChunkMetadataListSize;
          if (isAlignedDevice) {
            memCostToReadMetadata += currentSeriesRamSize;
          } else {
            maxMemCostToReadNonAlignedSeriesMetadata =
                Math.max(maxMemCostToReadNonAlignedSeriesMetadata, currentSeriesRamSize);
          }
          deviceChunkNum += currentChunkMetadataListSize;
          totalChunkNum += currentChunkMetadataListSize;
          maxChunkNum = Math.max(maxChunkNum, currentChunkMetadataListSize);
          totalMetadataSize += currentSeriesRamSize;
        }
      }
      if (isAlignedDevice) {
        maxAlignedSeriesNumInDevice =
            Math.max(maxAlignedSeriesNumInDevice, alignedSeriesNumInDevice);
      }
      maxDeviceChunkNum = Math.max(maxDeviceChunkNum, deviceChunkNum);
      maxMemCostToReadAlignedSeriesMetadata =
          Math.max(maxMemCostToReadAlignedSeriesMetadata, memCostToReadMetadata);
    }
    long averageChunkMetadataSize = totalChunkNum == 0 ? 0 : totalMetadataSize / totalChunkNum;
    return new FileInfo(
        totalChunkNum,
        maxChunkNum,
        maxAlignedSeriesNumInDevice,
        maxDeviceChunkNum,
        averageChunkMetadataSize,
        maxMemCostToReadAlignedSeriesMetadata,
        maxMemCostToReadNonAlignedSeriesMetadata);
  }

  static CompactionTaskMetadataInfo collectMetadataInfoFromDisk(
      List<TsFileResource> resources, CompactionType taskType) throws IOException {
    CompactionEstimateUtils.addReadLock(resources);
    CompactionTaskMetadataInfo metadataInfo = new CompactionTaskMetadataInfo();
    long cost = 0L;
    Map<IDeviceID, Long> deviceMetadataSizeMap = new HashMap<>();
    try {
      for (TsFileResource resource : resources) {
        cost += resource.getTotalModSizeInByte();
        try (CompactionTsFileReader reader =
            new CompactionTsFileReader(
                resource.getTsFilePath(),
                taskType,
                EncryptDBUtils.getFirstEncryptParamFromTSFilePath(resource.getTsFilePath()))) {
          for (Map.Entry<IDeviceID, Long> entry :
              getDeviceMetadataSizeMapAndCollectMetadataInfo(reader, metadataInfo).entrySet()) {
            deviceMetadataSizeMap.merge(entry.getKey(), entry.getValue(), Long::sum);
          }
        }
      }
      metadataInfo.metadataMemCost =
          cost + deviceMetadataSizeMap.values().stream().max(Long::compareTo).orElse(0L);
      return metadataInfo;
    } finally {
      CompactionEstimateUtils.releaseReadLock(resources);
    }
  }

  static CompactionTaskMetadataInfo collectMetadataInfoFromCachedFileInfo(
      List<TsFileResource> resources,
      Map<TsFileResource, FileInfo.RoughFileInfo> cachedFileInfo,
      boolean hasConcurrentSubTask) {
    CompactionTaskMetadataInfo metadataInfo = new CompactionTaskMetadataInfo();
    for (TsFileResource resource : resources) {
      metadataInfo.metadataMemCost += resource.getTotalModSizeInByte();
      long maxMemToReadAlignedSeries = cachedFileInfo.get(resource).maxMemToReadAlignedSeries;
      long maxMemToReadNonAlignedSeries = cachedFileInfo.get(resource).maxMemToReadNonAlignedSeries;
      metadataInfo.metadataMemCost +=
          Math.max(
              maxMemToReadAlignedSeries,
              maxMemToReadNonAlignedSeries
                  * (hasConcurrentSubTask
                      ? IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum()
                      : 1));
      if (maxMemToReadAlignedSeries > 0) {
        metadataInfo.hasAlignedSeries = true;
      }
    }
    return metadataInfo;
  }

  static Map<IDeviceID, Long> getDeviceMetadataSizeMapAndCollectMetadataInfo(
      CompactionTsFileReader reader, CompactionTaskMetadataInfo metadataInfo) throws IOException {
    Map<IDeviceID, Long> deviceMetadataSizeMap = new HashMap<>();
    TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
    while (deviceIterator.hasNext()) {
      Pair<IDeviceID, Boolean> deviceAlignedPair = deviceIterator.next();
      IDeviceID deviceID = deviceAlignedPair.getLeft();
      boolean isAligned = deviceAlignedPair.getRight();
      metadataInfo.hasAlignedSeries |= isAligned;
      MetadataIndexNode firstMeasurementNodeOfCurrentDevice =
          deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
      long totalTimeseriesMetadataSizeOfCurrentDevice = 0;
      Map<String, Pair<Long, Long>> timeseriesMetadataOffsetByDevice =
          reader.getTimeseriesMetadataOffsetByDevice(firstMeasurementNodeOfCurrentDevice);
      for (Pair<Long, Long> offsetPair : timeseriesMetadataOffsetByDevice.values()) {
        totalTimeseriesMetadataSizeOfCurrentDevice += (offsetPair.right - offsetPair.left);
      }
      deviceMetadataSizeMap.put(deviceID, totalTimeseriesMetadataSizeOfCurrentDevice);
    }
    return deviceMetadataSizeMap;
  }

  public static boolean shouldUseRoughEstimatedResult(long roughEstimatedMemCost) {
    return roughEstimatedMemCost > 0
        && IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * roughEstimatedMemCost
            < SystemInfo.getInstance().getMemorySizeForCompaction();
  }

  public static void addReadLock(List<TsFileResource> resources)
      throws CompactionSourceFileDeletedException {
    for (int i = 0; i < resources.size(); i++) {
      TsFileResource resource = resources.get(i);
      resource.readLock();
      if (resource.isDeleted()) {
        // release read lock
        for (int j = 0; j <= i; j++) {
          resources.get(j).readUnlock();
        }
        throw new CompactionSourceFileDeletedException(
            "source file " + resource.getTsFilePath() + " is deleted");
      }
    }
  }

  public static void releaseReadLock(List<TsFileResource> resources) {
    resources.forEach(TsFileResource::readUnlock);
  }
}
