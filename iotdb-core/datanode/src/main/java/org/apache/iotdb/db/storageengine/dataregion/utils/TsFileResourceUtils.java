/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileResourceUtils {

  private static final Logger logger = LoggerFactory.getLogger(TsFileResourceUtils.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String VALIDATE_FAILED = "validate failed,";

  private TsFileResourceUtils() {
    // util class
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static boolean validateTsFileResourceCorrectness(TsFileResource resource) {
    if (resource.isDeleted()) {
      return true;
    }
    ArrayDeviceTimeIndex timeIndex;
    try {
      if (resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE) {
        // if time index is not device time index, then deserialize it from resource file
        timeIndex = resource.buildDeviceTimeIndex();
      } else {
        timeIndex = (ArrayDeviceTimeIndex) resource.getTimeIndex();
      }
      if (timeIndex == null) {
        logger.error(
            StorageEngineMessages.TIME_INDEX_IS_NULL, resource.getTsFilePath(), VALIDATE_FAILED);
        return false;
      }
      Set<IDeviceID> devices = timeIndex.getDevices();
      if (devices.isEmpty()) {
        logger.error(
            StorageEngineMessages.EMPTY_RESOURCE, resource.getTsFilePath(), VALIDATE_FAILED);
        return false;
      }
      for (IDeviceID device : devices) {
        // iterating the index, must present
        long startTime = timeIndex.getStartTime(device).get();
        long endTime = timeIndex.getEndTime(device).get();
        if (startTime > endTime) {
          logger.error(
              StorageEngineMessages.STORAGE_LOG_THE_START_TIME_OF_IS_GREATER_THAN_END_TIME_44DD784A,
              resource.getTsFilePath(),
              VALIDATE_FAILED,
              device);
          return false;
        }
      }
    } catch (IOException e) {
      logger.error(StorageEngineMessages.ERROR_VALIDATE_RESOURCE_FILE, resource.getTsFilePath());
      return false;
    }
    return true;
  }

  public static boolean validateTsFileIsComplete(TsFileResource resource) {
    if (resource.getTsFile().exists()
        && resource.getTsFile().length()
            < TSFileConfig.MAGIC_STRING.getBytes().length * 2L + Byte.BYTES) {
      // file size is smaller than magic string and version number
      logger.error(
          String.format(
              StorageEngineMessages
                  .TARGET_FILE_SMALLER_THAN_MAGIC_STRING_AND_VERSION_NUMBER_SIZE_FMT,
              resource));
      return false;
    }
    return true;
  }

  public static boolean validateTsFileDataCorrectness(TsFileResource resource) {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            resource.getTsFilePath(),
            EncryptDBUtils.getFirstEncryptParamFromTSFilePath(resource.getTsFilePath()))) {
      if (!reader.isComplete()) {
        logger.error(
            StorageEngineMessages.ILLEGAL_TSFILE, resource.getTsFilePath(), VALIDATE_FAILED);
        return false;
      }

      Map<Long, IChunkMetadata> chunkMetadataMap = getChunkMetadata(reader);
      if (chunkMetadataMap.isEmpty()) {
        logger.error(
            StorageEngineMessages.STORAGE_LOG_THERE_IS_NO_DATA_IN_THE_FILE_F480954E,
            resource.getTsFilePath(),
            VALIDATE_FAILED);
        return false;
      }

      List<List<long[]>> alignedTimeBatches = new ArrayList<>();
      Map<String, Integer> valueColumn2TimeBatchIndex = new HashMap<>();
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            long chunkOffset = reader.position();
            ChunkHeader header = reader.readChunkHeader(marker);
            IChunkMetadata chunkMetadata = chunkMetadataMap.get(chunkOffset - Byte.BYTES);
            if (!chunkMetadata.getMeasurementUid().equals(header.getMeasurementID())) {
              logger.error(
                  StorageEngineMessages
                      .STORAGE_LOG_CHUNK_START_OFFSET_IS_INCONSISTENT_WITH_THE_VALUE_IN_THE_E1E7AF07,
                  VALIDATE_FAILED);
              return false;
            }

            String measurement = header.getMeasurementID();
            List<long[]> alignedTimeBatch = null;
            if (header.getDataType() == TSDataType.VECTOR) {
              alignedTimeBatch = new ArrayList<>();
              alignedTimeBatches.add(alignedTimeBatch);
            } else if (marker == MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER
                || marker == MetaMarker.VALUE_CHUNK_HEADER) {
              int timeBatchIndex = valueColumn2TimeBatchIndex.getOrDefault(measurement, 0);
              valueColumn2TimeBatchIndex.put(measurement, timeBatchIndex + 1);
              alignedTimeBatch = alignedTimeBatches.get(timeBatchIndex);
            }
            // empty value chunk
            int dataSize = header.getDataSize();
            if (dataSize == 0) {
              break;
            }

            boolean isHasStatistic = (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER;
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());

            pageIndex = 0;

            LinkedList<Long> lastNoAlignedPageTimeStamps = new LinkedList<>();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader = reader.readPageHeader(header.getDataType(), isHasStatistic);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());

              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                  == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                long[] pageTimestamps = timePageReader.getNextTimeBatch();
                long pageHeaderStartTime =
                    isHasStatistic ? pageHeader.getStartTime() : chunkMetadata.getStartTime();
                long pageHeaderEndTime =
                    isHasStatistic ? pageHeader.getEndTime() : chunkMetadata.getEndTime();
                if (!validateTimeFrame(
                    alignedTimeBatch,
                    pageTimestamps,
                    pageHeaderStartTime,
                    pageHeaderEndTime,
                    resource)) {
                  return false;
                }
                alignedTimeBatch.add(pageTimestamps);
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                  == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                valuePageReader.nextValueBatch(alignedTimeBatch.get(pageIndex));
              } else { // NonAligned Chunk
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
                long pageHeaderStartTime =
                    isHasStatistic ? pageHeader.getStartTime() : chunkMetadata.getStartTime();
                long pageHeaderEndTime =
                    isHasStatistic ? pageHeader.getEndTime() : chunkMetadata.getEndTime();
                long pageStartTime = Long.MAX_VALUE;
                long previousTime = Long.MIN_VALUE;

                while (batchData.hasCurrent()) {
                  long currentTime = batchData.currentTime();
                  if (!lastNoAlignedPageTimeStamps.isEmpty()
                      && currentTime <= lastNoAlignedPageTimeStamps.getLast()) {
                    logger.error(
                        StorageEngineMessages
                            .STORAGE_LOG_TIME_RANGES_OVERLAP_BETWEEN_PAGES_2A131465,
                        resource.getTsFilePath(),
                        VALIDATE_FAILED);
                    return false;
                  }
                  if (currentTime <= previousTime) {
                    logger.error(
                        StorageEngineMessages
                            .STORAGE_LOG_THE_TIMESTAMP_IN_THE_PAGE_IS_REPEATED_OR_NOT_INCREMENTAL_04627FDA,
                        resource.getTsFilePath(),
                        VALIDATE_FAILED);
                    return false;
                  }
                  pageStartTime = Math.min(pageStartTime, currentTime);
                  previousTime = currentTime;
                  lastNoAlignedPageTimeStamps.add(currentTime);
                  batchData.next();
                }
                if (pageHeaderStartTime != pageStartTime) {
                  logger.error(
                      StorageEngineMessages
                          .STORAGE_LOG_THE_START_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_C23CE8D4,
                      resource.getTsFilePath(),
                      VALIDATE_FAILED);
                  return false;
                }
                if (pageHeaderEndTime != previousTime) {
                  logger.error(
                      StorageEngineMessages
                          .STORAGE_LOG_THE_END_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_5E363FAB,
                      resource.getTsFilePath(),
                      VALIDATE_FAILED);
                  return false;
                }
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }

            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            valueColumn2TimeBatchIndex.clear();
            alignedTimeBatches.clear();
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            if (chunkGroupHeader.getDeviceID() == null
                || chunkGroupHeader.getDeviceID().isEmpty()) {
              logger.error(
                  StorageEngineMessages.STORAGE_LOG_DEVICE_ID_IS_NULL_OR_EMPTY_635DD75C,
                  resource.getTsFilePath(),
                  VALIDATE_FAILED);
              return false;
            }
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

    } catch (IOException
        | NegativeArraySizeException
        | IllegalArgumentException
        | ArrayIndexOutOfBoundsException e) {
      logger.error(StorageEngineMessages.ERROR_VALIDATING_TSFILE, resource.getTsFilePath(), e);
      return false;
    }
    return true;
  }

  private static boolean validateTimeFrame(
      List<long[]> timeBatch,
      long[] pageTimestamps,
      long pageHeaderStartTime,
      long pageHeaderEndTime,
      TsFileResource tsFileResource) {
    if (pageHeaderStartTime != pageTimestamps[0]) {
      logger.error(
          StorageEngineMessages
              .STORAGE_LOG_THE_START_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_C23CE8D4,
          tsFileResource.getTsFilePath(),
          VALIDATE_FAILED);
      return false;
    }
    if (pageHeaderEndTime != pageTimestamps[pageTimestamps.length - 1]) {
      logger.error(
          StorageEngineMessages
              .STORAGE_LOG_THE_END_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_5E363FAB,
          tsFileResource.getTsFilePath(),
          VALIDATE_FAILED);
      return false;
    }
    for (int i = 0; i < pageTimestamps.length - 1; i++) {
      if (pageTimestamps[i + 1] <= pageTimestamps[i]) {
        logger.error(
            StorageEngineMessages
                .STORAGE_LOG_THE_TIMESTAMP_IN_THE_PAGE_IS_REPEATED_OR_NOT_INCREMENTAL_04627FDA,
            tsFileResource.getTsFilePath(),
            VALIDATE_FAILED);
        return false;
      }
    }

    if (timeBatch.size() >= 1) {
      long[] lastPageTimes = timeBatch.get(timeBatch.size() - 1);
      if (lastPageTimes[lastPageTimes.length - 1] >= pageTimestamps[0]) {
        logger.error(
            StorageEngineMessages.STORAGE_LOG_TIME_RANGES_OVERLAP_BETWEEN_PAGES_2A131465,
            tsFileResource.getTsFilePath(),
            VALIDATE_FAILED);
        return false;
      }
    }
    return true;
  }

  public static Map<Long, IChunkMetadata> getChunkMetadata(TsFileSequenceReader reader)
      throws IOException {
    Map<Long, IChunkMetadata> offset2ChunkMetadata = new HashMap<>();
    Map<IDeviceID, List<TimeseriesMetadata>> device2Metadata =
        reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
      for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
        for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
          offset2ChunkMetadata.put(chunkMetadata.getOffsetOfChunkHeader(), chunkMetadata);
        }
      }
    }
    return offset2ChunkMetadata;
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static boolean validateTsFileResourcesHasNoOverlap(List<TsFileResource> resources) {
    // deviceID -> <TsFileResource, last end time>
    Map<IDeviceID, Pair<TsFileResource, Long>> lastEndTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      ArrayDeviceTimeIndex timeIndex;
      if (resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE) {
        // if time index is not device time index, then deserialize it from resource file
        try {
          timeIndex = CompactionUtils.buildDeviceTimeIndex(resource);
        } catch (IOException e) {
          // skip such files
          continue;
        }
      } else {
        timeIndex = (ArrayDeviceTimeIndex) resource.getTimeIndex();
      }
      if (timeIndex == null) {
        return false;
      }
      Set<IDeviceID> devices = timeIndex.getDevices();
      for (IDeviceID device : devices) {
        // iterating the index, must present
        long currentStartTime = timeIndex.getStartTime(device).get();
        long currentEndTime = timeIndex.getEndTime(device).get();
        Pair<TsFileResource, Long> lastDeviceInfo =
            lastEndTimeMap.computeIfAbsent(device, x -> new Pair<>(null, Long.MIN_VALUE));
        long lastEndTime = lastDeviceInfo.right;
        if (lastEndTime >= currentStartTime) {
          logger.error(
              StorageEngineMessages
                  .STORAGE_LOG_DEVICE_IS_OVERLAPPED_BETWEEN_AND_END_TIME_IN_IS_START_TIME_BA49D2AA,
              device.toString(),
              lastDeviceInfo.left,
              resource,
              lastDeviceInfo.left,
              lastEndTime,
              resource,
              currentStartTime);
          return false;
        }
        lastDeviceInfo.left = resource;
        lastDeviceInfo.right = currentEndTime;
        lastEndTimeMap.put(device, lastDeviceInfo);
      }
    }
    return true;
  }

  public static void updateTsFileResource(
      TsFileSequenceReader reader, TsFileResource tsFileResource) throws IOException {
    updateTsFileResource(reader.getAllTimeseriesMetadata(false), tsFileResource, false);
    tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
  }

  public static void updateTsFileResource(
      Map<IDeviceID, List<TimeseriesMetadata>> device2Metadata,
      TsFileResource tsFileResource,
      boolean cacheLastValue) {
    // For async recover tsfile, there might be a FileTimeIndex, we need a new newTimeIndex
    ITimeIndex newTimeIndex =
        tsFileResource.getTimeIndex().getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE
            ? config.getTimeIndexLevel().getTimeIndex()
            : tsFileResource.getTimeIndex();
    Map<IDeviceID, List<Pair<String, TimeValuePair>>> deviceLastValues =
        tsFileResource.getLastValues();
    long lastValueMemCost = 0;
    if (cacheLastValue && deviceLastValues == null) {
      deviceLastValues = new HashMap<>(device2Metadata.size());
    }
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
      List<Pair<String, TimeValuePair>> seriesLastValues = null;
      if (deviceLastValues != null) {
        seriesLastValues = new ArrayList<>(entry.getValue().size());
      }

      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        newTimeIndex.updateStartTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
        newTimeIndex.updateEndTime(entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
        if (deviceLastValues != null) {
          if (timeseriesMetaData.getTsDataType() != TSDataType.BLOB) {
            TsPrimitiveType value;
            value =
                TsPrimitiveType.getByType(
                    timeseriesMetaData.getTsDataType() == TSDataType.VECTOR
                        ? TSDataType.INT64
                        : timeseriesMetaData.getTsDataType(),
                    timeseriesMetaData.getStatistics().getLastValue());
            seriesLastValues.add(
                new Pair<>(
                    timeseriesMetaData.getMeasurementId(),
                    new TimeValuePair(timeseriesMetaData.getStatistics().getEndTime(), value)));
          } else {
            seriesLastValues.add(new Pair<>(timeseriesMetaData.getMeasurementId(), null));
          }
        }
      }
      if (deviceLastValues != null) {
        lastValueMemCost += entry.getKey().ramBytesUsed();
        for (Pair<String, TimeValuePair> lastValue : seriesLastValues) {
          if (lastValue == null) {
            continue;
          }
          // pair
          lastValueMemCost += RamUsageEstimator.shallowSizeOf(lastValue);
          // measurement name
          lastValueMemCost += RamUsageEstimator.sizeOf(lastValue.left);
          TimeValuePair right = lastValue.getRight();
          lastValueMemCost +=
              right != null ? right.getSize() : RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        // ArrayList
        lastValueMemCost +=
            (long) seriesLastValues.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        lastValueMemCost += RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
        if (lastValueMemCost <= config.getCacheLastValuesMemoryBudgetInByte()) {
          deviceLastValues
              .computeIfAbsent(entry.getKey(), deviceID -> new ArrayList<>())
              .addAll(seriesLastValues);
        } else {
          deviceLastValues = null;
        }
      }
    }
    tsFileResource.setTimeIndex(newTimeIndex);
    tsFileResource.setLastValues(deviceLastValues);
  }

  /**
   * Generate {@link TsFileResource} from a closed {@link TsFileIOWriter}. Notice that the writer
   * should have executed {@link TsFileIOWriter#endFile()}. And this method will not record plan
   * Index of this writer.
   *
   * @param writer a {@link TsFileIOWriter}
   * @return a updated {@link TsFileResource}
   */
  public static TsFileResource generateTsFileResource(TsFileIOWriter writer) {
    TsFileResource resource = new TsFileResource(writer.getFile());
    for (ChunkGroupMetadata chunkGroupMetadata : writer.getChunkGroupMetadataList()) {
      IDeviceID device = chunkGroupMetadata.getDevice();
      for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        resource.updateStartTime(device, chunkMetadata.getStartTime());
        resource.updateEndTime(device, chunkMetadata.getEndTime());
      }
    }
    resource.setStatus(TsFileResourceStatus.NORMAL);
    return resource;
  }
}
