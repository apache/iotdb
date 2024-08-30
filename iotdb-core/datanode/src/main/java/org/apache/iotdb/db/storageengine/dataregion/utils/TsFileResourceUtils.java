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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

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
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Pair;
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
  private static final String VALIDATE_FAILED = "validate failed,";

  private TsFileResourceUtils() {
    // util class
  }

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
        logger.error("{} {} time index is null", resource.getTsFilePath(), VALIDATE_FAILED);
        return false;
      }
      Set<IDeviceID> devices = timeIndex.getDevices();
      if (devices.isEmpty()) {
        logger.error("{} {} empty resource", resource.getTsFilePath(), VALIDATE_FAILED);
        return false;
      }
      for (IDeviceID device : devices) {
        long startTime = timeIndex.getStartTime(device);
        long endTime = timeIndex.getEndTime(device);
        if (startTime == Long.MAX_VALUE) {
          logger.error(
              "{} {} the start time of {} is {}",
              resource.getTsFilePath(),
              VALIDATE_FAILED,
              device,
              Long.MAX_VALUE);
          return false;
        }
        if (endTime == Long.MIN_VALUE) {
          logger.error(
              "{} {} the end time of {} is {}",
              resource.getTsFilePath(),
              VALIDATE_FAILED,
              device,
              Long.MIN_VALUE);
          return false;
        }
        if (startTime > endTime) {
          logger.error(
              "{} {} the start time of {} is greater than end time",
              resource.getTsFilePath(),
              VALIDATE_FAILED,
              device);
          return false;
        }
      }
    } catch (IOException e) {
      logger.error("meet error when validate .resource file:{},e", resource.getTsFilePath());
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
              "target file %s is smaller than magic string and version number size", resource));
      return false;
    }
    return true;
  }

  public static boolean validateTsFileDataCorrectness(TsFileResource resource) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      if (!reader.isComplete()) {
        logger.error("{} {} illegal tsfile", resource.getTsFilePath(), VALIDATE_FAILED);
        return false;
      }

      Map<Long, IChunkMetadata> chunkMetadataMap = getChunkMetadata(reader);
      if (chunkMetadataMap.isEmpty()) {
        logger.error(
            "{} {} there is no data in the file", resource.getTsFilePath(), VALIDATE_FAILED);
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
                  "{} chunk start offset is inconsistent with the value in the metadata.",
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
                        "{} {} time ranges overlap between pages.",
                        resource.getTsFilePath(),
                        VALIDATE_FAILED);
                    return false;
                  }
                  if (currentTime <= previousTime) {
                    logger.error(
                        "{} {} the timestamp in the page is repeated or not incremental.",
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
                      "{} {} the start time in page is different from that in page header.",
                      resource.getTsFilePath(),
                      VALIDATE_FAILED);
                  return false;
                }
                if (pageHeaderEndTime != previousTime) {
                  logger.error(
                      "{} {} the end time in page is different from that in page header.",
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
                  "{} {} device id is null or empty.", resource.getTsFilePath(), VALIDATE_FAILED);
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
      logger.error("Meets error when validating TsFile {}, ", resource.getTsFilePath(), e);
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
          "{} {} the start time in page is different from that in page header.",
          tsFileResource.getTsFilePath(),
          VALIDATE_FAILED);
      return false;
    }
    if (pageHeaderEndTime != pageTimestamps[pageTimestamps.length - 1]) {
      logger.error(
          "{} {} the end time in page is different from that in page header.",
          tsFileResource.getTsFilePath(),
          VALIDATE_FAILED);
      return false;
    }
    for (int i = 0; i < pageTimestamps.length - 1; i++) {
      if (pageTimestamps[i + 1] <= pageTimestamps[i]) {
        logger.error(
            "{} {} the timestamp in the page is repeated or not incremental.",
            tsFileResource.getTsFilePath(),
            VALIDATE_FAILED);
        return false;
      }
    }

    if (timeBatch.size() >= 1) {
      long[] lastPageTimes = timeBatch.get(timeBatch.size() - 1);
      if (lastPageTimes[lastPageTimes.length - 1] >= pageTimestamps[0]) {
        logger.error(
            "{} {} time ranges overlap between pages.",
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
        long currentStartTime = timeIndex.getStartTime(device);
        long currentEndTime = timeIndex.getEndTime(device);
        Pair<TsFileResource, Long> lastDeviceInfo =
            lastEndTimeMap.computeIfAbsent(device, x -> new Pair<>(null, Long.MIN_VALUE));
        long lastEndTime = lastDeviceInfo.right;
        if (lastEndTime >= currentStartTime) {
          logger.error(
              "Device {} is overlapped between {} and {}, "
                  + "end time in {} is {}, start time in {} is {}",
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
    updateTsFileResource(reader.getAllTimeseriesMetadata(false), tsFileResource);
    tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
  }

  public static void updateTsFileResource(
      Map<IDeviceID, List<TimeseriesMetadata>> device2Metadata, TsFileResource tsFileResource) {
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
      }
    }
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
