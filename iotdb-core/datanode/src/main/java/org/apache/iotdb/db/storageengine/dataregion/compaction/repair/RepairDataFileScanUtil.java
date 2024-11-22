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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionStatisticsCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionChunkReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.decryptAndUncompressPageData;

public class RepairDataFileScanUtil {
  private static final Logger logger = LoggerFactory.getLogger(RepairDataFileScanUtil.class);
  private final TsFileResource resource;
  private ArrayDeviceTimeIndex timeIndex;
  private boolean hasUnsortedDataOrWrongStatistics;
  private boolean isBrokenFile;
  private long previousTime;
  private boolean printLog;

  public RepairDataFileScanUtil(TsFileResource resource) {
    this(resource, false);
  }

  public RepairDataFileScanUtil(TsFileResource resource, boolean printLog) {
    this.resource = resource;
    this.hasUnsortedDataOrWrongStatistics = false;
    this.previousTime = Long.MIN_VALUE;
    this.printLog = printLog;
  }

  public void scanTsFile() {
    scanTsFile(false);
  }

  public void scanTsFile(boolean checkTsFileResource) {
    File tsfile = resource.getTsFile();
    try {
      timeIndex = checkTsFileResource ? getDeviceTimeIndex(resource) : null;
    } catch (IOException e) {
      logger.warn(
          "Meet error when read tsfile resource file {}, it may be repaired after reboot",
          tsfile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX,
          e);
      isBrokenFile = true;
      return;
    }
    try (TsFileSequenceReader reader =
        new CompactionTsFileReader(
            tsfile.getPath(),
            resource.isSeq()
                ? CompactionType.INNER_SEQ_COMPACTION
                : CompactionType.INNER_UNSEQ_COMPACTION)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      Set<IDeviceID> deviceIdsInTimeIndex =
          checkTsFileResource ? new HashSet<>(timeIndex.getDevices()) : Collections.emptySet();
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIterator.next();
        IDeviceID device = deviceIsAlignedPair.getLeft();
        if (checkTsFileResource) {
          if (!deviceIdsInTimeIndex.contains(device)) {
            throw new CompactionStatisticsCheckFailedException(
                device + " does not exist in the resource file");
          }
          deviceIdsInTimeIndex.remove(device);
        }
        MetadataIndexNode metadataIndexNode =
            deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
        TimeRange deviceTimeRangeInResource =
            checkTsFileResource
                ? new TimeRange(timeIndex.getStartTime(device), timeIndex.getEndTime(device))
                : null;
        boolean isAligned = deviceIsAlignedPair.getRight();
        if (isAligned) {
          checkAlignedDeviceSeries(
              reader, device, metadataIndexNode, deviceTimeRangeInResource, checkTsFileResource);
        } else {
          checkNonAlignedDeviceSeries(
              reader, device, metadataIndexNode, deviceTimeRangeInResource, checkTsFileResource);
        }
      }
      if (!deviceIdsInTimeIndex.isEmpty()) {
        throw new CompactionStatisticsCheckFailedException(
            "These devices (" + deviceIdsInTimeIndex + ") do not exist in the tsfile");
      }
    } catch (CompactionLastTimeCheckFailedException lastTimeCheckFailedException) {
      this.hasUnsortedDataOrWrongStatistics = true;
      if (printLog) {
        logger.error(
            "File {} has unsorted data: ",
            resource.getTsFile().getPath(),
            lastTimeCheckFailedException);
      }
    } catch (CompactionStatisticsCheckFailedException compactionStatisticsCheckFailedException) {
      this.hasUnsortedDataOrWrongStatistics = true;
      if (printLog) {
        logger.error(
            "File {} has wrong time statistics: ",
            resource.getTsFile().getPath(),
            compactionStatisticsCheckFailedException);
      }
    } catch (Exception e) {
      // ignored the exception caused by thread interrupt
      if (Thread.currentThread().isInterrupted()) {
        return;
      }
      // source file may be deleted
      if (!resource.tsFileExists()) {
        return;
      }
      logger.warn("Meet error when read tsfile {}", tsfile.getAbsolutePath(), e);
      isBrokenFile = true;
    }
  }

  private void checkAlignedDeviceSeries(
      TsFileSequenceReader reader,
      IDeviceID device,
      MetadataIndexNode metadataIndexNode,
      TimeRange deviceTimeRangeInResource,
      boolean checkTsFileResource)
      throws IOException {
    List<TimeseriesMetadata> timeColumnTimeseriesMetadata = new ArrayList<>(1);
    reader.readITimeseriesMetadata(timeColumnTimeseriesMetadata, metadataIndexNode, "");
    TimeseriesMetadata timeseriesMetadata = timeColumnTimeseriesMetadata.get(0);

    // check device time range
    TimeRange timeseriesTimeRange =
        new TimeRange(
            timeseriesMetadata.getStatistics().getStartTime(),
            timeseriesMetadata.getStatistics().getEndTime());
    if (checkTsFileResource && !timeseriesTimeRange.equals(deviceTimeRangeInResource)) {
      throw new CompactionStatisticsCheckFailedException(
          device, deviceTimeRangeInResource, timeseriesTimeRange);
    }

    long actualTimeseriesStartTime = Long.MAX_VALUE;
    long actualTimeseriesEndTime = Long.MIN_VALUE;
    List<ChunkMetadata> timeChunkMetadataList =
        reader.readChunkMetaDataList(timeColumnTimeseriesMetadata.get(0));
    for (ChunkMetadata timeChunkMetadata : timeChunkMetadataList) {
      actualTimeseriesStartTime =
          Math.min(actualTimeseriesStartTime, timeChunkMetadata.getStartTime());
      actualTimeseriesEndTime = Math.max(actualTimeseriesEndTime, timeChunkMetadata.getEndTime());
      checkTimeChunkInAlignedSeries(reader, device, timeChunkMetadata);
    }

    // reset previousTime
    previousTime = Long.MIN_VALUE;

    // check timeseries time range
    if (actualTimeseriesStartTime > actualTimeseriesEndTime) {
      return;
    }
    TimeRange actualTimeseriesTimeRange =
        new TimeRange(actualTimeseriesStartTime, actualTimeseriesEndTime);
    if (!actualTimeseriesTimeRange.equals(timeseriesTimeRange)) {
      throw new CompactionStatisticsCheckFailedException(
          device, timeseriesMetadata, actualTimeseriesTimeRange);
    }
  }

  private void checkTimeChunkInAlignedSeries(
      TsFileSequenceReader reader, IDeviceID device, ChunkMetadata timeChunkMetadata)
      throws IOException {
    Chunk timeChunk = reader.readMemChunk(timeChunkMetadata);

    CompactionChunkReader chunkReader = new CompactionChunkReader(timeChunk);
    ByteBuffer chunkDataBuffer = timeChunk.getData();
    ChunkHeader chunkHeader = timeChunk.getHeader();
    long actualChunkStartTime = Long.MAX_VALUE;
    long actualChunkEndTime = Long.MIN_VALUE;
    while (chunkDataBuffer.hasRemaining()) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader = null;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, timeChunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      actualChunkStartTime = Math.min(actualChunkStartTime, pageHeader.getStartTime());
      actualChunkEndTime = Math.max(actualChunkEndTime, pageHeader.getEndTime());
      ByteBuffer pageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);
      IDecryptor decryptor = IDecryptor.getDecryptor(timeChunk.getEncryptParam());
      ByteBuffer uncompressedPageData =
          decryptAndUncompressPageData(
              pageHeader,
              IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
              pageData,
              decryptor);
      validateTimeData(device, uncompressedPageData, pageHeader);
    }
    if (actualChunkStartTime > actualChunkEndTime) {
      return;
    }
    TimeRange actualChunkTimeRange = new TimeRange(actualChunkStartTime, actualChunkEndTime);
    if (!actualChunkTimeRange.equals(
        new TimeRange(timeChunkMetadata.getStartTime(), timeChunkMetadata.getEndTime()))) {
      throw new CompactionStatisticsCheckFailedException(
          device, timeChunkMetadata, actualChunkTimeRange);
    }
  }

  private void checkNonAlignedDeviceSeries(
      TsFileSequenceReader reader,
      IDeviceID device,
      MetadataIndexNode metadataIndexNode,
      TimeRange deviceTimeRangeInResource,
      boolean checkTsFileResource)
      throws IOException {
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    reader.getDeviceTimeseriesMetadata(
        timeseriesMetadataList, metadataIndexNode, Collections.emptySet(), true);
    long actualDeviceStartTime = Long.MAX_VALUE;
    long actualDeviceEndTime = Long.MIN_VALUE;
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      actualDeviceStartTime =
          Math.min(actualDeviceStartTime, timeseriesMetadata.getStatistics().getStartTime());
      actualDeviceEndTime =
          Math.max(actualDeviceStartTime, timeseriesMetadata.getStatistics().getEndTime());
      checkSingleNonAlignedSeries(reader, device, timeseriesMetadata);
      previousTime = Long.MIN_VALUE;
    }

    if (!checkTsFileResource || actualDeviceStartTime > actualDeviceEndTime) {
      return;
    }
    TimeRange actualDeviceTimeRange = new TimeRange(actualDeviceStartTime, actualDeviceEndTime);
    if (!actualDeviceTimeRange.equals(deviceTimeRangeInResource)) {
      throw new CompactionStatisticsCheckFailedException(
          device, deviceTimeRangeInResource, actualDeviceTimeRange);
    }
  }

  private void checkSingleNonAlignedSeries(
      TsFileSequenceReader reader, IDeviceID deviceID, TimeseriesMetadata timeseriesMetadata)
      throws IOException {
    TimeRange timeseriesTimeRange =
        new TimeRange(
            timeseriesMetadata.getStatistics().getStartTime(),
            timeseriesMetadata.getStatistics().getEndTime());
    long actualTimeseriesStartTime = Long.MAX_VALUE;
    long actualTimeseriesEndTime = Long.MIN_VALUE;
    for (IChunkMetadata iChunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
      ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
      actualTimeseriesStartTime = Math.min(actualTimeseriesStartTime, chunkMetadata.getStartTime());
      actualTimeseriesEndTime = Math.max(actualTimeseriesEndTime, chunkMetadata.getEndTime());
      checkChunkOfNonAlignedSeries(reader, deviceID, chunkMetadata);
    }
    if (actualTimeseriesStartTime > actualTimeseriesEndTime) {
      return;
    }
    TimeRange actualTimeseriesTimeRange =
        new TimeRange(actualTimeseriesStartTime, actualTimeseriesEndTime);
    if (!actualTimeseriesTimeRange.equals(timeseriesTimeRange)) {
      throw new CompactionStatisticsCheckFailedException(
          deviceID, timeseriesMetadata, actualTimeseriesTimeRange);
    }
  }

  private void checkChunkOfNonAlignedSeries(
      TsFileSequenceReader reader, IDeviceID deviceID, ChunkMetadata chunkMetadata)
      throws IOException {
    Chunk chunk = reader.readMemChunk(chunkMetadata);
    ChunkHeader chunkHeader = chunk.getHeader();
    CompactionChunkReader chunkReader = new CompactionChunkReader(chunk);
    ByteBuffer chunkDataBuffer = chunk.getData();
    long actualChunkStartTime = Long.MAX_VALUE;
    long actualChunkEndTime = Long.MIN_VALUE;
    while (chunkDataBuffer.hasRemaining()) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader = null;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      actualChunkStartTime = Math.min(actualChunkStartTime, pageHeader.getStartTime());
      actualChunkEndTime = Math.max(actualChunkEndTime, pageHeader.getEndTime());
      ByteBuffer pageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);
      IDecryptor decryptor = IDecryptor.getDecryptor(chunk.getEncryptParam());
      ByteBuffer uncompressedPageData =
          decryptAndUncompressPageData(
              pageHeader,
              IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
              pageData,
              decryptor);
      ByteBuffer timeBuffer = getTimeBufferFromNonAlignedPage(uncompressedPageData);
      validateTimeData(deviceID, timeBuffer, pageHeader);
    }
    if (actualChunkStartTime > actualChunkEndTime) {
      return;
    }
    TimeRange actualChunkTimeRange = new TimeRange(actualChunkStartTime, actualChunkEndTime);
    if (!actualChunkTimeRange.equals(
        new TimeRange(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()))) {
      throw new CompactionStatisticsCheckFailedException(
          deviceID, chunkMetadata, actualChunkTimeRange);
    }
  }

  private ByteBuffer getTimeBufferFromNonAlignedPage(ByteBuffer uncompressedPageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(uncompressedPageData);

    ByteBuffer timeBuffer = uncompressedPageData.slice();
    timeBuffer.limit(timeBufferLength);
    return timeBuffer;
  }

  private void validateTimeData(
      IDeviceID device, ByteBuffer uncompressedTimeData, PageHeader pageHeader) throws IOException {
    Decoder decoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    TimeRange pageHeaderTimeRange =
        new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime());
    long actualStartTime = Long.MAX_VALUE;
    long actualEndTime = Long.MIN_VALUE;
    while (decoder.hasNext(uncompressedTimeData)) {
      long currentTime = decoder.readLong(uncompressedTimeData);
      actualStartTime = Math.min(actualStartTime, currentTime);
      actualEndTime = Math.max(actualEndTime, currentTime);
      checkPreviousTimeAndUpdate(device, currentTime);
    }
    if (actualStartTime > actualEndTime) {
      return;
    }
    TimeRange actualPageTimeRange = new TimeRange(actualStartTime, actualEndTime);
    if (!actualPageTimeRange.equals(pageHeaderTimeRange)) {
      throw new CompactionStatisticsCheckFailedException(device, pageHeader, actualPageTimeRange);
    }
  }

  private void checkPreviousTimeAndUpdate(IDeviceID deviceID, String measurementId, long time) {
    if (previousTime >= time) {
      throw new CompactionLastTimeCheckFailedException(
          deviceID.toString() + TsFileConstant.PATH_SEPARATOR + measurementId, time, previousTime);
    }
    previousTime = time;
  }

  private void checkPreviousTimeAndUpdate(IDeviceID deviceID, long time) {
    if (previousTime >= time) {
      throw new CompactionLastTimeCheckFailedException(deviceID.toString(), time, previousTime);
    }
    previousTime = time;
  }

  public boolean hasUnsortedDataOrWrongStatistics() {
    return hasUnsortedDataOrWrongStatistics;
  }

  public boolean isBrokenFile() {
    return isBrokenFile;
  }

  public static List<TsFileResource> checkTimePartitionHasOverlap(
      List<TsFileResource> resources, boolean printOverlappedDevices) {
    List<TsFileResource> overlapResources = new ArrayList<>();
    Map<IDeviceID, Long> deviceEndTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      if (resource.getStatus() == TsFileResourceStatus.UNCLOSED
          || resource.getStatus() == TsFileResourceStatus.DELETED) {
        continue;
      }
      ArrayDeviceTimeIndex deviceTimeIndex;
      try {
        deviceTimeIndex = getDeviceTimeIndex(resource);
      } catch (Exception ignored) {
        continue;
      }

      Set<IDeviceID> devices = deviceTimeIndex.getDevices();
      boolean fileHasOverlap = false;
      // check overlap
      for (IDeviceID device : devices) {
        long deviceStartTimeInCurrentFile = deviceTimeIndex.getStartTime(device);
        if (deviceStartTimeInCurrentFile > deviceTimeIndex.getEndTime(device)) {
          continue;
        }
        if (!deviceEndTimeMap.containsKey(device)) {
          continue;
        }
        long deviceEndTimeInPreviousFile = deviceEndTimeMap.get(device);
        if (deviceStartTimeInCurrentFile <= deviceEndTimeInPreviousFile) {
          if (printOverlappedDevices) {
            logger.error(
                "Device {} has overlapped data, start time in current file is {}, end time in previous file is {}",
                device,
                deviceStartTimeInCurrentFile,
                deviceEndTimeInPreviousFile);
          }
          fileHasOverlap = true;
          overlapResources.add(resource);
          break;
        }
      }
      // update end time map
      if (!fileHasOverlap) {
        for (IDeviceID device : devices) {
          deviceEndTimeMap.put(device, deviceTimeIndex.getEndTime(device));
        }
      }
    }
    return overlapResources;
  }

  private static ArrayDeviceTimeIndex getDeviceTimeIndex(TsFileResource resource)
      throws IOException {
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof ArrayDeviceTimeIndex) {
      return (ArrayDeviceTimeIndex) timeIndex;
    }
    return CompactionUtils.buildDeviceTimeIndex(resource);
  }
}
