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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.uncompressPageData;

public class RepairDataFileScanUtil {
  private static final Logger logger = LoggerFactory.getLogger(RepairDataFileScanUtil.class);
  private final TsFileResource resource;
  private boolean hasUnsortedData;
  private boolean isBrokenFile;
  private long previousTime;

  public RepairDataFileScanUtil(TsFileResource resource) {
    this.resource = resource;
    this.hasUnsortedData = false;
    this.previousTime = Long.MIN_VALUE;
  }

  public void scanTsFile() {
    File tsfile = resource.getTsFile();
    try (TsFileSequenceReader reader =
        new CompactionTsFileReader(
            tsfile.getPath(),
            resource.isSeq()
                ? CompactionType.INNER_SEQ_COMPACTION
                : CompactionType.INNER_UNSEQ_COMPACTION)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIterator.next();
        IDeviceID device = deviceIsAlignedPair.getLeft();
        MetadataIndexNode metadataIndexNode =
            deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
        boolean isAligned = deviceIsAlignedPair.getRight();
        if (isAligned) {
          checkAlignedDeviceSeries(reader, device, metadataIndexNode);
        } else {
          checkNonAlignedDeviceSeries(reader, device, metadataIndexNode);
        }
      }
    } catch (CompactionLastTimeCheckFailedException lastTimeCheckFailedException) {
      this.hasUnsortedData = true;
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
      TsFileSequenceReader reader, IDeviceID device, MetadataIndexNode metadataIndexNode)
      throws IOException {
    List<AlignedChunkMetadata> chunkMetadataList =
        reader.getAlignedChunkMetadataByMetadataIndexNode(device, metadataIndexNode);
    for (AlignedChunkMetadata alignedChunkMetadata : chunkMetadataList) {
      IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
      Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);

      CompactionChunkReader chunkReader = new CompactionChunkReader(timeChunk);
      ByteBuffer chunkDataBuffer = timeChunk.getData();
      ChunkHeader chunkHeader = timeChunk.getHeader();
      while (chunkDataBuffer.hasRemaining()) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader = null;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, timeChunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }
        ByteBuffer pageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

        ByteBuffer uncompressedPageData =
            uncompressPageData(
                pageHeader,
                IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
                pageData);
        Decoder decoder =
            Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
        while (decoder.hasNext(uncompressedPageData)) {
          long currentTime = decoder.readLong(uncompressedPageData);
          checkPreviousTimeAndUpdate(device, currentTime);
        }
      }
    }
    previousTime = Long.MIN_VALUE;
  }

  private void checkNonAlignedDeviceSeries(
      TsFileSequenceReader reader, IDeviceID device, MetadataIndexNode metadataIndexNode)
      throws IOException {
    Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
        reader.getMeasurementChunkMetadataListMapIterator(metadataIndexNode);
    while (measurementChunkMetadataListMapIterator.hasNext()) {
      Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap =
          measurementChunkMetadataListMapIterator.next();
      for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataListEntry :
          measurementChunkMetadataListMap.entrySet()) {
        List<ChunkMetadata> chunkMetadataList = measurementChunkMetadataListEntry.getValue();
        checkSingleNonAlignedSeries(
            reader, device, measurementChunkMetadataListEntry.getKey(), chunkMetadataList);
        previousTime = Long.MIN_VALUE;
      }
    }
  }

  private void checkSingleNonAlignedSeries(
      TsFileSequenceReader reader,
      IDeviceID deviceID,
      String measurementId,
      List<ChunkMetadata> chunkMetadataList)
      throws IOException {
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {
      if (chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0) {
        continue;
      }
      Chunk chunk = reader.readMemChunk(chunkMetadata);
      ChunkHeader chunkHeader = chunk.getHeader();
      CompactionChunkReader chunkReader = new CompactionChunkReader(chunk);
      ByteBuffer chunkDataBuffer = chunk.getData();
      while (chunkDataBuffer.hasRemaining()) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader = null;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }
        ByteBuffer pageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

        ByteBuffer uncompressedPageData =
            uncompressPageData(
                pageHeader,
                IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
                pageData);
        ByteBuffer timeBuffer = getTimeBufferFromNonAlignedPage(uncompressedPageData);
        Decoder timeDecoder =
            Decoder.getDecoderByType(
                TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                TSDataType.INT64);
        while (timeDecoder.hasNext(timeBuffer)) {
          long currentTime = timeDecoder.readLong(timeBuffer);
          checkPreviousTimeAndUpdate(deviceID, measurementId, currentTime);
        }
      }
    }
  }

  private ByteBuffer getTimeBufferFromNonAlignedPage(ByteBuffer uncompressedPageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(uncompressedPageData);

    ByteBuffer timeBuffer = uncompressedPageData.slice();
    timeBuffer.limit(timeBufferLength);
    return timeBuffer;
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

  public boolean hasUnsortedData() {
    return hasUnsortedData;
  }

  public boolean isBrokenFile() {
    return isBrokenFile;
  }

  public static List<TsFileResource> checkTimePartitionHasOverlap(List<TsFileResource> resources) {
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
