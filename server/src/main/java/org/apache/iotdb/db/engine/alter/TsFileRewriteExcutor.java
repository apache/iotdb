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
package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileAlignedSeriesReaderIterator;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** This class is used to rewrite one tsfile with current encoding & compressionType. */
public class TsFileRewriteExcutor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewriteExcutor.class);

  private final TsFileResource tsFileResource;
  private final TsFileResource targetTsFileResource;
  private boolean sequence;
  // record the min time and max time to update the target resource
  private long minStartTimestamp = Long.MAX_VALUE;
  private long maxEndTimestamp = Long.MIN_VALUE;

  private final AlteringRecordsCache alteringRecordsCache = AlteringRecordsCache.getInstance();

  public TsFileRewriteExcutor(
      TsFileResource tsFileResource,
      TsFileResource targetTsFileResource,
      boolean sequence) {
    this.tsFileResource = tsFileResource;
    this.targetTsFileResource = targetTsFileResource;
    this.sequence = sequence;
  }

  /**
   * This function execute the rewrite task
   * @return false if not rewrite
   * */
  public boolean execute() throws IOException {

    tsFileResource.readLock();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
        TsFileIOWriter writer = new TsFileIOWriter(targetTsFileResource.getTsFile())) {
      // fast done, Many files may have been rewrite at merge time
      boolean needRewrite = readFirstChunkMetaCheckNeedRewrite(reader);
      if(!needRewrite) {
        return false;
      }
      // read devices
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        minStartTimestamp = Long.MAX_VALUE;
        maxEndTimestamp = Long.MIN_VALUE;
        Pair<String, Boolean> deviceInfo = deviceIterator.next();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;
        // write chunkGroup header
        writer.startChunkGroup(device);
        // write chunk & page data
        if (aligned) {
//          rewriteAlgined(reader, writer, device);
        } else {
          rewriteNotAligned(device, reader, writer);
        }
        // chunkGroup end
        writer.endChunkGroup();

        targetTsFileResource.updateStartTime(device, minStartTimestamp);
        targetTsFileResource.updateEndTime(device, maxEndTimestamp);
      }

      targetTsFileResource.updatePlanIndexes(tsFileResource);
      // write index,bloom,footer, end file
      writer.endFile();
      targetTsFileResource.close();
    } finally {
      tsFileResource.readUnlock();
    }
    return true;
  }

  /**
   * Read the first ChunkMeta and determine whether it needs to be rewrite
   * @param reader
   * @return
   * @throws IOException
   */
  private boolean readFirstChunkMetaCheckNeedRewrite(TsFileSequenceReader reader) throws IOException {
    TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
    while (deviceIterator.hasNext()) {
      Pair<String, Boolean> deviceInfo = deviceIterator.next();
      String device = deviceInfo.left;
      boolean aligned = deviceInfo.right;
      Map<String, Pair<TSEncoding, CompressionType>> deviceRecords = alteringRecordsCache.getDeviceRecords(device);
      if(deviceRecords == null || deviceRecords.isEmpty()) {
        continue;
      }
      if(aligned) {
        Set<String> measurements = new HashSet<>();
        List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
        if (alignedChunkMetadatas == null || alignedChunkMetadatas.isEmpty()) {
          logger.warn("[alter timeseries] device({}) alignedChunkMetadatas is null", device);
          continue;
        }
        for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
          List<IChunkMetadata> valueChunkMetadataList =
                  alignedChunkMetadata.getValueChunkMetadataList();
          if(valueChunkMetadataList == null || valueChunkMetadataList.isEmpty()) {
            continue;
          }
          for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
            if (chunkMetadata == null) {
              continue;
            }
            // Measurement may be distributed across many ChunkMetadata, using set to get the first one
            boolean isFirst = measurements.add(chunkMetadata.getMeasurementUid());
            if(isFirst) {
              Pair<TSEncoding, CompressionType> cacheType = deviceRecords.get(chunkMetadata.getMeasurementUid());
              if(cacheType == null) {
                continue;
              }
              Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
              if (isTypeDiff(cacheType, chunk)) return true;
            }
          }
        }
      } else {
        Map<String, List<ChunkMetadata>> measurementMap = reader.readChunkMetadataInDevice(device);
        if (measurementMap == null || measurementMap.isEmpty()) {
          logger.warn("[alter timeseries] device({}) chunkMetadatas is null", device);
          continue;
        }
        Iterator<String> it = measurementMap.keySet().iterator();
        while(it.hasNext()) {
          String measurement = it.next();
          Pair<TSEncoding, CompressionType> cacheType = deviceRecords.get(measurement);
          if(cacheType == null) {
            continue;
          }
          List<ChunkMetadata> chunkMetadata = measurementMap.get(measurement);
          if(chunkMetadata == null || chunkMetadata.isEmpty()) {
            continue;
          }
          // read first chunk type
          ChunkMetadata curChunkMetadata = chunkMetadata.get(0);
          Chunk chunk = reader.readMemChunk(curChunkMetadata);
          if (isTypeDiff(cacheType, chunk)) return true;
        }
      }
    }
    // If and only if the first chunk type of all measurements is the same
    return false;
  }

  private boolean isTypeDiff(Pair<TSEncoding, CompressionType> cacheType, Chunk chunk) throws IOException {
    if (chunk == null) {
      throw new IOException("read chunk is null");
    }
    ChunkHeader header = chunk.getHeader();
    if (header == null) {
      throw new IOException("read chunk header is null");
    }
    if (header.getEncodingType() != cacheType.left || header.getCompressionType() != cacheType.right) {
      // find it
      return true;
    }
    return false;
  }

  private void rewriteAlgined(
      TsFileSequenceReader reader,
      TsFileIOWriter writer,
      String device,
      boolean isTargetDevice,
      String targetMeasurement)
      throws IOException {
    List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
    if (alignedChunkMetadatas == null || alignedChunkMetadatas.isEmpty()) {
      logger.warn("[alter timeseries] device({}) alignedChunkMetadatas is null", device);
      return;
    }
    // TODO To be optimized: Non-target modification measurements are directly written to data
    Pair<List<IMeasurementSchema>, List<IMeasurementSchema>> listPair =
        collectSchemaList(alignedChunkMetadatas, reader, targetMeasurement, isTargetDevice);
    List<IMeasurementSchema> schemaList = listPair.left;
    List<IMeasurementSchema> schemaOldList = listPair.right;
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(schemaList);
    TsFileAlignedSeriesReaderIterator readerIterator =
        new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadatas, schemaOldList);

    while (readerIterator.hasNext()) {
      Pair<AlignedChunkReader, Long> chunkReaderAndChunkSize = readerIterator.nextReader();
      AlignedChunkReader chunkReader = chunkReaderAndChunkSize.left;
      while (chunkReader.hasNextSatisfiedPage()) {
        IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
        while (batchDataIterator.hasNext()) {
          TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
          long time = batchDataIterator.currentTime();
          chunkWriter.write(time, pointsData);
          targetTsFileResource.updateStartTime(device, time);
          targetTsFileResource.updateEndTime(device, time);
          batchDataIterator.next();
        }
      }
    }
    chunkWriter.writeToFileWriter(writer);
  }

  protected Pair<List<IMeasurementSchema>, List<IMeasurementSchema>> collectSchemaList(
      List<AlignedChunkMetadata> alignedChunkMetadatas,
      TsFileSequenceReader reader,
      String targetMeasurement,
      boolean isTargetDevice)
      throws IOException {

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<IMeasurementSchema> schemaOldList = new ArrayList<>();
    Set<String> measurementSet = new HashSet<>();
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
        if (chunkMetadata == null) {
          continue;
        }
        if (measurementSet.contains(chunkMetadata.getMeasurementUid())) {
          continue;
        }
        measurementSet.add(chunkMetadata.getMeasurementUid());
        Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
        ChunkHeader header = chunk.getHeader();
        boolean findTarget =
            isTargetDevice
                && CommonUtils.equal(chunkMetadata.getMeasurementUid(), targetMeasurement);
        MeasurementSchema measurementSchema =
            new MeasurementSchema(
                header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(),
                header.getCompressionType());
        if (!findTarget) {
          schemaList.add(measurementSchema);
          schemaOldList.add(measurementSchema);
        } else {
//          schemaList.add(
//              new MeasurementSchema(
//                  header.getMeasurementID(),
//                  header.getDataType(),
//                  this.curEncoding,
//                  this.curCompressionType));
          schemaOldList.add(measurementSchema);
        }
      }
    }

    schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    schemaOldList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return new Pair<>(schemaList, schemaOldList);
  }

  private void rewriteNotAligned(
      String device,
      TsFileSequenceReader reader,
      TsFileIOWriter writer)
      throws IOException {
    Map<String, List<ChunkMetadata>> measurementMap = reader.readChunkMetadataInDevice(device);
    if (measurementMap == null) {
      logger.warn("[alter timeseries] device({}) measurementMap is null", device);
      return;
    }
    Map<String, Pair<TSEncoding, CompressionType>> deviceRecords = alteringRecordsCache.getDeviceRecords(device);
    boolean isAlteringDevice = (deviceRecords == null || deviceRecords.isEmpty());
    for (Map.Entry<String, List<ChunkMetadata>> next : measurementMap.entrySet()) {
      String measurementId = next.getKey();
      List<ChunkMetadata> chunkMetadatas = next.getValue();
      if (chunkMetadatas == null || chunkMetadatas.isEmpty()) {
        logger.warn("[alter timeseries] empty measurement({})", measurementId);
        return;
      }
      // target chunk writer
      ChunkMetadata firstChunkMetadata = chunkMetadatas.get(0);
      ChunkWriterImpl chunkWriter = null;
      Pair<TSEncoding, CompressionType> alterType = deviceRecords.get(measurementId);
      for (ChunkMetadata chunkMetadata : chunkMetadatas) {
        // old mem chunk
        Chunk currentChunk = reader.readMemChunk(chunkMetadata);
        ChunkHeader header = currentChunk.getHeader();
        TSEncoding curEncoding = header.getEncodingType();
        CompressionType curCompressionType = header.getCompressionType();
        if(chunkWriter == null) {
          // chunkWriter init
          if(isAlteringDevice && alterType != null && (alterType.left != curEncoding || alterType.right != curCompressionType)) {
            curEncoding = alterType.left;
            curCompressionType = alterType.right;
          }
          chunkWriter =
                  new ChunkWriterImpl(
                          new MeasurementSchema(
                                  measurementId,
                                  firstChunkMetadata.getDataType(),
                                  curEncoding,
                                  curCompressionType));
        }
        if (!isAlteringDevice || alterType == null || (chunkWriter.getEncoding() == curEncoding && chunkWriter.getCompressionType() == curCompressionType)) {
          // fast write chunk
          writer.writeChunk(currentChunk, chunkMetadata);
          continue;
        }
        IChunkReader chunkReader = new ChunkReader(currentChunk, null);
        while (chunkReader.hasNextSatisfiedPage()) {
          IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
          while (batchIterator.hasNextTimeValuePair()) {
            TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
            writeTimeAndValueToChunkWriter(chunkWriter, timeValuePair);
            if (timeValuePair.getTimestamp() > maxEndTimestamp) {
              maxEndTimestamp = timeValuePair.getTimestamp();
            }
            if (timeValuePair.getTimestamp() < minStartTimestamp) {
              minStartTimestamp = timeValuePair.getTimestamp();
            }
          }
        }
        // flush
        chunkWriter.writeToFileWriter(writer);
      }
    }
  }

  /** TODO Copy it from the compaction code and extract it later into the utility class */
  private void writeTimeAndValueToChunkWriter(
      ChunkWriterImpl chunkWriter, TimeValuePair timeValuePair) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }
}
