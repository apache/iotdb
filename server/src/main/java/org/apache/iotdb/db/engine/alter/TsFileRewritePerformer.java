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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

/** This class is used to rewrite one tsfile with current encoding & compressionType. */
public class TsFileRewritePerformer implements ICompactionPerformer {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewritePerformer.class);

  private TsFileResource tsFileResource;
  private TsFileResource targetTsFileResource;
  // record the min time and max time to update the target resource
  private long minStartTimestamp = Long.MAX_VALUE;
  private long maxEndTimestamp = Long.MIN_VALUE;
  private CompactionTaskSummary summary;
  private boolean hasRewrite = true;

  private final AlteringRecordsCache alteringRecordsCache = AlteringRecordsCache.getInstance();

  public TsFileRewritePerformer() {}

  @Override
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    this.hasRewrite = execute();
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    if (targetFiles != null && targetFiles.size() > 0) {
      this.targetTsFileResource = targetFiles.get(0);
    }
  }

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    this.summary = summary;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> files) {
    if (files != null && files.size() > 0) {
      this.tsFileResource = files.get(0);
    }
  }

  /**
   * This function execute the rewrite task
   *
   * @return false if not rewrite
   */
  private boolean execute() throws IOException, MetadataException, InterruptedException {

    tsFileResource.readLock();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
        TsFileIOWriter writer = new TsFileIOWriter(targetTsFileResource.getTsFile())) {
      // fast done, Many files may have been rewrite at merge time
      boolean needRewrite = readFirstChunkMetaCheckNeedRewrite(reader);
      if (!needRewrite) {
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
          rewriteAlgined(reader, writer, device);
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
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      tsFileResource.readUnlock();
    }
    return true;
  }

  /** Read the first ChunkMeta and determine whether it needs to be rewrite */
  private boolean readFirstChunkMetaCheckNeedRewrite(TsFileSequenceReader reader)
      throws IOException {
    TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
    while (deviceIterator.hasNext()) {
      Pair<String, Boolean> deviceInfo = deviceIterator.next();
      String device = deviceInfo.left;
      boolean aligned = deviceInfo.right;
      Map<String, Pair<TSEncoding, CompressionType>> deviceRecords =
          alteringRecordsCache.getDeviceRecords(device);
      if (deviceRecords == null || deviceRecords.isEmpty()) {
        continue;
      }
      if (aligned) {
        Set<String> measurements = new HashSet<>();
        List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
        if (alignedChunkMetadatas == null || alignedChunkMetadatas.isEmpty()) {
          logger.warn("[alter timeseries] device({}) alignedChunkMetadatas is null", device);
          continue;
        }
        for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
          List<IChunkMetadata> valueChunkMetadataList =
              alignedChunkMetadata.getValueChunkMetadataList();
          if (valueChunkMetadataList == null || valueChunkMetadataList.isEmpty()) {
            continue;
          }
          for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
            if (chunkMetadata == null) {
              continue;
            }
            // Measurement may be distributed across many ChunkMetadata, using set to get the first
            // one
            boolean isFirst = measurements.add(chunkMetadata.getMeasurementUid());
            if (isFirst) {
              Pair<TSEncoding, CompressionType> cacheType =
                  deviceRecords.get(chunkMetadata.getMeasurementUid());
              if (cacheType == null) {
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
        for (String measurement : measurementMap.keySet()) {
          Pair<TSEncoding, CompressionType> cacheType = deviceRecords.get(measurement);
          if (cacheType == null) {
            continue;
          }
          List<ChunkMetadata> chunkMetadata = measurementMap.get(measurement);
          if (chunkMetadata == null || chunkMetadata.isEmpty()) {
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

  private boolean isTypeDiff(Pair<TSEncoding, CompressionType> cacheType, Chunk chunk)
      throws IOException {
    if (chunk == null) {
      throw new IOException("read chunk is null");
    }
    ChunkHeader header = chunk.getHeader();
    if (header == null) {
      throw new IOException("read chunk header is null");
    }
    // find it
    return header.getEncodingType() != cacheType.left
        || header.getCompressionType() != cacheType.right;
  }

  private void rewriteAlgined(TsFileSequenceReader reader, TsFileIOWriter writer, String device)
      throws IOException, InterruptedException {
    List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
    if (alignedChunkMetadatas == null || alignedChunkMetadatas.isEmpty()) {
      logger.warn("[alter timeseries] device({}) alignedChunkMetadatas is null", device);
      return;
    }
    Map<String, Pair<TSEncoding, CompressionType>> deviceRecords =
        alteringRecordsCache.getDeviceRecords(device);
    boolean isAlteringDevice = (deviceRecords != null && !deviceRecords.isEmpty());
    Pair<Boolean, Pair<List<IMeasurementSchema>, List<IMeasurementSchema>>> schemaPair =
        collectSchemaList(alignedChunkMetadatas, reader, deviceRecords, isAlteringDevice);
    Boolean allSame = schemaPair.left;
    if (!isAlteringDevice || allSame) {
      // fast: rewrite chunk data to tsfile
      alignedFastWrite(reader, writer, alignedChunkMetadatas);
    } else {
      // need to rewrite
      alignedRewritePoints(reader, writer, device, alignedChunkMetadatas, schemaPair);
    }
  }

  private void alignedRewritePoints(
      TsFileSequenceReader reader,
      TsFileIOWriter writer,
      String device,
      List<AlignedChunkMetadata> alignedChunkMetadatas,
      Pair<Boolean, Pair<List<IMeasurementSchema>, List<IMeasurementSchema>>> schemaPair)
      throws IOException, InterruptedException {
    Pair<List<IMeasurementSchema>, List<IMeasurementSchema>> listPair = schemaPair.right;
    List<IMeasurementSchema> schemaList = listPair.left;
    List<IMeasurementSchema> schemaOldList = listPair.right;
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(schemaList);
    TsFileAlignedSeriesReaderIterator readerIterator =
        new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadatas, schemaOldList);

    while (readerIterator.hasNext()) {
      checkThreadInterrupted();
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

  private void alignedFastWrite(
      TsFileSequenceReader reader,
      TsFileIOWriter writer,
      List<AlignedChunkMetadata> alignedChunkMetadatas)
      throws IOException, InterruptedException {
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
      // write time chunk
      IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
      Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
      writer.writeChunk(timeChunk, (ChunkMetadata) timeChunkMetadata);
      // write value chunks
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
        checkThreadInterrupted();
        if (chunkMetadata == null) {
          continue;
        }
        Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
        writer.writeChunk(chunk, (ChunkMetadata) chunkMetadata);
      }
    }
  }

  protected Pair<Boolean, Pair<List<IMeasurementSchema>, List<IMeasurementSchema>>>
      collectSchemaList(
          List<AlignedChunkMetadata> alignedChunkMetadatas,
          TsFileSequenceReader reader,
          Map<String, Pair<TSEncoding, CompressionType>> deviceRecords,
          boolean isAlteringDevice)
          throws IOException {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<IMeasurementSchema> schemaOldList = new ArrayList<>();
    Set<String> measurementSet = new HashSet<>();
    boolean allSame = true;
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
        if (chunkMetadata == null) {
          continue;
        }
        String measurementId = chunkMetadata.getMeasurementUid();
        if (measurementSet.contains(measurementId)) {
          continue;
        }
        Pair<TSEncoding, CompressionType> alterType = null;
        if (deviceRecords != null) {
          alterType = deviceRecords.get(measurementId);
        }
        measurementSet.add(measurementId);
        Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
        ChunkHeader header = chunk.getHeader();
        boolean findTarget = isAlteringDevice && alterType != null;
        MeasurementSchema measurementSchema =
            new MeasurementSchema(
                header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(),
                header.getCompressionType());
        if (!findTarget) {
          schemaList.add(measurementSchema);
        } else {
          allSame = false;
          schemaList.add(
              new MeasurementSchema(
                  header.getMeasurementID(),
                  header.getDataType(),
                  alterType.left,
                  alterType.right));
        }
        schemaOldList.add(measurementSchema);
      }
    }

    schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    schemaOldList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return new Pair<>(allSame, new Pair<>(schemaList, schemaOldList));
  }

  private void rewriteNotAligned(String device, TsFileSequenceReader reader, TsFileIOWriter writer)
      throws IOException, MetadataException {
    Map<String, List<ChunkMetadata>> measurementMap = reader.readChunkMetadataInDevice(device);
    if (measurementMap == null) {
      logger.warn("[alter timeseries] device({}) measurementMap is null", device);
      return;
    }
    Map<String, Pair<TSEncoding, CompressionType>> deviceRecords =
        alteringRecordsCache.getDeviceRecords(device);
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
      Pair<TSEncoding, CompressionType> alterType = null;
      if (deviceRecords != null) {
        alterType = deviceRecords.get(measurementId);
      }
      for (ChunkMetadata chunkMetadata : chunkMetadatas) {
        // old mem chunk
        Chunk currentChunk = reader.readMemChunk(chunkMetadata);
        ChunkHeader header = currentChunk.getHeader();
        TSEncoding curEncoding = header.getEncodingType();
        CompressionType curCompressionType = header.getCompressionType();
        if (chunkWriter == null) {
          // chunkWriter init
          if (alterType != null
              && (alterType.left != curEncoding || alterType.right != curCompressionType)) {
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
        if (alterType == null
            || (chunkWriter.getEncoding() == header.getEncodingType()
                && chunkWriter.getCompressionType() == header.getCompressionType())) {
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
      case VECTOR:
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  public boolean hasRewrite() {
    return hasRewrite;
  }

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.interrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format(
              "[alter timeseries] rewrite for target file %s abort",
              targetTsFileResource.toString()));
    }
  }
}
