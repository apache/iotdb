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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TsFileSplitByPartitionTool implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSplitByPartitionTool.class);

  protected TsFileSequenceReader reader;
  protected File oldTsFile;
  protected Collection<ModEntry> oldModification;
  protected TsFileResource oldTsFileResource;
  protected Iterator<ModEntry> modsIterator;

  protected Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
  protected Decoder valueDecoder;

  /** PartitionId -> TsFileIOWriter */
  protected Map<Long, TsFileIOWriter> partitionWriterMap;

  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the real data and rewrite to some
   * new tsFiles.
   *
   * @throws IOException If some I/O error occurs
   */
  public TsFileSplitByPartitionTool(TsFileResource resourceToBeRewritten) throws IOException {
    oldTsFileResource = resourceToBeRewritten;
    oldTsFile = resourceToBeRewritten.getTsFile();
    String file = oldTsFile.getAbsolutePath();
    reader = new TsFileSequenceReader(file);
    partitionWriterMap = new HashMap<>();
    if (resourceToBeRewritten.anyModFileExists()) {
      oldModification = resourceToBeRewritten.getAllModEntries();
      modsIterator = oldModification.iterator();
    }
  }

  /**
   * Rewrite an old file to the latest version
   *
   * @param resourceToBeRewritten the tsfile which to be rewrite
   * @param rewrittenResources the rewritten files
   */
  public static void rewriteTsFile(
      TsFileResource resourceToBeRewritten, List<TsFileResource> rewrittenResources)
      throws IOException, WriteProcessException, IllegalPathException {
    try (TsFileSplitByPartitionTool rewriteTool =
        new TsFileSplitByPartitionTool(resourceToBeRewritten)) {
      rewriteTool.parseAndRewriteFile(rewrittenResources);
    }
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  /**
   * Parse the old files and generate some new files according to the time partition interval.
   *
   * @throws IOException WriteProcessException
   */
  @SuppressWarnings({"squid:S3776", "deprecation"}) // Suppress high Cognitive Complexity warning
  public void parseAndRewriteFile(List<TsFileResource> rewrittenResources)
      throws IOException, WriteProcessException, IllegalPathException {
    // check if the TsFile has correct header
    if (!fileCheck()) {
      return;
    }
    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length;
    reader.position(headerLength);
    byte version = reader.readMarker();
    if (version != (byte) 3 && version != (byte) 4) {
      throw new WriteProcessException(
          "The version of this tsfile is too low, please upgrade it to the version 4.");
    }
    // start to scan chunks and chunkGroups
    byte marker;
    IDeviceID deviceId = null;
    boolean firstChunkInChunkGroup = true;
    long chunkHeaderOffset;
    try {
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            deviceId = chunkGroupHeader.getDeviceID();
            firstChunkInChunkGroup = true;
            endChunkGroup();
            break;
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            chunkHeaderOffset = reader.position() - 1;
            ChunkHeader header = reader.readChunkHeader(marker);
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    header.getMeasurementID(),
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());
            TSDataType dataType = header.getDataType();
            TSEncoding encoding = header.getEncodingType();
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            List<Boolean> needToDecodeInfo = new ArrayList<>();
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              // a new Page
              PageHeader pageHeader =
                  reader.readPageHeader(dataType, header.getChunkType() == MetaMarker.CHUNK_HEADER);
              boolean needToDecode = checkIfNeedToDecode(measurementSchema, deviceId, pageHeader);
              needToDecodeInfo.add(needToDecode);
              ByteBuffer pageData =
                  !needToDecode
                      ? reader.readCompressedPage(pageHeader)
                      : reader.readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
              dataSize -= pageHeader.getSerializedPageSize();
            }
            reWriteChunk(
                deviceId,
                firstChunkInChunkGroup,
                measurementSchema,
                pageHeadersInChunk,
                dataInChunk,
                needToDecodeInfo,
                chunkHeaderOffset);
            firstChunkInChunkGroup = false;
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            // write plan indices for ending memtable
            for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
              long tmpMinPlanIndex = reader.getMinPlanIndex();
              if (tmpMinPlanIndex < minPlanIndex) {
                minPlanIndex = tmpMinPlanIndex;
              }

              long tmpMaxPlanIndex = reader.getMaxPlanIndex();
              if (tmpMaxPlanIndex < maxPlanIndex) {
                maxPlanIndex = tmpMaxPlanIndex;
              }

              tsFileIOWriter.setMaxPlanIndex(tmpMinPlanIndex);
              tsFileIOWriter.setMaxPlanIndex(tmpMaxPlanIndex);
              tsFileIOWriter.writePlanIndices();
            }
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      endChunkGroup();
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        rewrittenResources.add(endFileAndGenerateResource(tsFileIOWriter));
      }

    } catch (IOException e2) {
      throw new IOException(
          "TsFile rewrite process cannot proceed at position "
              + reader.position()
              + "because: "
              + e2.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * If the page have no statistics or crosses multi partitions, will return true, otherwise return
   * false.
   */
  protected boolean checkIfNeedToDecode(
      MeasurementSchema schema, IDeviceID deviceId, PageHeader pageHeader) {
    if (pageHeader.getStatistics() == null) {
      return true;
    }
    // Decode is required if the page has data to be deleted. Otherwise, decode is not required
    if (oldModification != null) {
      modsIterator = oldModification.iterator();
      ModEntry currentDeletion;
      while (modsIterator.hasNext()) {
        currentDeletion = modsIterator.next();
        if (currentDeletion.affects(deviceId)
            && currentDeletion.affects(schema.getMeasurementName())
            && currentDeletion
                .getTimeRange()
                .contains(pageHeader.getStartTime(), pageHeader.getEndTime())) {
          return true;
        }
      }
    }
    return TimePartitionUtils.getTimePartitionId(pageHeader.getStartTime())
        != TimePartitionUtils.getTimePartitionId(pageHeader.getEndTime());
  }

  /**
   * This method is for rewriting the Chunk which data is in the different time partitions. In this
   * case, we have to decode the data to points, and then rewrite the data points to different
   * chunkWriters, finally write chunks to their own upgraded TsFiles.
   */
  protected void reWriteChunk(
      IDeviceID deviceId,
      boolean firstChunkInChunkGroup,
      MeasurementSchema schema,
      List<PageHeader> pageHeadersInChunk,
      List<ByteBuffer> pageDataInChunk,
      List<Boolean> needToDecodeInfoInChunk,
      long chunkHeaderOffset)
      throws IOException, PageException, IllegalPathException {
    valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());
    Map<Long, ChunkWriterImpl> partitionChunkWriterMap = new HashMap<>();
    for (int i = 0; i < pageDataInChunk.size(); i++) {
      if (Boolean.TRUE.equals(needToDecodeInfoInChunk.get(i))) {
        decodeAndWritePage(
            deviceId, schema, pageDataInChunk.get(i), partitionChunkWriterMap, chunkHeaderOffset);
      } else {
        writePage(
            schema, pageHeadersInChunk.get(i), pageDataInChunk.get(i), partitionChunkWriterMap);
      }
    }
    for (Entry<Long, ChunkWriterImpl> entry : partitionChunkWriterMap.entrySet()) {
      long partitionId = entry.getKey();
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);
      if (firstChunkInChunkGroup || !tsFileIOWriter.isWritingChunkGroup()) {
        tsFileIOWriter.startChunkGroup(deviceId);
      }
      // write chunks to their own upgraded tsFiles
      IChunkWriter chunkWriter = entry.getValue();
      chunkWriter.writeToFileWriter(tsFileIOWriter);
    }
  }

  protected void endChunkGroup() throws IOException {
    for (TsFileIOWriter tsFileIoWriter : partitionWriterMap.values()) {
      tsFileIoWriter.endChunkGroup();
    }
  }

  public String upgradeTsFileName(String oldTsFileName) {
    return oldTsFileName;
  }

  protected TsFileIOWriter getOrDefaultTsFileIOWriter(File oldTsFile, long partition) {
    return partitionWriterMap.computeIfAbsent(
        partition,
        k -> {
          File partitionDir =
              FSFactoryProducer.getFSFactory()
                  .getFile(oldTsFile.getParent() + File.separator + partition);
          if (!partitionDir.exists()) {
            partitionDir.mkdirs();
          }
          File newFile =
              FSFactoryProducer.getFSFactory()
                  .getFile(partitionDir + File.separator + upgradeTsFileName(oldTsFile.getName()));
          try {
            if (newFile.exists()) {
              LOGGER.debug("delete uncomplated file {}", newFile);
              Files.delete(newFile.toPath());
            }
            if (!newFile.createNewFile()) {
              LOGGER.error("Create new TsFile {} failed because it exists", newFile);
            }
            TsFileIOWriter writer = new TsFileIOWriter(newFile);
            return writer;
          } catch (IOException e) {
            LOGGER.error("Create new TsFile {} failed ", newFile, e);
            return null;
          }
        });
  }

  protected void writePage(
      MeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap)
      throws PageException {
    long partitionId = TimePartitionUtils.getTimePartitionId(pageHeader.getStartTime());
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
    ChunkWriterImpl chunkWriter =
        partitionChunkWriterMap.computeIfAbsent(partitionId, v -> new ChunkWriterImpl(schema));
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader);
  }

  protected void decodeAndWritePage(
      IDeviceID deviceId,
      MeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap,
      long chunkHeaderOffset)
      throws IOException, IllegalPathException {
    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, schema.getType(), valueDecoder, defaultTimeDecoder);
    // read delete time range from old modification file
    List<TimeRange> deleteIntervalList = getOldSortedDeleteIntervals(deviceId, schema);
    pageReader.setDeleteIntervalList(deleteIntervalList);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    rewritePageIntoFiles(batchData, schema, partitionChunkWriterMap);
  }

  private List<TimeRange> getOldSortedDeleteIntervals(
      IDeviceID deviceId, MeasurementSchema schema) {
    if (oldModification != null) {
      ChunkMetadata chunkMetadata = new ChunkMetadata();
      modsIterator = oldModification.iterator();
      ModEntry currentDeletion = null;
      while (modsIterator.hasNext()) {
        currentDeletion = modsIterator.next();
        // if deletion path match the chunkPath, then add the deletion to the list
        if (currentDeletion.affects(deviceId)
            && currentDeletion.affects(schema.getMeasurementName())) {
          chunkMetadata.insertIntoSortedDeletions(currentDeletion.getTimeRange());
        }
      }
      return chunkMetadata.getDeleteIntervalList();
    }
    return null;
  }

  protected void rewritePageIntoFiles(
      BatchData batchData,
      MeasurementSchema schema,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap) {
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      long partitionId = TimePartitionUtils.getTimePartitionId(time);

      ChunkWriterImpl chunkWriter =
          partitionChunkWriterMap.computeIfAbsent(partitionId, v -> new ChunkWriterImpl(schema));
      getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
      switch (schema.getType()) {
        case INT32:
        case DATE:
          chunkWriter.write(time, (int) value);
          break;
        case INT64:
        case TIMESTAMP:
          chunkWriter.write(time, (long) value);
          break;
        case FLOAT:
          chunkWriter.write(time, (float) value);
          break;
        case DOUBLE:
          chunkWriter.write(time, (double) value);
          break;
        case BOOLEAN:
          chunkWriter.write(time, (boolean) value);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          chunkWriter.write(time, (Binary) value);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schema.getType()));
      }
      batchData.next();
    }
    partitionChunkWriterMap
        .values()
        .forEach(
            writer -> {
              writer.sealCurrentPage();
            });
  }

  /** check if the file has correct magic strings and version number */
  protected boolean fileCheck() throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      LOGGER.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    byte versionNumber = reader.readVersionNumber();
    if (versionNumber != TSFileConfig.VERSION_NUMBER) {
      LOGGER.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      LOGGER.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }

  protected TsFileResource endFileAndGenerateResource(TsFileIOWriter tsFileIOWriter)
      throws IOException {
    Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        tsFileIOWriter.getDeviceTimeseriesMetadataMap();
    tsFileIOWriter.endFile();
    TsFileResource tsFileResource = new TsFileResource(tsFileIOWriter.getFile());
    for (Entry<IDeviceID, List<TimeseriesMetadata>> entry :
        deviceTimeseriesMetadataMap.entrySet()) {
      IDeviceID device = entry.getKey();
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.setMinPlanIndex(minPlanIndex);
    tsFileResource.setMaxPlanIndex(maxPlanIndex);
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    tsFileResource.serialize();
    return tsFileResource;
  }
}
