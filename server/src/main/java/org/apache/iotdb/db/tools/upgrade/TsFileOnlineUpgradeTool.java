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
package org.apache.iotdb.db.tools.upgrade;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.v2.read.TsFileSequenceReaderForV2;
import org.apache.iotdb.tsfile.v2.read.reader.page.PageReaderV2;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileOnlineUpgradeTool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileOnlineUpgradeTool.class);

  private TsFileSequenceReaderForV2 reader;
  private File oldTsFile;
  private List<Modification> oldModification;
  private Iterator<Modification> modsIterator;
  // new tsFile writer -> list of new modification
  private Map<TsFileIOWriter, ModificationFile> fileModificationMap;
  private Deletion currentMod;
  private Decoder defaultTimeDecoder = Decoder.getDecoderByType(
      TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
      TSDataType.INT64);
  private Decoder valueDecoder;

  // PartitionId -> TsFileIOWriter
  private Map<Long, TsFileIOWriter> partitionWriterMap;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size. Then the reader will skip the first TSFileConfig.OLD_MAGIC_STRING.length()
   * bytes of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileOnlineUpgradeTool(TsFileResource resourceToBeUpgraded) throws IOException {
    oldTsFile = resourceToBeUpgraded.getTsFile();
    String file = oldTsFile.getAbsolutePath();
    reader = new TsFileSequenceReaderForV2(file);
    partitionWriterMap = new HashMap<>();
    if (FSFactoryProducer.getFSFactory().getFile(file +
        ModificationFile.FILE_SUFFIX).exists()) {
      oldModification = (List<Modification>) resourceToBeUpgraded.getModFile().getModifications();
      modsIterator = oldModification.iterator();
      fileModificationMap = new HashMap<>();
    }
  }

  /**
   * upgrade a single TsFile
   *
   * @param tsFileName old version tsFile's absolute path
   * @param upgradedResources new version tsFiles' resources
   */
  public static void upgradeOneTsfile(TsFileResource resourceToBeUpgraded, List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {
    try (TsFileOnlineUpgradeTool updater = new TsFileOnlineUpgradeTool(resourceToBeUpgraded)) {
      updater.upgradeFile(upgradedResources);
    }
  }

  public void close() throws IOException {
    this.reader.close();
  }

  /**
   * upgrade file resource
   *
   * @throws IOException, WriteProcessException
   */
  @SuppressWarnings({ "squid:S3776", "deprecation" }) // Suppress high Cognitive Complexity warning
  private void upgradeFile(List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {

    // check if the old TsFile has correct header
    if (!fileCheck()) {
      return;
    }

    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length +
        TSFileConfig.VERSION_NUMBER_V2.getBytes().length;
    reader.position(headerLength);
    // start to scan chunks and chunkGroups
    boolean newChunkGroup = true;
    List<List<PageHeader>> pageHeadersInChunkGroup = new ArrayList<>();
    List<List<ByteBuffer>> pageDataInChunkGroup = new ArrayList<>();
    List<List<Boolean>> needToDecodeInfoInChunkGroup = new ArrayList<>();
    byte marker;
    List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
            }
            ChunkHeader header = reader.readChunkHeader();
            MeasurementSchema measurementSchema = new MeasurementSchema(
                header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(),
                header.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            TSDataType dataType = header.getDataType();
            TSEncoding encoding = header.getEncodingType();
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            List<Boolean> needToDecodeInfo = new ArrayList<>();
            for (int j = 0; j < header.getNumOfPages(); j++) {
              // a new Page
              PageHeader pageHeader = reader.readPageHeader(dataType);
              boolean needToDecode = 
                  checkIfNeedToDecode(dataType, encoding, pageHeader);
              needToDecodeInfo.add(needToDecode);
              ByteBuffer pageData = !needToDecode
                  ? reader.readCompressedPage(pageHeader) 
                  : reader.readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
            }
            pageHeadersInChunkGroup.add(pageHeadersInChunk);
            pageDataInChunkGroup.add(dataInChunk);
            needToDecodeInfoInChunkGroup.add(needToDecodeInfo);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // this is the footer of a ChunkGroup in TsFileV2.
            ChunkGroupHeader chunkGroupFooter = reader.readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();
            rewrite(deviceID, measurementSchemaList, pageHeadersInChunkGroup,
                pageDataInChunkGroup, needToDecodeInfoInChunkGroup);
            pageHeadersInChunkGroup.clear();
            pageDataInChunkGroup.clear();
            measurementSchemaList.clear();
            needToDecodeInfoInChunkGroup.clear();
            newChunkGroup = true;
            break;
          case MetaMarker.VERSION:
            long version = reader.readVersion();
            // convert old Modification to new 
            if (oldModification != null && modsIterator.hasNext()) {
              if (currentMod == null) {
                currentMod = (Deletion) modsIterator.next();
              }
              if (currentMod.getFileOffset() <= version) {
                for (Entry<TsFileIOWriter, ModificationFile> entry 
                    : fileModificationMap.entrySet()) {
                  TsFileIOWriter tsFileIOWriter = entry.getKey();
                  ModificationFile newMods = entry.getValue();
                  newMods.write(new Deletion(currentMod.getPath(), 
                      tsFileIOWriter.getFile().length(),
                      currentMod.getStartTime(),
                      currentMod.getEndTime()));
                }
                currentMod = null;
              }
            }
            // write plan indices for ending memtable
            for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) { 
              tsFileIOWriter.writePlanIndices(); 
            }
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unrecognized marker detected, "
                + "this file may be corrupted");
        }
      }
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        upgradedResources.add(endFileAndGenerateResource(tsFileIOWriter));
      }
      // write the remain modification for new file
      if (oldModification != null) {
        while (currentMod != null || modsIterator.hasNext()) {
          if (currentMod == null) {
            currentMod = (Deletion) modsIterator.next();
          }
          for (Entry<TsFileIOWriter, ModificationFile> entry 
              : fileModificationMap.entrySet()) {
            TsFileIOWriter tsFileIOWriter = entry.getKey();
            ModificationFile newMods = entry.getValue();
            newMods.write(new Deletion(currentMod.getPath(), 
                tsFileIOWriter.getFile().length(),
                currentMod.getStartTime(),
                currentMod.getEndTime()));
          }
          currentMod = null;
        }
      }
    } catch (IOException e2) {
      throw new IOException("TsFile upgrade process cannot proceed at position " +
          reader.position() + "because: " + e2.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Due to TsFile version-3 changed the serialize way of integer in TEXT data and 
   * INT32 data with PLAIN encoding, and also add a sum statistic for BOOLEAN data,
   * these types of data need to decode to points and rewrite in new TsFile.
   */
  private boolean checkIfNeedToDecode(TSDataType dataType, TSEncoding encoding,
      PageHeader pageHeader) {
    return dataType == TSDataType.BOOLEAN ||
        dataType == TSDataType.TEXT ||
        (dataType == TSDataType.INT32 && encoding == TSEncoding.PLAIN) ||
        StorageEngine.getTimePartition(pageHeader.getStartTime()) 
        != StorageEngine.getTimePartition(pageHeader.getEndTime());
  }

  /**
   * This method is for rewriting the ChunkGroup which data is in the different time partitions. In
   * this case, we have to decode the data to points, and then rewrite the data points to different
   * chunkWriters, finally write chunks to their own upgraded TsFiles
   */
  private void rewrite(String deviceId, List<MeasurementSchema> schemas,
      List<List<PageHeader>> pageHeadersInChunkGroup, List<List<ByteBuffer>> dataInChunkGroup,
      List<List<Boolean>> needToDecodeInfoInChunkGroup)
      throws IOException, PageException {
    Map<Long, Map<MeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      MeasurementSchema schema = schemas.get(i);
      List<ByteBuffer> pageDataInChunk = dataInChunkGroup.get(i);
      List<PageHeader> pageHeadersInChunk = pageHeadersInChunkGroup.get(i);
      List<Boolean> needToDecodeInfoInChunk = needToDecodeInfoInChunkGroup.get(i);
      valueDecoder = Decoder
          .getDecoderByType(schema.getEncodingType(), schema.getType());
      boolean isOnlyOnePageChunk = pageDataInChunk.size() == 1;
      for (int j = 0; j < pageDataInChunk.size(); j++) {
        if (Boolean.TRUE.equals(needToDecodeInfoInChunk.get(j))) {
          decodeAndWritePageInToFiles(oldTsFile, schema, pageDataInChunk.get(j),
              chunkWritersInChunkGroup);
        } else {
          writePageInToFile(oldTsFile, schema, pageHeadersInChunk.get(j),
              pageDataInChunk.get(j), chunkWritersInChunkGroup, isOnlyOnePageChunk);
        }
      }
    }

    for (Entry<Long, Map<MeasurementSchema, ChunkWriterImpl>> entry : chunkWritersInChunkGroup
        .entrySet()) {
      long partitionId = entry.getKey();
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);
      tsFileIOWriter.startChunkGroup(deviceId);
      // write chunks to their own upgraded tsFiles
      for (IChunkWriter chunkWriter : entry.getValue().values()) {
        chunkWriter.writeToFileWriter(tsFileIOWriter);
      }
      tsFileIOWriter.endChunkGroup();
    }
  }

  private TsFileIOWriter getOrDefaultTsFileIOWriter(File oldTsFile, long partition) {
    return partitionWriterMap.computeIfAbsent(partition, k ->
        {
          File partitionDir = FSFactoryProducer.getFSFactory().getFile(oldTsFile.getParent()
              + File.separator + partition);
          if (!partitionDir.exists()) {
            partitionDir.mkdirs();
          }
          File newFile = FSFactoryProducer.getFSFactory().getFile(partitionDir 
              + File.separator + oldTsFile.getName());
          try {
            if (!newFile.createNewFile()) {
              logger.error("The TsFile {} has been created ", newFile);
              return null;
            }
            TsFileIOWriter writer = new TsFileIOWriter(newFile);
            if (oldModification != null) {
              fileModificationMap.put(writer, new ModificationFile(newFile + ModificationFile.FILE_SUFFIX));
            }
            return writer;
          } catch (IOException e) {
            logger.error("Create new TsFile {} failed ", newFile);
            return null;
          }
        }
    );
  }

  private void writePageInToFile(File oldTsFile, MeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Map<Long, Map<MeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup,
      boolean isOnlyOnePageChunk)
      throws PageException {
    long partitionId = StorageEngine.getTimePartition(pageHeader.getStartTime());
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
    Map<MeasurementSchema, ChunkWriterImpl> chunkWriters = chunkWritersInChunkGroup
        .getOrDefault(partitionId, new HashMap<>());
    ChunkWriterImpl chunkWriter = chunkWriters
        .getOrDefault(schema, new ChunkWriterImpl(schema));
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader, isOnlyOnePageChunk);
    chunkWriters.put(schema, chunkWriter);
    chunkWritersInChunkGroup.put(partitionId, chunkWriters);
  }

  private void decodeAndWritePageInToFiles(File oldTsFile, MeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, Map<MeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup)
      throws IOException {
    valueDecoder.reset();
    PageReaderV2 pageReader = new PageReaderV2(pageData, schema.getType(), valueDecoder,
        defaultTimeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      long partitionId = StorageEngine.getTimePartition(time);

      Map<MeasurementSchema, ChunkWriterImpl> chunkWriters = chunkWritersInChunkGroup
          .getOrDefault(partitionId, new HashMap<>());
      ChunkWriterImpl chunkWriter = chunkWriters
          .getOrDefault(schema, new ChunkWriterImpl(schema));
      getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
      switch (schema.getType()) {
        case INT32:
          chunkWriter.write(time, (int) value);
          break;
        case INT64:
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
          chunkWriter.write(time, (Binary) value);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schema.getType()));
      }
      batchData.next();
      chunkWriters.put(schema, chunkWriter);
      chunkWritersInChunkGroup.put(partitionId, chunkWriters);
    }
  }

  /**
   * check if the file to be upgraded has correct magic strings and version number
   */
  private boolean fileCheck() throws IOException {

    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    String versionNumber = reader.readVersionNumberV2();
    if (!versionNumber.equals(TSFileConfig.VERSION_NUMBER_V2)) {
      logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }

  private TsFileResource endFileAndGenerateResource(TsFileIOWriter tsFileIOWriter)
      throws IOException {
    tsFileIOWriter.endFile();
    TsFileResource tsFileResource = new TsFileResource(tsFileIOWriter.getFile());
    Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap = tsFileIOWriter
        .getDeviceTimeseriesMetadataMap();
    for (Map.Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap
        .entrySet()) {
      String device = entry.getKey();
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.setClosed(true);
    tsFileResource.serialize();
    return tsFileResource;
  }

}