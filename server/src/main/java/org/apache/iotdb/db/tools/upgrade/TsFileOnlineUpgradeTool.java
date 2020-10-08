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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v1.file.metadata.ChunkGroupMetaDataV1;
import org.apache.iotdb.tsfile.v1.file.metadata.TsDeviceMetadataIndexV1;
import org.apache.iotdb.tsfile.v1.file.metadata.TsDeviceMetadataV1;
import org.apache.iotdb.tsfile.v1.file.metadata.TsFileMetadataV1;
import org.apache.iotdb.tsfile.v1.file.utils.HeaderUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileOnlineUpgradeTool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileOnlineUpgradeTool.class);

  private TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  private Decoder defaultTimeDecoder = Decoder.getDecoderByType(
      TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
      TSDataType.INT64);
  private Decoder valueDecoder;
  protected String file;

  // PartitionId -> TsFileIOWriter 
  private Map<Long, TsFileIOWriter> partitionWriterMap;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.OLD_MAGIC_STRING.length()
   * bytes of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileOnlineUpgradeTool(String file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsfileOnlineUpgradeTool.
   *
   * @param file -given file name
   * @param loadMetadataSize -load meta data size
   */
  public TsFileOnlineUpgradeTool(String file, boolean loadMetadataSize) throws IOException {
    this.file = file;
    tsFileInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(file);
    partitionWriterMap = new HashMap<>();
    try {
      if (loadMetadataSize) {
        loadMetadataSize();
      }
    } catch (Exception e) {
      tsFileInput.close();
      throw e;
    }
  }

  /**
   * upgrade a single tsfile
   *
   * @param tsFileName old version tsFile's absolute path
   * @param upgradedResources new version tsFiles' resources
   */
  public static void upgradeOneTsfile(String tsFileName, List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {
    try (TsFileOnlineUpgradeTool updater = new TsFileOnlineUpgradeTool(tsFileName)) {
      updater.upgradeFile(upgradedResources);
    }
  }

  /**
   *
   */
  public void loadMetadataSize() throws IOException {
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
    tsFileInput.read(metadataSize,
        tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES);
    metadataSize.flip();
    // read file metadata size and position
    fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
    fileMetadataPos =
        tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES
            - fileMetadataSize;
    // skip the magic header
    position(TSFileConfig.MAGIC_STRING.length());
  }

  public String readTailMagic() throws IOException {
    long totalSize = tsFileInput.size();

    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.length());
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * whether the file is a complete TsFile: only if the head magic and tail magic string exists.
   */
  public boolean isComplete() throws IOException {
    return tsFileInput.size() >= TSFileConfig.MAGIC_STRING.length() * 2 && readTailMagic()
        .equals(readHeadMagic());
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readHeadMagic() throws IOException {
    return readHeadMagic(false);
  }

  /**
   * @param movePosition whether move the position of the file reader after reading the magic header
   * to the end of the magic head string.
   */
  public String readHeadMagic(boolean movePosition) throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
    if (movePosition) {
      tsFileInput.position(0);
      tsFileInput.read(magicStringBytes);
    } else {
      tsFileInput.read(magicStringBytes, 0);
    }
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * this function reads version number and checks compatibility of TsFile.
   */
  public String readVersionNumber() throws IOException {
    ByteBuffer versionNumberBytes = ByteBuffer
        .allocate(TSFileConfig.VERSION_NUMBER.getBytes().length);
    tsFileInput.position(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(versionNumberBytes);
    versionNumberBytes.flip();
    return new String(versionNumberBytes.array());
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsFileMetadataV1 readFileMetadata() throws IOException {
    return TsFileMetadataV1.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsDeviceMetadataV1 readTsDeviceMetaData(TsDeviceMetadataIndexV1 index) throws IOException {
    return TsDeviceMetadataV1.deserializeFrom(readData(index.getOffset(), index.getLen()));
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupFooter readChunkGroupFooter() throws IOException {
    return ChunkGroupFooter.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br> This
   * method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader() throws IOException {
    return HeaderUtils.deserializeChunkHeaderV1(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return HeaderUtils.deserializePageHeaderV1(tsFileInput.wrapAsInputStream(), type);
  }

  public ByteBuffer readPage(PageHeader header, CompressionType type)
      throws IOException {
    ByteBuffer buffer = readData(-1, header.getCompressedSize());
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
    if (type == CompressionType.UNCOMPRESSED) {
      return buffer;
    }
    unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(),
        uncompressedBuffer.array(),
        0);
    return uncompressedBuffer;
  }

  public ByteBuffer readCompressedPage(PageHeader header) throws IOException {
    return readData(-1, header.getCompressedSize());
  }

  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void position(long offset) throws IOException {
    tsFileInput.position(offset);
  }

  /**
   * read one byte from the input. <br> this method is not thread safe
   */
  public byte readMarker() throws IOException {
    markerBuffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, markerBuffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    markerBuffer.flip();
    return markerBuffer.get();
  }

  public byte readMarker(long position) throws IOException {
    return readData(position, Byte.BYTES).get();
  }

  public void close() throws IOException {
    this.tsFileInput.close();
  }

  public String getFileName() {
    return this.file;
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position. <br> if position = -1, the tsFileInput's position will be changed to the current
   * position + real data size that been read. Other wise, the tsFileInput's position is not
   * changed.
   *
   * @param position the start position of data in the tsFileInput, or the current position if
   * position = -1
   * @param size the size of data that want to read
   * @return data that been read.
   */
  private ByteBuffer readData(long position, int size) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    if (position == -1) {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != size) {
        throw new IOException("reach the end of the data");
      }
    } else {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, size) != size) {
        throw new IOException("reach the end of the data");
      }
    }
    buffer.flip();
    return buffer;
  }

  /**
   * upgrade file and resource
   *
   * @throws IOException, WriteProcessException
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upgradeFile(List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {
    File oldTsFile = FSFactoryProducer.getFSFactory().getFile(this.file);

    // check if the old TsFile has correct header 
    if (!fileCheck(oldTsFile)) {
      return;
    }

    // ChunkGroupOffset -> version
    Map<Long, Long> oldVersionInfo = getVersionInfo();

    // start to scan chunks and chunkGroups
    long startOffsetOfChunkGroup = 0;
    boolean newChunkGroup = true;
    long versionOfChunkGroup = 0;
    int chunkGroupCount = 0;
    List<List<PageHeader>> pageHeadersInChunkGroup = new ArrayList<>();
    List<List<ByteBuffer>> pageDataInChunkGroup = new ArrayList<>();
    List<List<Boolean>> pagePartitionInfoInChunkGroup = new ArrayList<>();
    byte marker;
    List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
              startOffsetOfChunkGroup = this.position() - 1;
              versionOfChunkGroup = oldVersionInfo.get(startOffsetOfChunkGroup);
            }
            ChunkHeader header = this.readChunkHeader();
            MeasurementSchema measurementSchema = new MeasurementSchema(header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(),
                header.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            List<Boolean> pagePartitionInfo = new ArrayList<>();
            for (int j = 0; j < header.getNumOfPages(); j++) {
              PageHeader pageHeader = readPageHeader(header.getDataType());
              boolean pageInSamePartition = checkIfPageInSameTimePartition(pageHeader);
              pagePartitionInfo.add(pageInSamePartition);
              ByteBuffer pageData = pageInSamePartition ?
                  readCompressedPage(pageHeader)
                  : readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
            }
            pageHeadersInChunkGroup.add(pageHeadersInChunk);
            pageDataInChunkGroup.add(dataInChunk);
            pagePartitionInfoInChunkGroup.add(pagePartitionInfo);
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            // this is the footer of a ChunkGroup.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();
            rewrite(oldTsFile, deviceID, measurementSchemaList, pageHeadersInChunkGroup,
                pageDataInChunkGroup, versionOfChunkGroup, pagePartitionInfoInChunkGroup);

            pageHeadersInChunkGroup.clear();
            pageDataInChunkGroup.clear();
            measurementSchemaList.clear();
            pagePartitionInfoInChunkGroup.clear();
            newChunkGroup = true;
            chunkGroupCount++;
            break;

          default:
            // the disk file is corrupted, using this file may be dangerous
            logger.error("Unrecognized marker detected, this file may be corrupted");
            return;
        }
      }
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        upgradedResources.add(endFileAndGenerateResource(tsFileIOWriter));
      }
    } catch (IOException e2) {
      logger.info("TsFile upgrade process cannot proceed at position {} after {} chunk groups "
          + "recovered, because : {}", this.position(), chunkGroupCount, e2.getMessage());
    } finally {
      if (tsFileInput != null) {
        tsFileInput.close();
      }
    }
  }

  private boolean checkIfPageInSameTimePartition(PageHeader pageHeader) {
    return StorageEngine.getTimePartition(pageHeader.getStartTime())
        == StorageEngine.getTimePartition(pageHeader.getEndTime());
  }

  /**
   * This method is for rewriting the ChunkGroup which data is in the different time partitions. In
   * this case, we have to decode the data to points, and then rewrite the data points to different
   * chunkWriters, finally write chunks to their own upgraded TsFiles
   */
  private void rewrite(File oldTsFile, String deviceId, List<MeasurementSchema> schemas,
      List<List<PageHeader>> pageHeadersInChunkGroup, List<List<ByteBuffer>> dataInChunkGroup,
      long versionOfChunkGroup, List<List<Boolean>> pagePartitionInfoInChunkGroup)
      throws IOException, PageException {
    Map<Long, Map<MeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      MeasurementSchema schema = schemas.get(i);
      List<ByteBuffer> pageDataInChunk = dataInChunkGroup.get(i);
      List<PageHeader> pageHeadersInChunk = pageHeadersInChunkGroup.get(i);
      List<Boolean> pagePartitionInfo = pagePartitionInfoInChunkGroup.get(i);
      valueDecoder = Decoder
          .getDecoderByType(schema.getEncodingType(), schema.getType());
      for (int j = 0; j < pageDataInChunk.size(); j++) {
        if (Boolean.TRUE.equals(pagePartitionInfo.get(j))) {
          writePageInSamePartitionToFile(oldTsFile, schema, pageHeadersInChunk.get(j),
              pageDataInChunk.get(j), chunkWritersInChunkGroup);
        } else {
          writePageInDifferentPartitionsToFiles(oldTsFile, schema, pageDataInChunk.get(j),
              chunkWritersInChunkGroup);
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
      tsFileIOWriter.writeVersion(versionOfChunkGroup);
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
          File newFile = FSFactoryProducer.getFSFactory().getFile(oldTsFile.getParent()
              + File.separator + partition + File.separator + oldTsFile.getName());
          try {
            if (!newFile.createNewFile()) {
              logger.error("The TsFile {} has been created ", newFile);
              return null;
            }
            return new TsFileIOWriter(newFile);
          } catch (IOException e) {
            logger.error("Create new TsFile {} failed ", newFile);
            return null;
          }
        }
    );
  }

  private void writePageInSamePartitionToFile(File oldTsFile, MeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Map<Long, Map<MeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup)
      throws PageException {
    long partitionId = StorageEngine.getTimePartition(pageHeader.getStartTime());
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
    Map<MeasurementSchema, ChunkWriterImpl> chunkWriters = chunkWritersInChunkGroup
        .getOrDefault(partitionId, new HashMap<>());
    ChunkWriterImpl chunkWriter = chunkWriters
        .getOrDefault(schema, new ChunkWriterImpl(schema));
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader);
    chunkWriters.put(schema, chunkWriter);
    chunkWritersInChunkGroup.put(partitionId, chunkWriters);
  }

  private void writePageInDifferentPartitionsToFiles(File oldTsFile, MeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, Map<MeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup)
      throws IOException {
    valueDecoder.reset();
    PageReader pageReader = new PageReader(pageData, schema.getType(), valueDecoder,
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
  private boolean fileCheck(File oldTsFile) throws IOException {
    long fileSize;
    if (!oldTsFile.exists()) {
      logger.error("the file to be updated does not exist, file path: {}", oldTsFile.getPath());
      return false;
    } else {
      fileSize = oldTsFile.length();
    }

    String magic = readHeadMagic(true);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", oldTsFile.getPath());
      return false;
    }

    String versionNumber = readVersionNumber();
    if (!versionNumber.equals(TSFileConfig.VERSION_NUMBER_V1)) {
      logger.error("the file's Version Number is incorrect, file path: {}", oldTsFile.getPath());
      return false;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.length()) {
      logger.error("the file only contains magic string, file path: {}", oldTsFile.getPath());
      return false;
    } else if (!readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file cannot upgrade, file path: {}", oldTsFile.getPath());
      return false;
    }
    return true;
  }

  private Map<Long, Long> getVersionInfo() throws IOException {
    Map<Long, Long> versionInfo = new HashMap<>();
    TsFileMetadataV1 fileMetadata = readFileMetadata();
    List<TsDeviceMetadataV1> oldDeviceMetadataList = new ArrayList<>();
    for (TsDeviceMetadataIndexV1 index : fileMetadata.getDeviceMap().values()) {
      TsDeviceMetadataV1 oldDeviceMetadata = readTsDeviceMetaData(index);
      oldDeviceMetadataList.add(oldDeviceMetadata);
    }

    for (TsDeviceMetadataV1 oldTsDeviceMetadata : oldDeviceMetadataList) {
      for (ChunkGroupMetaDataV1 oldChunkGroupMetadata : oldTsDeviceMetadata
          .getChunkGroupMetaDataList()) {
        long version = oldChunkGroupMetadata.getVersion();
        long offsetOfChunkGroup = oldChunkGroupMetadata.getStartOffsetOfChunkGroup();
        // get version informations
        versionInfo.put(offsetOfChunkGroup, version);
      }
    }
    return versionInfo;
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
    return tsFileResource;
  }

}