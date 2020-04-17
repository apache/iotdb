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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.OldChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.OldChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.OldTsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.OldTsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.OldTsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsfileUpgradeToolV0_9_0 implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsfileUpgradeToolV0_9_0.class);

  private TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
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
  public TsfileUpgradeToolV0_9_0(String file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsfileUpgradeToolV0_9_0.
   *
   * @param file -given file name
   * @param loadMetadataSize -load meta data size
   */
  public TsfileUpgradeToolV0_9_0(String file, boolean loadMetadataSize) throws IOException {
    this.file = file;
    final java.nio.file.Path path = Paths.get(file);
    tsFileInput = new LocalTsFileInput(path);
    partitionWriterMap = new HashMap<>();
    try {
      if (loadMetadataSize) {
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
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
  public OldTsFileMetadata readFileMetadata() throws IOException {
    return OldTsFileMetadata.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public OldTsDeviceMetadata readTsDeviceMetaData(OldTsDeviceMetadataIndex index) throws IOException {
    return OldTsDeviceMetadata.deserializeFrom(readData(index.getOffset(), index.getLen()));
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
   * read chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  public Chunk readChunk(OldChunkMetadata metaData) throws IOException {
    int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());
    chunkHeadSize += Long.BYTES; // maxTombstoneTime
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize, false);
    ByteBuffer buffer = readData(metaData.getOffsetOfChunkHeader() + header.getSerializedSize(),
        header.getDataSize());
    return new Chunk(header, buffer, metaData.getDeletedAt(), EndianType.BIG_ENDIAN);
  }
  
  /**
   * read the chunk's header.
   *
   * @param position        the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead      true if the offset does not contains the marker , otherwise false
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize, boolean markerRead)
      throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize, markerRead, 
        true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br> This
   * method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader() throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true, true);
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type, true);
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
   * upgrade file and return the boolean value whether upgrade task completes
   * @throws IOException, WriteProcessException 
   */
  public boolean upgradeFile(List<String> upgradedFileList) throws IOException, WriteProcessException {
    File oldTsFile = FSFactoryProducer.getFSFactory().getFile(this.file);

    // check if the old TsFile has correct header 
    if (!fileCheck(oldTsFile)) {
      return false;
    }

    // ChunkGroupOffset -> version
    Map<Long, Long> oldVersionInfo = new HashMap<>();

    // ChunkGroupOffset -> time partition, chunk group those offsets in this map are in same partition
    Map<Long, Long> chunkGroupTimePartitionInfo = new HashMap<>();

    // ChunkGroupOffset -> ChunkGroupMetaData, chunk group those offsets in this map are in same partition
    Map<Long, OldChunkGroupMetaData> groupMetadatas = new HashMap<>();

    // scan metadata to get version Info and chunkGroupTimePartitionInfo
    scanMetadata(oldVersionInfo, chunkGroupTimePartitionInfo, groupMetadatas);
    
    // start to scan chunks and chunkGroups
    long startOffsetOfChunkGroup = 0;
    boolean newChunkGroup = true;
    long versionOfChunkGroup = 0;
    boolean chunkGroupInSamePartition = false;
    List<ChunkGroupMetadata> newMetaData = new ArrayList<>();
    List<List<ByteBuffer>> dataInChunkGroup = new ArrayList<>();
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
              chunkGroupInSamePartition = chunkGroupTimePartitionInfo
                  .containsKey(startOffsetOfChunkGroup);
            }
            // if data of this chunk group are in same time partition,
            // do a quick rewrite without splitting data to data points,
            // and then jump to the next chunk group. 
            if (chunkGroupInSamePartition) {
              OldChunkGroupMetaData oldChunkGroupMetadata = groupMetadatas.get(startOffsetOfChunkGroup);
              quickRewrite(oldTsFile, oldChunkGroupMetadata, 
                  versionOfChunkGroup, chunkGroupTimePartitionInfo.get(startOffsetOfChunkGroup));
              tsFileInput.position(oldChunkGroupMetadata.getEndOffsetOfChunkGroup());
              newChunkGroup = true;
              break;
            }
            ChunkHeader header = this.readChunkHeader();
            MeasurementSchema measurementSchema = new MeasurementSchema(header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(), 
                header.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            for (int j = 0; j < header.getNumOfPages(); j++) {
              PageHeader pageHeader = readPageHeader(header.getDataType());
              ByteBuffer pageData = readPage(pageHeader, header.getCompressionType());
              dataInChunk.add(pageData);
            }
            dataInChunkGroup.add(dataInChunk);
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            // this is the footer of a ChunkGroup.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();
            rewrite(oldTsFile, deviceID, measurementSchemaList, 
              dataInChunkGroup, versionOfChunkGroup);

            dataInChunkGroup.clear();
            measurementSchemaList.clear();
            newChunkGroup = true;
            break;

          default:
            // the disk file is corrupted, using this file may be dangerous
            logger.error("Unrecognized marker detected, this file may be corrupted");
            return false;
        }
      }
      
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        upgradedFileList.add(tsFileIOWriter.getFile().getAbsolutePath());
        tsFileIOWriter.endFile();
      }
      return true;
    } catch (IOException e2) {
      logger.info("TsFile upgrade process cannot proceed at position {} after {} chunk groups "
          + "recovered, because : {}", this.position(), newMetaData.size(), e2.getMessage());
      return false;
    } finally {
      if (tsFileInput != null) {
        tsFileInput.close();
      }
    }
  }

  /**
   *  Rewrite the chunk group to new TsFile.
   *  If data of this chunk group are in different time partitions,
   *  create multiple new TsFiles and rewrite data in each partition.
   */
  private void rewrite(File oldTsFile, String deviceId, List<MeasurementSchema> schemas, 
      List<List<ByteBuffer>> dataInChunkGroup, long versionOfChunkGroup) 
          throws IOException, WriteProcessException {

    Map<Long, Map<MeasurementSchema, IChunkWriter>> chunkWritersInChunkGroup = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      MeasurementSchema schema = schemas.get(i);
      Decoder defaultTimeDecoder = Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
      Decoder valueDecoder = Decoder
          .getDecoderByType(schema.getEncodingType(), schema.getType());
      List<ByteBuffer> dataInChunk = dataInChunkGroup.get(i);
      for (ByteBuffer pageData : dataInChunk) {
        valueDecoder.reset();
        PageReader pageReader = new PageReader(pageData, schema.getType(), valueDecoder,
            defaultTimeDecoder, null);
        BatchData batchData = pageReader.getAllSatisfiedPageData();
        while (batchData.hasCurrent()) {
          long time = batchData.currentTime();
          Object value = batchData.currentValue();
          long partition = StorageEngine.getTimePartition(time);
          
          Map<MeasurementSchema, IChunkWriter> chunkWriters = chunkWritersInChunkGroup.getOrDefault(partition, new HashMap<>());
          IChunkWriter chunkWriter = chunkWriters.getOrDefault(schema, new ChunkWriterImpl(schema));
          TsFileIOWriter tsFileIOWriter = partitionWriterMap
              .computeIfAbsent(partition, k -> 
                {
                  File partitionDir = FSFactoryProducer.getFSFactory().getFile(oldTsFile.getParent()
                      + File.separator + partition);
                  partitionDir.mkdirs();
                  File newFile = FSFactoryProducer.getFSFactory().getFile(oldTsFile.getParent()
                      + File.separator + partition + File.separator+ oldTsFile.getName());
                  try {
                    newFile.createNewFile();
                    TsFileIOWriter writer = new TsFileIOWriter(newFile);
                    return writer;
                  } catch (IOException e) {
                  }
                  return null;
                }
              );
          partitionWriterMap.put(partition, tsFileIOWriter);
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
          chunkWriters.put(schema, chunkWriter);
          chunkWritersInChunkGroup.put(partition, chunkWriters);
          batchData.next();
        }
      }
    }
    // set version info to each upgraded tsFile 
    for (long partition : chunkWritersInChunkGroup.keySet()) {
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partition);
      tsFileIOWriter.startChunkGroup(deviceId);
      for (IChunkWriter chunkWriter : chunkWritersInChunkGroup.get(partition).values()) {
        chunkWriter.writeToFileWriter(tsFileIOWriter);
      }
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.writeVersion(versionOfChunkGroup);
    }
  }

  private void quickRewrite(File oldTsFile, OldChunkGroupMetaData oldChunkGroupMetadata, 
      long versionOfChunkGroup, long partition) throws IOException {
    TsFileIOWriter tsFileIOWriter = partitionWriterMap
        .computeIfAbsent(partition, k -> 
          {
            File partitionDir = FSFactoryProducer.getFSFactory().getFile(oldTsFile.getParent()
                + File.separator + partition);
            partitionDir.mkdirs();
            File newFile = FSFactoryProducer.getFSFactory().getFile(oldTsFile.getParent()
                + File.separator + partition + File.separator+ oldTsFile.getName());
            try {
              newFile.createNewFile();
              TsFileIOWriter writer = new TsFileIOWriter(newFile);
              return writer;
            } catch (IOException e) {
            }
            return null;
          }
        );
    tsFileIOWriter.startChunkGroup(oldChunkGroupMetadata.getDeviceID());
    for (OldChunkMetadata oldChunkMetadata : oldChunkGroupMetadata.getChunkMetaDataList()) {
      Chunk chunk = readChunk(oldChunkMetadata);
      tsFileIOWriter.writeChunk(chunk, new ChunkMetadata(oldChunkMetadata));
    }
    tsFileIOWriter.endChunkGroup();
    tsFileIOWriter.writeVersion(versionOfChunkGroup);
  }

  /**
   *  check if the file to be upgraded has correct magic strings and version number
   *  @param oldTsFile
   *  @throws IOException 
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
    if (!versionNumber.equals(TSFileConfig.OLD_VERSION)) {
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
  
  private void scanMetadata(Map<Long, Long> oldVersionInfo, Map<Long, Long> chunkGroupTimePartitionInfo, 
      Map<Long, OldChunkGroupMetaData> groupMetadatas) throws IOException {
    OldTsFileMetadata fileMetadata = readFileMetadata();
    List<OldTsDeviceMetadata> oldDeviceMetadataList = new ArrayList<>();
    for (OldTsDeviceMetadataIndex index : fileMetadata.getDeviceMap().values()) {
      OldTsDeviceMetadata oldDeviceMetadata = readTsDeviceMetaData(index);
      oldDeviceMetadataList.add(oldDeviceMetadata);
    }

    for (OldTsDeviceMetadata oldTsDeviceMetadata : oldDeviceMetadataList) {
      for (OldChunkGroupMetaData oldChunkGroupMetadata : oldTsDeviceMetadata
          .getChunkGroupMetaDataList()) {
        long version = oldChunkGroupMetadata.getVersion();
        long offsetOfChunkGroupMetaData = oldChunkGroupMetadata.getStartOffsetOfChunkGroup();
        oldVersionInfo.put(offsetOfChunkGroupMetaData, version);

        // check if data of a chunk group is in a same time partition 
        long chunkGroupPartition = -1;
        boolean chunkGroupInSamePartition = true;
        for (OldChunkMetadata oldChunkMetadata : oldChunkGroupMetadata.getChunkMetaDataList()) {
          if (chunkGroupPartition == -1) {
            chunkGroupPartition = StorageEngine.getTimePartition(oldChunkMetadata.getStartTime());
          }
          long startPartition = StorageEngine.getTimePartition(oldChunkMetadata.getStartTime());
          long endPartition = StorageEngine.getTimePartition(oldChunkMetadata.getEndTime());
          if (chunkGroupPartition != startPartition || chunkGroupPartition != endPartition) {
            chunkGroupInSamePartition = false;
            break;
          }
        }
        if (chunkGroupInSamePartition) {
          chunkGroupTimePartitionInfo.put(offsetOfChunkGroupMetaData, chunkGroupPartition);
          groupMetadatas.put(offsetOfChunkGroupMetaData, oldChunkGroupMetadata);
        }
      }
    }
  }

}