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
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
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
  private Map<Long, TsFileWriter> partitionWriterMap;

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
   * @throws WriteProcessException 
   */
  public boolean upgradeFile(String updateDirectoryName) throws IOException, WriteProcessException {
    File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
    long fileSize;
    if (!checkFile.exists()) {
      logger.error("the file to be updated does not exist, file path: {}", checkFile.getPath());
      return false;
    } else {
      fileSize = checkFile.length();
    }
    // TODO file parentFile
    File upgradeFile = FSFactoryProducer.getFSFactory().getFile(updateDirectoryName);
    if (!upgradeFile.getParentFile().exists()) {
      upgradeFile.getParentFile().mkdirs();
    }
    //upgradeFile.createNewFile();

    List<ChunkHeader> chunkHeaders = new ArrayList<>();

    String magic = readHeadMagic(true);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", checkFile.getPath());
      return false;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.length()) {
      logger.error("the file only contains magic string, file path: {}", checkFile.getPath());
      return false;
    } else if (!readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file cannot upgrade, file path: {}", checkFile.getPath());
      return false;
    }
    OldTsFileMetadata fileMetadata = readFileMetadata();
    long partition = -1;

    boolean allDataInSamePartition = true;
    List<OldTsDeviceMetadata> oldDeviceMetadataList = new ArrayList<>();
    for (OldTsDeviceMetadataIndex index : fileMetadata.getDeviceMap().values()) {

      // check if all data in this TsFile are in same time partition
      long startPartition = StorageEngine.getTimePartition(index.getStartTime());
      long endPartition = StorageEngine.getTimePartition(index.getEndTime());
      if (partition == -1) {
        partition = startPartition;
      }
      if (partition != startPartition || partition != endPartition) {
        allDataInSamePartition = false;
      }
      OldTsDeviceMetadata oldDeviceMetadata = readTsDeviceMetaData(index);
      oldDeviceMetadataList.add(oldDeviceMetadata);
    }
    
    partitionWriterMap.put(partition, new TsFileWriter(new File(updateDirectoryName 
        + File.separator + partition + File.separator+ file)));

    for (OldTsDeviceMetadata oldTsDeviceMetadata : oldDeviceMetadataList) {
      for (OldChunkGroupMetaData oldChunkGroupMetadata : oldTsDeviceMetadata
          .getChunkGroupMetaDataList()) {
        long version = oldChunkGroupMetadata.getVersion();
      }
    }
    
    
    

    long startOffsetOfChunkGroup = 0;
    boolean newChunkGroup = true;
    long versionOfChunkGroup = 0;
    List<ChunkGroupMetadata> newMetaData = new ArrayList<>();
    List<List<ByteBuffer>> datasInOneChunkGroup = new ArrayList<>();
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
            }

            long fileOffsetOfChunk = this.position() - 1;
            ChunkHeader header = this.readChunkHeader();
            chunkHeaders.add(header);
            MeasurementSchema measurementSchema = new MeasurementSchema(header.getMeasurementID(),
                header.getDataType(),
                header.getEncodingType(), 
                header.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            List<ByteBuffer> datasInOneChunk = new ArrayList<>();
            for (int j = 0; j < header.getNumOfPages(); j++) {
              PageHeader pageHeader = readPageHeader(header.getDataType());
              ByteBuffer pageData = readPage(pageHeader, header.getCompressionType());
              datasInOneChunk.add(pageData);
            }
            datasInOneChunkGroup.add(datasInOneChunk);
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();

            versionOfChunkGroup++;
            rewrite(updateDirectoryName, deviceID, measurementSchemaList, 
                datasInOneChunkGroup, allDataInSamePartition);

            datasInOneChunkGroup.clear();
            chunkHeaders.clear();
            measurementSchemaList.clear();
            newChunkGroup = true;
            break;

          default:
            // the disk file is corrupted, using this file may be dangerous
            logger.error("Unrecognized marker detected, this file may be corrupted");
            return false;
        }
      }
      for (TsFileWriter tsFileWriter : partitionWriterMap.values()) {
        tsFileWriter.close();
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

  void rewrite(String upgradeDir, String deviceId, List<MeasurementSchema> schemas, 
      List<List<ByteBuffer>> datasInOneChunkGroup, boolean allDataInSamePartition) 
          throws IOException, WriteProcessException {

    // if all data is in same time partition, 
    // do a quick rewrite without inserting points one by one
    if (allDataInSamePartition) {
      quickRewrite(upgradeDir, deviceId, schemas, datasInOneChunkGroup);
    }
    else {
      for (int i = 0; i < schemas.size(); i++) {
        MeasurementSchema schema = schemas.get(i);
        Decoder defaultTimeDecoder = Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
        Decoder valueDecoder = Decoder
            .getDecoderByType(schema.getEncodingType(), schema.getType());
        List<ByteBuffer> datasInOneChunk = datasInOneChunkGroup.get(i);
        for (ByteBuffer pageData : datasInOneChunk) {
          valueDecoder.reset();
          PageReader pageReader = new PageReader(pageData, schema.getType(), valueDecoder,
              defaultTimeDecoder, null);
          BatchData batchData = pageReader.getAllSatisfiedPageData();
          while (batchData.hasCurrent()) {
            long time = batchData.currentTime();
            Object value = batchData.currentValue();
            long partition = StorageEngine.getTimePartition(time);
            TSRecord tsRecord = new TSRecord(time, deviceId);
            DataPoint dataPoint;
            switch (schema.getType()) {
              case INT32:
                dataPoint = new IntDataPoint(schema.getMeasurementId(), (int) value);
                break;
              case INT64:
                dataPoint = new LongDataPoint(schema.getMeasurementId(), (long) value);
                break;
              case FLOAT:
                dataPoint = new FloatDataPoint(schema.getMeasurementId(), (float) value);
                break;
              case DOUBLE:
                dataPoint = new DoubleDataPoint(schema.getMeasurementId(), (double) value);
                break;
              case BOOLEAN:
                dataPoint = new BooleanDataPoint(schema.getMeasurementId(), (boolean) value);
                break;
              case TEXT:
                dataPoint = new StringDataPoint(schema.getMeasurementId(), (Binary) value);
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", schema.getType()));
              }
            tsRecord.addTuple(dataPoint);
            TsFileWriter tsFileWriter = partitionWriterMap
                .getOrDefault(partition, new TsFileWriter(new File(upgradeDir 
                    + File.separator + partition + File.separator+ file)));
            try {
              tsFileWriter.registerTimeseries(new Path(deviceId, schema.getMeasurementId()), schema);
            }
            catch (WriteProcessException e) {
              // do nothing
            }
            tsFileWriter.write(tsRecord);
            batchData.next();
          }
        }
      }
    }
  }
  
  void quickRewrite(String upgradeDir, String deviceId, List<MeasurementSchema> schemas, 
      List<List<ByteBuffer>> datasInOneChunkGroup) throws IOException {
    if (partitionWriterMap.size() != 1) {
      throw new IOException("This TsFile cannot do a quick rewrite.");
    }
    for (TsFileWriter tsFileWriter : partitionWriterMap.values()) {
      TsFileIOWriter tsFileIOWriter = tsFileWriter.getIOWriter();
    }
  }
}