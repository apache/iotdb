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
package org.apache.iotdb.tsfile.tool.upgrade;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.compress.ICompressor.SnappyCompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor.SnappyUnCompressor;
import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.OldTsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.OldTsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.OldTsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
   * construct function for TsfileUpgradeToolV0_8_0.
   *
   * @param file -given file name
   * @param loadMetadataSize -load meta data size
   */
  public TsfileUpgradeToolV0_9_0(String file, boolean loadMetadataSize) throws IOException {
    this.file = file;
    final java.nio.file.Path path = Paths.get(file);
    tsFileInput = new LocalTsFileInput(path);
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
   */
  public boolean upgradeFile(String updateFileName) throws IOException {
    File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
    long fileSize;
    if (!checkFile.exists()) {
      logger.error("the file to be updated does not exist, file path: {}", checkFile.getPath());
      return false;
    } else {
      fileSize = checkFile.length();
    }
    File upgradeFile = FSFactoryProducer.getFSFactory().getFile(updateFileName);
    if (!upgradeFile.getParentFile().exists()) {
      upgradeFile.getParentFile().mkdirs();
    }
    upgradeFile.createNewFile();
    TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(upgradeFile);

    List<ChunkHeader> chunkHeaders = new ArrayList<>();
    List<List<PageHeader>> pageHeadersList = new ArrayList<>();
    List<List<ByteBuffer>> pagesList = new ArrayList<>();
    Map<Path, MeasurementSchema> newSchema = new HashMap<>();

    String magic = readHeadMagic(true);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", checkFile.getPath());
      return false;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.length()) {
      logger.error("the file only contains magic string, file path: {}", checkFile.getPath());
      return false;
    } else if (readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      loadMetadataSize();
      OldTsFileMetadata oldTsFileMetaData = readFileMetadata();
    } else {
      logger.error("the file cannot upgrade, file path: {}", checkFile.getPath());
    }

    ChunkMetadata currentChunkMetaData;
    List<ChunkMetadata> chunkMetaDataList = null;
    long startOffsetOfChunkGroup = 0;
    boolean newChunkGroup = true;
    long versionOfChunkGroup = 0;
    List<ChunkGroupMetadata> newMetaData = new ArrayList<>();
    List<Statistics<?>> chunkStatisticsList = new ArrayList<>();

    boolean goon = true;
    byte marker;
    List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while (goon && (marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
              chunkMetaDataList = new ArrayList<>();
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
            List<PageHeader> pageHeaders = new ArrayList<>();
            List<ByteBuffer> pages = new ArrayList<>();
            TSDataType dataType = header.getDataType();
            Statistics<?> chunkStatistics = Statistics.getStatsByType(dataType);
            chunkStatisticsList.add(chunkStatistics);
            for (int j = 0; j < header.getNumOfPages(); j++) {
              // a new Page
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              pageHeaders.add(pageHeader);
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              pages.add(readData(-1, pageHeader.getCompressedSize()));
            }
            currentChunkMetaData = new ChunkMetadata(header.getMeasurementID(), dataType,
                fileOffsetOfChunk, chunkStatistics);
            chunkMetaDataList.add(currentChunkMetaData);
            pageHeadersList.add(pageHeaders);
            pagesList.add(pages);
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();
            if (newSchema != null) {
              for (MeasurementSchema tsSchema : measurementSchemaList) {
                newSchema.putIfAbsent(new Path(deviceID, tsSchema.getMeasurementId()), tsSchema);
              }
            }
            newChunkGroup = true;
            newMetaData.add(new ChunkGroupMetadata(deviceID, chunkMetaDataList));
            
            
            
            
            // TODO: writer
            
            
            
            tsFileIOWriter.startChunkGroup(deviceID);
            for (int i = 0; i < chunkHeaders.size(); i++) {
              TSDataType tsDataType = chunkHeaders.get(i).getDataType();
              TSEncoding encodingType = chunkHeaders.get(i).getEncodingType();
              CompressionType compressionType = chunkHeaders.get(i).getCompressionType();
              ChunkHeader chunkHeader = chunkHeaders.get(i);
              List<PageHeader> pageHeaderList = pageHeadersList.get(i);
              List<ByteBuffer> pageList = pagesList.get(i);
              Path path = new Path(deviceID, chunkHeader.getMeasurementID());

              if (newSchema.get(path) != null) {
                ChunkWriterImpl chunkWriter = new ChunkWriterImpl(newSchema.get(path));
                for (int j = 0; j < pageHeaderList.size(); j++) {
                  if (encodingType.equals(TSEncoding.PLAIN)) {
                    pageList.set(j, rewrite(pageList.get(j), tsDataType, compressionType,
                        pageHeaderList.get(j)));
                  }
                  chunkWriter
                      .writePageHeaderAndDataIntoBuff(pageList.get(j), pageHeaderList.get(j));
                }
                chunkWriter
                    .writeAllPagesOfChunkToTsFile(tsFileIOWriter, chunkStatisticsList.get(i));
              }
            }
            tsFileIOWriter.endChunkGroup();
            chunkStatisticsList.clear();
            chunkHeaders.clear();
            pageHeadersList.clear();
            pagesList.clear();
            newChunkGroup = true;
            break;

          default:
            // the disk file is corrupted, using this file may be dangerous
            logger.error("Unrecognized marker detected, this file may be corrupted");
            return false;
        }
      }
      tsFileIOWriter.endFile();
      return true;
    } catch (IOException | PageException e2) {
      logger.info("TsFile upgrade process cannot proceed at position {} after {} chunk groups "
          + "recovered, because : {}", this.position(), newMetaData.size(), e2.getMessage());
      return false;
    } finally {
      if (tsFileInput != null) {
        tsFileInput.close();
      }
      if (tsFileIOWriter != null) {
        tsFileIOWriter.close();
      }
    }
  }

  static ByteBuffer rewrite(ByteBuffer page, TSDataType tsDataType,
      CompressionType compressionType, PageHeader pageHeader) {
    switch (compressionType) {
      case UNCOMPRESSED:
        break;
      case SNAPPY:
        SnappyUnCompressor snappyUnCompressor = new SnappyUnCompressor();
        page = ByteBuffer.wrap(snappyUnCompressor.uncompress(page.array()));
        break;
      default:
        throw new CompressionTypeNotSupportedException(compressionType.toString());
    }
    ByteBuffer modifiedPage = ByteBuffer.allocate(page.capacity());

    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(page);
    ByteBuffer timeBuffer = page.slice();
    ByteBuffer valueBuffer = page.slice();

    timeBuffer.limit(timeBufferLength);
    valueBuffer.position(timeBufferLength);
    valueBuffer.order(ByteOrder.LITTLE_ENDIAN);

    ReadWriteForEncodingUtils.writeUnsignedVarInt(timeBufferLength, modifiedPage);
    modifiedPage.put(timeBuffer);
    modifiedPage.order(ByteOrder.BIG_ENDIAN);
    switch (tsDataType) {
      case BOOLEAN:
        modifiedPage.put(valueBuffer);
        break;
      case INT32:
        while (valueBuffer.remaining() > 0) {
          modifiedPage.putInt(valueBuffer.getInt());
        }
        break;
      case INT64:
        while (valueBuffer.remaining() > 0) {
          modifiedPage.putLong(valueBuffer.getLong());
        }
        break;
      case FLOAT:
        while (valueBuffer.remaining() > 0) {
          modifiedPage.putFloat(valueBuffer.getFloat());
        }
        break;
      case DOUBLE:
        while (valueBuffer.remaining() > 0) {
          modifiedPage.putDouble(valueBuffer.getDouble());
        }
        break;
      case TEXT:
        while (valueBuffer.remaining() > 0) {
          int length = valueBuffer.getInt();
          byte[] buf = new byte[length];
          valueBuffer.get(buf, 0, buf.length);
          modifiedPage.putInt(length);
          modifiedPage.put(buf);
        }
        break;
    }
    switch (compressionType) {
      case UNCOMPRESSED:
        modifiedPage.flip();
        break;
      case SNAPPY:
        pageHeader.setUncompressedSize(modifiedPage.array().length);
        SnappyCompressor snappyCompressor = new SnappyCompressor();
        try {
          modifiedPage = ByteBuffer.wrap(snappyCompressor.compress(modifiedPage.array()));
          pageHeader.setCompressedSize(modifiedPage.array().length);
        } catch (IOException e) {
          logger.error("failed to compress page as snappy", e);
        }
        break;
      default:
        throw new CompressionTypeNotSupportedException(compressionType.toString());
    }
    return modifiedPage;
  }
}