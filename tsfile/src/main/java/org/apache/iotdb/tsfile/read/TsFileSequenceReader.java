/**
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
package org.apache.iotdb.tsfile.read;

import static org.apache.iotdb.tsfile.write.writer.TsFileIOWriter.magicStringBytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.DefaultTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileSequenceReader implements AutoCloseable{

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSequenceReader.class);

  private TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  protected String file;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.length() bytes
   * of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileSequenceReader(String file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
    this.file = file;
    final Path path = Paths.get(file);
    tsFileInput = new DefaultTsFileInput(path);
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
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.length() bytes
   * of the file for preparing reading real data.
   *
   * @param input given input
   */
  public TsFileSequenceReader(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReader(TsFileInput input, boolean loadMetadataSize)
      throws IOException {
    this.tsFileInput = input;
    try {
      if (loadMetadataSize) { // NOTE no autoRepair here
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input the input of a tsfile. The current position should be a markder and then a chunk
   * Header, rather than the magic number
   * @param fileMetadataPos the position of the file metadata in the TsFileInput from the beginning
   * of the input to the current position
   * @param fileMetadataSize the byte size of the file metadata in the input
   */
  public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    this.tsFileInput = input;
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  protected void loadMetadataSize() throws IOException {
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
    tsFileInput.read(metadataSize,
        tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES);
    metadataSize.flip();
    // read file metadata size and position
    fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
    fileMetadataPos =
        tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES - fileMetadataSize;
    // skip the magic header
    tsFileInput.position(TSFileConfig.MAGIC_STRING.length());
  }

  public long getFileMetadataPos() {
    return fileMetadataPos;
  }

  public int getFileMetadataSize() {
    return fileMetadataSize;
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readTailMagic() throws IOException {
    long totalSize = tsFileInput.size();

    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.length());
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * whether the file is a complete TsFile: only if the head magic and tail magic string exists.
   * @return
   * @throws IOException
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
   * this function does not modify the position of the file reader.
   *
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
  public TsFileMetaData readFileMetadata() throws IOException {
    return TsFileMetaData.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
  }

  /**
   * @return get the position after the last chunk group in the file
   */
  public long getPositionOfFirstDeviceMetaIndex() throws IOException {
    TsFileMetaData metaData = readFileMetadata();
    Optional<Long> data = metaData.getDeviceMap().values().stream().map(TsDeviceMetadataIndex::getOffset)
        .min(Comparator.comparing(Long::valueOf));
    if(data.isPresent()) {
      return data.get();
    } else {
      //no real data
      return TSFileConfig.MAGIC_STRING.length();
    }
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
    return TsDeviceMetadata.deserializeFrom(readData(index.getOffset(), index.getLen()));
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
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER.
   *
   * @param position the offset of the chunk group footer in the file
   * @param markerRead true if the offset does not contains the marker , otherwise false
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupFooter readChunkGroupFooter(long position, boolean markerRead)
      throws IOException {
    return ChunkGroupFooter.deserializeFrom(tsFileInput, position, markerRead);
  }

  /**
   * After reading the footer of a ChunkGroup, call this method to set the file pointer to the start
   * of the data of this ChunkGroup if you want to read its data next. <br> This method is not
   * threadsafe.
   *
   * @param footer the chunkGroupFooter which you want to read data
   */
  public void setPositionToAChunkGroup(ChunkGroupFooter footer) throws IOException {
    tsFileInput
        .position(tsFileInput.position() - footer.getDataSize() - footer.getSerializedSize());
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br> This
   * method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader() throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param markerRead true if the offset does not contains the marker , otherwise false
   */
  private ChunkHeader readChunkHeader(long position, boolean markerRead) throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position, markerRead);
  }

  /**
   * notice, the position of the channel MUST be at the end of this header. <br> This method is not
   * threadsafe.
   *
   * @return the pages of this chunk
   */
  public ByteBuffer readChunk(ChunkHeader header) throws IOException {
    return readData(-1, header.getDataSize());
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  public ByteBuffer readChunk(ChunkHeader header, long position) throws IOException {
    return readData(position, header.getDataSize());
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  private ByteBuffer readChunk(long position, int dataSize) throws IOException {
    return readData(position, dataSize);
  }

  /**
   * read memory chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  public Chunk readMemChunk(ChunkMetaData metaData) throws IOException {
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), false);
    ByteBuffer buffer = readChunk(metaData.getOffsetOfChunkHeader() + header.getSerializedSize(),
        header.getDataSize());
    return new Chunk(header, buffer);
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
  }

  /**
   * read the page's header.
   *
   * @param dataType given tsfile data type
   * @param position the file offset of this chunk's header
   * @param markerRead true if the offset does not contains the marker , otherwise false
   */
  private PageHeader readPageHeader(TSDataType dataType, long position, boolean markerRead) throws IOException {
    return PageHeader.deserializeFrom(dataType, tsFileInput, position, markerRead);
  }

  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void skipPageData(PageHeader header) throws IOException {
    tsFileInput.position(tsFileInput.position() + header.getCompressedSize());
  }

  /**
   *
   * @param header
   * @param position
   * @return
   * @throws IOException
   */
  public long skipPageData(PageHeader header, long position) throws IOException {
    return position + header.getCompressedSize();
  }


  public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
    return readPage(header, type, -1);
  }

  private ByteBuffer readPage(PageHeader header, CompressionType type, long position)
      throws IOException {
    ByteBuffer buffer = readData(position, header.getCompressedSize());
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
    switch (type) {
      case UNCOMPRESSED:
        return buffer;
      default:
        // FIXME if the buffer is not array-implemented.
        unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(),
            uncompressedBuffer.array(),
            0);
        return uncompressedBuffer;
    }
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

  public long fileSize() throws IOException {
    return tsFileInput.size();
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
   * notice, the target bytebuffer are not flipped.
   */
  public int readRaw(long position, int length, ByteBuffer target) throws IOException {
    return ReadWriteIOUtils
        .readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   *
   * @param newSchema @OUT.  the measurement schema in the file will be added into
   * this parameter. (can be null)
   * @param newMetaData @OUT can not be null, the chunk group metadta in the file will be added into
   * this parameter.
   * @param fastFinish if true and the file is complete, then newSchema and newMetaData parameter
   * will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   * be truncated.
   */
  public long selfCheck(Map<String, MeasurementSchema> newSchema,
      List<ChunkGroupMetaData> newMetaData, boolean fastFinish) throws IOException {
    File checkFile = new File(this.file);
    long fileSize;
    if (!checkFile.exists()) {
      return TsFileCheckStatus.FILE_NOT_FOUND;
    } else {
      fileSize = checkFile.length();
    }
    ChunkMetaData currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;
    long startTimeOfChunk = 0;
    long endTimeOfChunk = 0;
    long numOfPoints = 0;

    ChunkGroupMetaData currentChunkGroup;
    List<ChunkMetaData> chunks = null;
    String deviceID;
    long startOffsetOfChunkGroup = 0;
    long endOffsetOfChunkGroup;
    long versionOfChunkGroup = 0;
    boolean haveReadAnUnverifiedGroupFooter = false;
    boolean newGroup = true;

    if (fileSize < TSFileConfig.MAGIC_STRING.length()) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    String magic = readHeadMagic(true);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.length()) {
      return TsFileCheckStatus.ONLY_MAGIC_HEAD;
    } else if (readTailMagic().equals(magic)) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }

    // not a complete file, we will recover it...
    long truncatedPosition = magicStringBytes.length;
    boolean goon = true;
    byte marker;
    try {
      while (goon && (marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            //this is a chunk.
            if (haveReadAnUnverifiedGroupFooter) {
              //now we are sure that the last ChunkGroupFooter is complete.
              haveReadAnUnverifiedGroupFooter = false;
              truncatedPosition = this.position() - 1;
              newGroup = true;
            }
            if (newGroup) {
              chunks = new ArrayList<>();
              startOffsetOfChunkGroup = this.position() - 1;
              newGroup = false;
            }
            //if there is something wrong with a chunk, we will drop this part of data
            // (the whole ChunkGroup)
            ChunkHeader header = this.readChunkHeader();
            measurementID = header.getMeasurementID();
            if (newSchema != null) {
              newSchema.putIfAbsent(measurementID,
                  new MeasurementSchema(measurementID, header.getDataType(),
                      header.getEncodingType(), header.getCompressionType()));
            }
            dataType = header.getDataType();
            fileOffsetOfChunk = this.position() - 1;
            if (header.getNumOfPages() > 0) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              numOfPoints += pageHeader.getNumOfValues();
              startTimeOfChunk = pageHeader.getMinTimestamp();
              endTimeOfChunk = pageHeader.getMaxTimestamp();
              this.skipPageData(pageHeader);
            }
            for (int j = 1; j < header.getNumOfPages() - 1; j++) {
              //a new Page
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              this.skipPageData(pageHeader);
            }
            if (header.getNumOfPages() > 1) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              endTimeOfChunk = pageHeader.getMaxTimestamp();
              this.skipPageData(pageHeader);
            }
            currentChunk = new ChunkMetaData(measurementID, dataType, fileOffsetOfChunk,
                startTimeOfChunk, endTimeOfChunk);
            currentChunk.setNumOfPoints(numOfPoints);
            chunks.add(currentChunk);
            numOfPoints = 0;
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            //this is a chunk group
            //if there is something wrong with the chunkGroup Footer, we will drop this part of data
            //because we can not guarantee the correction of the deviceId.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            deviceID = chunkGroupFooter.getDeviceID();
            endOffsetOfChunkGroup = this.position();
            currentChunkGroup = new ChunkGroupMetaData(deviceID, chunks, startOffsetOfChunkGroup);
            currentChunkGroup.setEndOffsetOfChunkGroup(endOffsetOfChunkGroup);
            currentChunkGroup.setVersion(versionOfChunkGroup++);
            newMetaData.add(currentChunkGroup);
            // though we have read the current ChunkMetaData from Disk, it may be incomplete.
            // because if the file only loses one byte, the ChunkMetaData.deserialize() returns ok,
            // while the last filed of the ChunkMetaData is incorrect.
            // So, only reading the next MASK, can make sure that this ChunkMetaData is complete.
            haveReadAnUnverifiedGroupFooter = true;
            break;

          default:
            // it is impossible that we read an incorrect data.
            MetaMarker.handleUnexpectedMarker(marker);
            goon = false;
        }
      }
      //now we read the tail of the file, so we are sure that the last ChunkGroupFooter is complete.
      truncatedPosition = this.position() - 1;
    } catch (IOException e2) {
      //if it is the end of the file, and we read an unverifiedGroupFooter, we must remove this ChunkGroup
      if (haveReadAnUnverifiedGroupFooter && !newMetaData.isEmpty()) {
        newMetaData.remove(newMetaData.size() - 1);
      }
    } finally {
      //something wrong or all data is complete. We will discard current FileMetadata
      // so that we can continue to write data into this tsfile.
      return truncatedPosition;
    }
  }
}
