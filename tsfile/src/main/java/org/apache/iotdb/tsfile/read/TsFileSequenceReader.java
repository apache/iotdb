/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.exception.NotCompatibleException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.tsfile.write.writer.TsFileIOWriter.magicStringBytes;

public class TsFileSequenceReader implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSequenceReader.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  protected String file;
  private TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  private int totalChunkNum;
  private TsFileMetaData tsFileMetaData;
  private EndianType endianType = EndianType.BIG_ENDIAN;
  private Map<String, Map<String, TimeseriesMetaData>> cachedTimeseriesMetaDataMap;
  private boolean cacheDeviceMetadata;
  private ConcurrentHashMap deviceMetadataMap;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.getBytes().length
   * + TSFileConfig.NUMBER_VERSION.getBytes().length bytes of the file for preparing reading real
   * data.
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
   * @param file             -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
    if (resourceLogger.isInfoEnabled()) {
      resourceLogger.info("{} reader is opened. {}", file, getClass().getName());
    }
    this.file = file;
    tsFileInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(file);
    try {
      if (loadMetadataSize) {
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  // used in merge resource

  // TODO: deviceMetadataMap
  public TsFileSequenceReader(String file, boolean loadMetadata, boolean cacheDeviceMetadata)
      throws IOException {
    this(file, loadMetadata);
    this.cacheDeviceMetadata = cacheDeviceMetadata;
    if (cacheDeviceMetadata) {
      deviceMetadataMap = new ConcurrentHashMap<>();
    }
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.getBytes().length
   * + TSFileConfig.NUMBER_VERSION.getBytes().length bytes of the file for preparing reading real
   * data.
   *
   * @param input given input
   */
  public TsFileSequenceReader(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input            -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReader(TsFileInput input, boolean loadMetadataSize) throws IOException {
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
   * @param input            the input of a tsfile. The current position should be a markder and
   *                         then a chunk Header, rather than the magic number
   * @param fileMetadataPos  the position of the file metadata in the TsFileInput from the beginning
   *                         of the input to the current position
   * @param fileMetadataSize the byte size of the file metadata in the input
   */
  public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    this.tsFileInput = input;
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  public void loadMetadataSize() throws IOException {
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
    if (readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      tsFileInput.read(metadataSize,
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES);
      metadataSize.flip();
      // read file metadata size and position
      fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
      fileMetadataPos =
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES
              - fileMetadataSize;
    }
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
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * whether the file is a complete TsFile: only if the head magic and tail magic string exists.
   */
  public boolean isComplete() throws IOException {
    return tsFileInput.size() >= TSFileConfig.MAGIC_STRING.getBytes().length * 2
        + TSFileConfig.VERSION_NUMBER.getBytes().length
        && (readTailMagic().equals(readHeadMagic()) || readTailMagic()
        .equals(TSFileConfig.OLD_VERSION));
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
   *                     to the end of the magic head string.
   */
  public String readHeadMagic(boolean movePosition) throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
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
  public String readVersionNumber() throws IOException, NotCompatibleException {
    ByteBuffer versionNumberBytes = ByteBuffer
        .allocate(TSFileConfig.VERSION_NUMBER.getBytes().length);
    tsFileInput.read(versionNumberBytes, TSFileConfig.MAGIC_STRING.getBytes().length);
    versionNumberBytes.flip();
    return new String(versionNumberBytes.array());
  }

  public EndianType getEndianType() {
    return this.endianType;
  }

  /**
   * this function does not modify the position of the file reader.
   * @throws IOException io error
   */
  public TsFileMetaData readFileMetadata() throws IOException {
    if (tsFileMetaData == null) {
      tsFileMetaData = TsFileMetaData.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
    }
    return tsFileMetaData;
  }

  /**
   * this function reads measurements and TimeseriesMetaDatas in given device
   *
   * @param device name
   * @return the map measurementId -> TimeseriesMetaData in one device
   * @throws IOException io error
   */
  public Map<String, TimeseriesMetaData> readAllTimeseriesMetaDataInDevice(String device)
      throws IOException {
    if (cachedTimeseriesMetaDataMap == null) {
      cachedTimeseriesMetaDataMap = new HashMap<>();
    }
    if (cachedTimeseriesMetaDataMap.containsKey(device)) {
      return cachedTimeseriesMetaDataMap.get(device);
    }

    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    if (tsFileMetaData.getDeviceMetaDataMap() == null
        || !tsFileMetaData.getDeviceMetaDataMap().containsKey(device)) {
      return new HashMap<>();
    }
    Pair<Long, Integer> deviceMetadata = tsFileMetaData.getDeviceMetaDataMap().get(device);
    Map<String, TimeseriesMetaData> timeseriesMetaDataMapInOneDevice = new HashMap<>();
    ByteBuffer buffer = readData(deviceMetadata.left, deviceMetadata.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetaData tsMetaData = TimeseriesMetaData.deserializeFrom(buffer);
      if (tsMetaData != null) {
        timeseriesMetaDataMapInOneDevice.put(tsMetaData.getMeasurementId(), tsMetaData);
      }
    }
    cachedTimeseriesMetaDataMap.put(device, timeseriesMetaDataMapInOneDevice);
    return timeseriesMetaDataMapInOneDevice;
  }

  /**
   * this function reads ChunkMetaDataList in given device
   *
   * @param device name
   * @return ChunkMetaDataList
   * @throws IOException io error
   */
  public List<ChunkMetaData> readChunkMetadataInDevice(String device) throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    if (tsFileMetaData.getDeviceMetaDataMap() == null
        || !tsFileMetaData.getDeviceMetaDataMap().containsKey(device)) {
      return new ArrayList<>();
    }
    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    Pair<Long, Integer> deviceMetaData = tsFileMetaData.getDeviceMetaDataMap().get(device);
    ByteBuffer buffer = readData(deviceMetaData.left, deviceMetaData.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetaData timeseriesMetaData = TimeseriesMetaData.deserializeFrom(buffer);
      chunkMetaDataList.addAll(readChunkMetaDataList(timeseriesMetaData));
    }
    return chunkMetaDataList;
  }

  /**
   * this function reads all ChunkMetaData in this file
   *
   * @return ChunkMetaDataList
   * @throws IOException io error
   */
  public List<ChunkMetaData> readAllChunkMetadatas() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    if (tsFileMetaData.getDeviceMetaDataMap() == null) {
      return new ArrayList<>();
    }
    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    for (String deviceId : tsFileMetaData.getDeviceMetaDataMap().keySet()) {
      chunkMetaDataList.addAll(readChunkMetadataInDevice(deviceId));
    }
    return chunkMetaDataList;
  }

  /**
   * this function return all timeseries names in this file
   *
   * @return list of Paths
   * @throws IOException io error
   */
  public List<Path> getAllPaths() throws IOException {
    List<Path> paths = new ArrayList<>();
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, Pair<Long, Integer>> deviceMetaDataMap = tsFileMetaData.getDeviceMetaDataMap();
    for (Map.Entry<String, Pair<Long, Integer>> entry : deviceMetaDataMap.entrySet()) {
      String deviceId = entry.getKey();
      Pair<Long, Integer> deviceMetaData = entry.getValue();
      ByteBuffer buffer = readData(deviceMetaData.left, deviceMetaData.right);
      while (buffer.hasRemaining()) {
        TimeseriesMetaData tsMetaData = TimeseriesMetaData.deserializeFrom(buffer);
        paths.add(new Path(deviceId, tsMetaData.getMeasurementId()));
      }
    }
    return paths;
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
   * @param position   the offset of the chunk group footer in the file
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
   * @param position        the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead      true if the offset does not contains the marker , otherwise false
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize, boolean markerRead)
      throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize, markerRead);
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
    int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize, false);
    ByteBuffer buffer = readChunk(metaData.getOffsetOfChunkHeader() + header.getSerializedSize(),
        header.getDataSize());
    return new Chunk(header, buffer, metaData.getDeletedAt(), endianType);
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
  }

  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void position(long offset) throws IOException {
    tsFileInput.position(offset);
  }

  public void skipPageData(PageHeader header) throws IOException {
    tsFileInput.position(tsFileInput.position() + header.getCompressedSize());
  }

  public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
    return readPage(header, type, -1);
  }

  private ByteBuffer readPage(PageHeader header, CompressionType type, long position)
      throws IOException {
    ByteBuffer buffer = readData(position, header.getCompressedSize());
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
    if (type == CompressionType.UNCOMPRESSED) {
      return buffer;
    }// FIXME if the buffer is not array-implemented.
    unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(),
        uncompressedBuffer.array(),
        0);
    return uncompressedBuffer;
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
    if (resourceLogger.isInfoEnabled()) {
      resourceLogger.info("{} reader is closed.", file);
    }
    this.tsFileInput.close();
    // deviceMetadataMap = null;
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
   *                 position = -1
   * @param size     the size of data that want to read
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
    return ReadWriteIOUtils.readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   *
   * @param newSchema   @OUT. the measurement schema in the file will be added into this parameter.
   *                    (can be null)
   * @param chunkMetadataListMap   @OUT. the treeMap (Path -> ChunkmetadataList)
   *                    (can be null)
   * @param fastFinish  if true and the file is complete, then newSchema and newMetaData parameter
   *                    will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   * be truncated.
   */

  public long selfCheck(Map<Path, MeasurementSchema> newSchema, 
      Map<Path, List<ChunkMetaData>> chunkMetadataListMap,
      boolean fastFinish) throws IOException {
    File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
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

    List<ChunkMetaData> chunks = null;
    String deviceID;
    long startOffsetOfChunkGroup = 0;
    long endOffsetOfChunkGroup;

    if (fileSize < TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    String magic = readHeadMagic(true);
    tsFileInput.position(TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length) {
      return TsFileCheckStatus.ONLY_MAGIC_HEAD;
    } else if (readTailMagic().equals(magic)) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }
    boolean newChunkGroup = true;
    // not a complete file, we will recover it...
    long truncatedPosition = TSFileConfig.MAGIC_STRING.getBytes().length;
    boolean goon = true;
    byte marker;
    int chunkCnt = 0;
    List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while (goon && (marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
              chunks = new ArrayList<>();
              startOffsetOfChunkGroup = this.position() - 1;
            }
            fileOffsetOfChunk = this.position() - 1;
            // if there is something wrong with a chunk, we will drop the whole ChunkGroup
            // as different chunks may be created by the same insertions(sqls), and partial
            // insertion is not tolerable
            ChunkHeader header = this.readChunkHeader();
            measurementID = header.getMeasurementID();
            MeasurementSchema measurementSchema = new MeasurementSchema(measurementID,
                header.getDataType(),
                header.getEncodingType(), header.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            dataType = header.getDataType();
            Statistics<?> chunkStatistics = Statistics.getStatsByType(dataType);
            if (header.getNumOfPages() > 0) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            for (int j = 1; j < header.getNumOfPages() - 1; j++) {
              // a new Page
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            if (header.getNumOfPages() > 1) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            currentChunk = new ChunkMetaData(measurementID, dataType, fileOffsetOfChunk,
                chunkStatistics);
            chunks.add(currentChunk);
            chunkCnt++;
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            // this is a chunk group
            // if there is something wrong with the ChunkGroup Footer, we will drop this
            // ChunkGroup
            // because we can not guarantee the correctness of the deviceId.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            deviceID = chunkGroupFooter.getDeviceID();
            if (newSchema != null) {
              for (MeasurementSchema tsSchema : measurementSchemaList) {
                newSchema.putIfAbsent(new Path(deviceID, tsSchema.getMeasurementId()), tsSchema);
              }
            }
            if (chunkMetadataListMap != null) {
              for (ChunkMetaData chunk : chunks) {
                Path path = new Path(deviceID, chunk.getMeasurementUid());
                chunkMetadataListMap.computeIfAbsent(path, k -> new ArrayList<>()).add(chunk);
              }
            }
            endOffsetOfChunkGroup = this.position();
            newChunkGroup = true;
            truncatedPosition = this.position();

            totalChunkNum += chunkCnt;
            chunkCnt = 0;
            measurementSchemaList = new ArrayList<>();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            MetaMarker.handleUnexpectedMarker(marker);
            goon = false;
            logger.error(String
                .format("Unrecognized marker detected, this file {%s} may be corrupted", file));
        }
      }
      // now we read the tail of the data section, so we are sure that the last
      // ChunkGroupFooter is
      // complete.
      truncatedPosition = this.position() - 1;
    } catch (Exception e2) {
      e2.printStackTrace();
      logger.info("TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
          file,
          this.position(), e2.getMessage());
    }
    // Despite the completeness of the data section, we will discard current
    // FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedPosition;
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  /**
   * get ChunkMetaDatas in given path
   *
   * @param Path of timeseries
   * @return List of ChunkMetaData
   */
  public List<ChunkMetaData> getChunkMetadataList(Path path) throws IOException {
    Map<String, TimeseriesMetaData> timeseriesMetaDataMap =
        readAllTimeseriesMetaDataInDevice(path.getDevice());

    TimeseriesMetaData timeseriesMetaData = timeseriesMetaDataMap.get(path.getMeasurement());
    if (timeseriesMetaData == null) {
      return new ArrayList<>();
    }
    List<ChunkMetaData> chunkMetaDataList = readChunkMetaDataList(timeseriesMetaData);
    chunkMetaDataList.sort(Comparator.comparingLong(ChunkMetaData::getStartTime));
    return chunkMetaDataList;
  }

  /**
   * get ChunkMetaDatas in given TimeseriesMetaData
   *
   * @param TimeseriesMetaData
   * @return List of ChunkMetaData
   */
  public List<ChunkMetaData> readChunkMetaDataList(TimeseriesMetaData timeseriesMetaData)
      throws IOException {
    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    long startOffsetOfChunkMetadataList = timeseriesMetaData.getOffsetOfChunkMetaDataList();
    int dataSizeOfChunkMetadataList = timeseriesMetaData.getDataSizeOfChunkMetaDataList();
    ByteBuffer buffer = readData(startOffsetOfChunkMetadataList, dataSizeOfChunkMetadataList);
    while (buffer.hasRemaining()) {
      chunkMetaDataList.add(ChunkMetaData.deserializeFrom(buffer));
    }
    return chunkMetaDataList;
  }


  /**
   * get all TimeseriesMetaData in file, sorted by device Ids
   *
   * @return list of TimeseriesMetaData
   */
  public List<TimeseriesMetaData> getSortedTimeseriesMetaDataListByDeviceIds() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    List<TimeseriesMetaData> result = new ArrayList<>();
    for (Map.Entry<String, Pair<Long, Integer>> entry : tsFileMetaData.getDeviceMetaDataMap()
        .entrySet()) {
      // read TimeseriesMetaData from file
      ByteBuffer buffer = readData(entry.getValue().left, entry.getValue().right);
      while (buffer.hasRemaining()) {
        TimeseriesMetaData timeserieMetaData = TimeseriesMetaData.deserializeFrom(buffer);
        result.add(timeserieMetaData);
      }
    } // sort by the start offset Of the ChunkMetaDataList
    result.sort(Comparator.comparingLong(TimeseriesMetaData::getOffsetOfChunkMetaDataList));
    return result;
  }


  /**
   * get device names in range
   *
   * @param start start of the file
   * @param end   end of the file
   * @return device names in range
   */

  public List<String> getDeviceNameInRange(long start, long end) {
    List<String> res = new ArrayList<>();

    try {
      TsFileMetaData tsFileMetaData = readFileMetadata();
      for (Map.Entry<String, Pair<Long, Integer>> entry : tsFileMetaData.getDeviceMetaDataMap()
          .entrySet()) {
        List<ChunkMetaData> chunkMetaDataInOneDevices = readChunkMetadataInDevice(entry.getKey());
        LocateStatus mode = checkLocateStatus(chunkMetaDataInOneDevices, start,
            end);
        if (mode == LocateStatus.in) {
          res.add(entry.getKey());
          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return res;
  }

  public Map<String, List<TimeseriesMetaData>> getSortedTimeseriesMetaDataMap() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, List<TimeseriesMetaData>> result = new LinkedHashMap<>();
    for (Map.Entry<String, Pair<Long, Integer>> entry : tsFileMetaData.getDeviceMetaDataMap()
        .entrySet()) {
      // read TimeseriesMetaData from file
      String deviceId = entry.getKey();
      List<TimeseriesMetaData> timeseriesMetaDataList = new ArrayList<>();
      ByteBuffer buffer = readData(entry.getValue().left, entry.getValue().right);
      while (buffer.hasRemaining()) {
        TimeseriesMetaData timeserieMetaData = TimeseriesMetaData.deserializeFrom(buffer);
        timeseriesMetaDataList.add(timeserieMetaData);
      }
      result.put(deviceId, timeseriesMetaDataList);
    }
    return result;
  }

  /**
   * Check the location of a given chunkGroupMetaData with respect to a space partition constraint.
   *
   * @param chunkMetaDataList     the given chunkMetaDataList In one device
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos   the end position of the space partition
   * @return LocateStatus
   */
  private LocateStatus checkLocateStatus(List<ChunkMetaData> chunkMetaDataList,
      long spacePartitionStartPos, long spacePartitionEndPos) {
    Collections.sort(chunkMetaDataList, new Comparator<ChunkMetaData>(){
      @Override
      public int compare(ChunkMetaData chunkMetaData1, ChunkMetaData chunkMetaData2) {
        return (int) (chunkMetaData1.getOffsetOfChunkHeader() - chunkMetaData2.getOffsetOfChunkHeader());
      }
    });
    long middleOffset = chunkMetaDataList.get(chunkMetaDataList.size() / 2)
        .getOffsetOfChunkHeader();
    if (spacePartitionStartPos <= middleOffset && middleOffset < spacePartitionEndPos) {
      return LocateStatus.in;
    } else if (middleOffset < spacePartitionStartPos) {
      return LocateStatus.before;
    } else {
      return LocateStatus.after;
    }
  }

  /**
   * The location of a chunkGroupMetaData with respect to a space partition constraint.
   * <p>
   * in - the middle point of the chunkGroupMetaData is located in the current space partition.
   * before - the middle point of the chunkGroupMetaData is located before the current space
   * partition. after - the middle point of the chunkGroupMetaData is located after the current
   * space partition.
   */
  private enum LocateStatus {
    in, before, after
  }
}
