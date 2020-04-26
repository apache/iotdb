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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.ChildMetadataIndexType;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.MetadataIndex;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.VersionUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private TsFileMetadata tsFileMetaData;
  private EndianType endianType = EndianType.BIG_ENDIAN;
  // device -> measurement -> TimeseriesMetadata
  private Map<String, Map<String, TimeseriesMetadata>> cachedDeviceMetadata = new ConcurrentHashMap<>();
  private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private boolean cacheDeviceMetadata;

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
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is opened. {}", file, getClass().getName());
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
  public TsFileSequenceReader(String file, boolean loadMetadata, boolean cacheDeviceMetadata)
      throws IOException {
    this(file, loadMetadata);
    this.cacheDeviceMetadata = cacheDeviceMetadata;
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
   * @param input -given input
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
   * to the end of the magic head string.
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
  public String readVersionNumber() throws IOException {
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
   *
   * @throws IOException io error
   */
  public TsFileMetadata readFileMetadata() throws IOException {
    if (tsFileMetaData == null) {
      tsFileMetaData = TsFileMetadata.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
    }
    return tsFileMetaData;
  }

  /**
   * this function reads measurements and TimeseriesMetaDatas in given device Thread Safe
   *
   * @param device name
   * @return the map measurementId -> TimeseriesMetaData in one device
   * @throws IOException io error
   */
  public Map<String, TimeseriesMetadata> readDeviceMetadata(String device) throws IOException {
    if (!cacheDeviceMetadata) {
      return readDeviceMetadataFromDisk(device);
    }

    cacheLock.readLock().lock();
    try {
      if (cachedDeviceMetadata.containsKey(device)) {
        return cachedDeviceMetadata.get(device);
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    cacheLock.writeLock().lock();
    try {
      if (cachedDeviceMetadata.containsKey(device)) {
        return cachedDeviceMetadata.get(device);
      }
      readFileMetadata();
      Map<String, TimeseriesMetadata> deviceMetadata = readDeviceMetadataFromDisk(device);
      cachedDeviceMetadata.put(device, deviceMetadata);
      return deviceMetadata;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private Map<String, TimeseriesMetadata> readDeviceMetadataFromDisk(String device)
      throws IOException {
    readFileMetadata();
    List<TimeseriesMetadata> timeseriesMetadataList = getDeviceTimeseriesMetadata(device);
    Map<String, TimeseriesMetadata> deviceMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      deviceMetadata.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }
    return deviceMetadata;
  }

  public TimeseriesMetadata readMeasurementMetadata(Path path) throws IOException {
    readFileMetadata();
    List<MetadataIndex> deviceMetadataIndexList = tsFileMetaData.getDeviceMetadataIndex();
    Pair<MetadataIndex, Long> metadataIndexPair = getDeviceMetaDataAndEndOffset(
        deviceMetadataIndexList, path.getDevice());
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(),
        metadataIndexPair.right);
    while (!metadataIndexPair.left.getChildMetadataIndexType()
        .equals(ChildMetadataIndexType.MEASUREMENT)) {
      List<MetadataIndex> measurementMetadataIndexList = new ArrayList<>();
      while (buffer.hasRemaining()) {
        measurementMetadataIndexList.add(MetadataIndex.deserializeFrom(buffer));
      }
      metadataIndexPair = getMeasurementMetaDataAndEndOffset(
          measurementMetadataIndexList, path.getMeasurement());
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer));
    }
    String[] measurementNameList = timeseriesMetadataList.stream()
        .map(TimeseriesMetadata::getMeasurementId).collect(Collectors.toList())
        .toArray(new String[timeseriesMetadataList.size()]);

    return timeseriesMetadataList
        .get(Arrays.binarySearch(measurementNameList, path.getMeasurement()));
  }

  public List<String> getDevicesByMetadata(List<MetadataIndex> metadataIndexList)
      throws IOException {
    Set<String> deviceSet = new TreeSet<>();
    for (int i = 0; i < metadataIndexList.size() - 1; i++) {
      MetadataIndex metadataIndex = metadataIndexList.get(i);
      switch (metadataIndex.getChildMetadataIndexType()) {
        case DEVICE:
        case MEASUREMENT:
        case MEASUREMENT_INDEX:
          for (MetadataIndex index : metadataIndexList) {
            if (!index.getName().equals("")) {
              deviceSet.add(index.getName());
            }
          }
          break;
        case DEVICE_INDEX:
          ByteBuffer buffer = readData(metadataIndex.getOffset(),
              metadataIndexList.get(i + 1).getOffset());
          List<MetadataIndex> nextMetadataIndexList = new ArrayList<>();
          while (buffer.hasRemaining()) {
            MetadataIndex nextMetadataIndex = MetadataIndex.deserializeFrom(buffer);
            nextMetadataIndexList.add(nextMetadataIndex);

          }
          for (String name : getDevicesByMetadata(nextMetadataIndexList)) {
            if (!name.equals("")) {
              deviceSet.add(name);
            }
          }
          break;
        default:
          throw new IOException("Error TsFileMetadata to get devices");
      }

    }
    return new ArrayList<>(deviceSet);
  }

  /**
   * read all ChunkMetaDatas of given device
   *
   * @param device name
   * @return measurement -> ChunkMetadata list
   * @throws IOException io error
   */
  public Map<String, List<ChunkMetadata>> readChunkMetadataInDevice(String device)
      throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }

    long start = 0;
    int size = 0;
    List<TimeseriesMetadata> timeseriesMetadataMap = getDeviceTimeseriesMetadata(device);
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap) {
      if (start == 0) {
        start = timeseriesMetadata.getOffsetOfChunkMetaDataList();
      }
      size += timeseriesMetadata.getDataSizeOfChunkMetaDataList();
    }
    // read buffer of all ChunkMetadatas of this device
    ByteBuffer buffer = readData(start, size);

    Map<String, List<ChunkMetadata>> seriesMetadata = new HashMap<>();

    while (buffer.hasRemaining()) {
      ChunkMetadata chunkMetadata = ChunkMetadata.deserializeFrom(buffer);
      seriesMetadata.computeIfAbsent(chunkMetadata.getMeasurementUid(), key -> new ArrayList<>())
          .add(chunkMetadata);
    }

    // set version in ChunkMetadata
    List<Pair<Long, Long>> versionInfo = tsFileMetaData.getVersionInfo();
    for (Entry<String, List<ChunkMetadata>> entry : seriesMetadata.entrySet()) {
      VersionUtils.applyVersion(entry.getValue(), versionInfo);
    }
    return seriesMetadata;
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
    List<MetadataIndex> metadataIndexList = tsFileMetaData.getDeviceMetadataIndex();
    List<String> devices = getDevicesByMetadata(metadataIndexList);
    for (String device : devices) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (String measurementId : timeseriesMetadataMap.keySet()) {
        paths.add(new Path(device, measurementId));
      }
    }
    return paths;
  }

  /**
   * Traverse the metadata index from top-level MetadataIndex to get TimeseriesMetadatas
   *
   * @param metadataIndex top-level MetadataIndex
   * @param buffer byte buffer
   * @param timeseriesMetadataMap map deviceId -> timeseriesMetadata list
   */
  private void analyzeMetadataIndex(MetadataIndex metadataIndex, ByteBuffer buffer,
      Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap) throws IOException {
    String deviceId;
    switch (metadataIndex.getChildMetadataIndexType()) {
      case DEVICE_INDEX:
      case DEVICE:
      case MEASUREMENT_INDEX:
        List<MetadataIndex> metadataIndexList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          metadataIndexList.add(MetadataIndex.deserializeFrom(buffer));
        }
        for (int i = 0; i < metadataIndexList.size() - 1; i++) {
          ByteBuffer nextBuffer = readData(metadataIndexList.get(i).getOffset(),
              metadataIndexList.get(i + 1).getOffset());
          analyzeMetadataIndex(metadataIndexList.get(i), nextBuffer, timeseriesMetadataMap);
        }
        break;
      case MEASUREMENT:
        deviceId = metadataIndex.getName();
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer));
        }
        if (timeseriesMetadataMap.containsKey(deviceId)) {
          timeseriesMetadataList.addAll(timeseriesMetadataMap.get(deviceId));
        }
        timeseriesMetadataMap.put(deviceId, timeseriesMetadataList);
        break;
    }
  }

  private List<TimeseriesMetadata> getDeviceTimeseriesMetadata(String device) throws IOException {
    List<MetadataIndex> deviceMetadataIndexList = tsFileMetaData.getDeviceMetadataIndex();
    Pair<MetadataIndex, Long> metadataIndexPair = getDeviceMetaDataAndEndOffset(
        deviceMetadataIndexList, device);
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    analyzeMetadataIndex(metadataIndexPair.left, buffer, timeseriesMetadataMap);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  private Pair<MetadataIndex, Long> getDeviceMetaDataAndEndOffset(
      List<MetadataIndex> metadataIndexList, String device) throws IOException {
    int size = metadataIndexList.size();
    String[] deviceNameList = metadataIndexList.stream().map(MetadataIndex::getName).collect(
        Collectors.toList()).toArray(new String[size]);
    int deviceIndex = Arrays.binarySearch(deviceNameList, device);
    if (deviceIndex < 0) {
      deviceIndex = -deviceIndex - 2;
      if (deviceIndex == size - 1) {
        deviceIndex--;
      }
    }
    MetadataIndex metadataIndex = metadataIndexList.get(deviceIndex);
    if (!metadataIndex.getChildMetadataIndexType().equals(ChildMetadataIndexType.DEVICE_INDEX)) {
      return new Pair<>(metadataIndex, metadataIndexList.get(deviceIndex + 1).getOffset());
    }
    List<MetadataIndex> nextMetadataIndexList = new ArrayList<>();
    ByteBuffer buffer = readData(metadataIndex.getOffset(),
        metadataIndexList.get(deviceIndex + 1).getOffset());
    while (buffer.hasRemaining()) {
      nextMetadataIndexList.add(MetadataIndex.deserializeFrom(buffer));
    }
    return getDeviceMetaDataAndEndOffset(nextMetadataIndexList, device);
  }

  private Pair<MetadataIndex, Long> getMeasurementMetaDataAndEndOffset(
      List<MetadataIndex> metadataIndexList, String measurement) throws IOException {
    int size = metadataIndexList.size();
    String[] measurementnameList = metadataIndexList.stream().map(MetadataIndex::getName).collect(
        Collectors.toList()).toArray(new String[size]);
    int measurementIndex = Arrays.binarySearch(measurementnameList, measurement);
    if (measurementIndex < 0) {
      measurementIndex = -measurementIndex - 2;
      if (measurementIndex == size - 1) {
        measurementIndex--;
      }
    }
    MetadataIndex metadataIndex = metadataIndexList.get(measurementIndex);
    if (!metadataIndex.getChildMetadataIndexType()
        .equals(ChildMetadataIndexType.MEASUREMENT_INDEX)) {
      return new Pair<>(metadataIndex, metadataIndexList.get(measurementIndex + 1).getOffset());
    }
    List<MetadataIndex> nextMetadataIndexList = new ArrayList<>();
    ByteBuffer buffer = readData(metadataIndex.getOffset(),
        metadataIndexList.get(measurementIndex + 1).getOffset());
    while (buffer.hasRemaining()) {
      nextMetadataIndexList.add(MetadataIndex.deserializeFrom(buffer));
    }
    return getMeasurementMetaDataAndEndOffset(nextMetadataIndexList, measurement);
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

  public long readVersion() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    return buffer.getLong();
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
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead true if the offset does not contains the marker , otherwise false
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize, boolean markerRead)
      throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize, markerRead);
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
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
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

  public void close() throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is closed.", file);
    }
    this.tsFileInput.close();
  }

  public String getFileName() {
    return this.file;
  }

  public long fileSize() throws IOException {
    return tsFileInput.size();
  }

  public long getStartOffset() throws IOException {
    List<MetadataIndex> metadataIndexList = readFileMetadata().getDeviceMetadataIndex();
    MetadataIndex metadataIndex = metadataIndexList.get(0);
    Long endOffset = metadataIndexList.get(1).getOffset();
    while (!metadataIndex.getChildMetadataIndexType().equals(ChildMetadataIndexType.MEASUREMENT)) {
      ByteBuffer buffer = readData(metadataIndex.getOffset(), endOffset);
      List<MetadataIndex> nextMetadataIndexList = new ArrayList<>();
      while (buffer.hasRemaining()) {
        nextMetadataIndexList.add(MetadataIndex.deserializeFrom(buffer));
      }
      metadataIndex = nextMetadataIndexList.get(0);
      endOffset = nextMetadataIndexList.get(1).getOffset();
    }
    return metadataIndex.getOffset();
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
    if (position < 0) {
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
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position.
   *
   * @param start the start position of data in the tsFileInput, or the current position if position
   * = -1
   * @param end the end position of data that want to read
   * @return data that been read.
   */
  private ByteBuffer readData(long start, long end) throws IOException {
    return readData(start, (int) (end - start));
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
   * @param newSchema the schema on each time series in the file
   * @param chunkGroupMetadataList ChunkGroupMetadata List
   * @param fastFinish if true and the file is complete, then newSchema and newMetaData parameter
   * will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   * be truncated.
   */

  public long selfCheck(Map<Path, MeasurementSchema> newSchema,
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      List<Pair<Long, Long>> versionInfo,
      boolean fastFinish) throws IOException {
    File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
    long fileSize;
    if (!checkFile.exists()) {
      return TsFileCheckStatus.FILE_NOT_FOUND;
    } else {
      fileSize = checkFile.length();
    }
    ChunkMetadata currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;

    // ChunkMetadata of current ChunkGroup
    List<ChunkMetadata> chunkMetadataList = null;
    String deviceID;

    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length;
    if (fileSize < headerLength) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    String magic = readHeadMagic(true);
    tsFileInput.position(headerLength);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    if (fileSize == headerLength) {
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
    byte marker;
    int chunkCnt = 0;
    List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
              chunkMetadataList = new ArrayList<>();
            }
            fileOffsetOfChunk = this.position() - 1;
            // if there is something wrong with a chunk, we will drop the whole ChunkGroup
            // as different chunks may be created by the same insertions(sqls), and partial
            // insertion is not tolerable
            ChunkHeader chunkHeader = this.readChunkHeader();
            measurementID = chunkHeader.getMeasurementID();
            MeasurementSchema measurementSchema = new MeasurementSchema(measurementID,
                chunkHeader.getDataType(),
                chunkHeader.getEncodingType(), chunkHeader.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            dataType = chunkHeader.getDataType();
            Statistics<?> chunkStatistics = Statistics.getStatsByType(dataType);
            for (int j = 0; j < chunkHeader.getNumOfPages(); j++) {
              // a new Page
              PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            currentChunk = new ChunkMetadata(measurementID, dataType, fileOffsetOfChunk,
                chunkStatistics);
            chunkMetadataList.add(currentChunk);
            chunkCnt++;
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            // this is a chunk group
            // if there is something wrong with the ChunkGroup Footer, we will drop this ChunkGroup
            // because we can not guarantee the correctness of the deviceId.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            deviceID = chunkGroupFooter.getDeviceID();
            if (newSchema != null) {
              for (MeasurementSchema tsSchema : measurementSchemaList) {
                newSchema.putIfAbsent(new Path(deviceID, tsSchema.getMeasurementId()), tsSchema);
              }
            }
            chunkGroupMetadataList.add(new ChunkGroupMetadata(deviceID, chunkMetadataList));
            newChunkGroup = true;
            truncatedPosition = this.position();

            totalChunkNum += chunkCnt;
            chunkCnt = 0;
            measurementSchemaList = new ArrayList<>();
            break;
          case MetaMarker.VERSION:
            long version = readVersion();
            versionInfo.add(new Pair<>(position(), version));
            truncatedPosition = this.position();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
      // now we read the tail of the data section, so we are sure that the last
      // ChunkGroupFooter is complete.
      truncatedPosition = this.position() - 1;
    } catch (Exception e) {
      logger.info("TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
          file, this.position(), e.getMessage());
    }
    // Despite the completeness of the data section, we will discard current FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedPosition;
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  /**
   * get ChunkMetaDatas of given path
   *
   * @param path timeseries path
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> getChunkMetadataList(Path path) throws IOException {
    TimeseriesMetadata timeseriesMetaData = readMeasurementMetadata(path);
    if (timeseriesMetaData == null) {
      return new ArrayList<>();
    }
    List<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetaData);
    chunkMetadataList.sort(Comparator.comparingLong(ChunkMetadata::getStartTime));
    return chunkMetadataList;
  }

  /**
   * get ChunkMetaDatas in given TimeseriesMetaData
   *
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> readChunkMetaDataList(TimeseriesMetadata timeseriesMetaData)
      throws IOException {
    List<Pair<Long, Long>> versionInfo = tsFileMetaData.getVersionInfo();
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    long startOffsetOfChunkMetadataList = timeseriesMetaData.getOffsetOfChunkMetaDataList();
    int dataSizeOfChunkMetadataList = timeseriesMetaData.getDataSizeOfChunkMetaDataList();

    ByteBuffer buffer = readData(startOffsetOfChunkMetadataList, dataSizeOfChunkMetadataList);
    while (buffer.hasRemaining()) {
      chunkMetadataList.add(ChunkMetadata.deserializeFrom(buffer));
    }

    VersionUtils.applyVersion(chunkMetadataList, versionInfo);

    return chunkMetadataList;
  }

  /**
   * get all measurements in this file
   *
   * @return measurement -> datatype
   */
  public Map<String, TSDataType> getAllMeasurements() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, TSDataType> result = new HashMap<>();
    List<MetadataIndex> metadataIndexList = tsFileMetaData.getDeviceMetadataIndex();
    List<String> devices = getDevicesByMetadata(metadataIndexList);
    for (String device : devices) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      }
    }
    return result;
  }

  /**
   * get device names which has valid chunks in [start, end)
   *
   * @param start start of the partition
   * @param end end of the partition
   * @return device names in range
   */
  public List<String> getDeviceNameInRange(long start, long end) throws IOException {
    List<String> res = new ArrayList<>();
    readFileMetadata();
    List<String> devices = getDevicesByMetadata(tsFileMetaData.getDeviceMetadataIndex());
    for (String device : devices) {
      Map<String, List<ChunkMetadata>> seriesMetadataMap = readChunkMetadataInDevice(device);
      if (hasDataInPartition(seriesMetadataMap, start, end)) {
        res.add(device);
      }
    }

    return res;
  }

  /**
   * Check if the device has at least one Chunk in this partition
   *
   * @param seriesMetadataMap chunkMetaDataList of each measurement
   * @param start the start position of the space partition
   * @param end the end position of the space partition
   */
  private boolean hasDataInPartition(Map<String, List<ChunkMetadata>> seriesMetadataMap,
      long start, long end) {
    for (List<ChunkMetadata> chunkMetadataList : seriesMetadataMap.values()) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        LocateStatus location = MetadataQuerierByFileImpl
            .checkLocateStatus(chunkMetadata, start, end);
        if (location == LocateStatus.in) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The location of a chunkGroupMetaData with respect to a space partition constraint. <p> in - the
   * middle point of the chunkGroupMetaData is located in the current space partition. before - the
   * middle point of the chunkGroupMetaData is located before the current space partition. after -
   * the middle point of the chunkGroupMetaData is located after the current space partition.
   */
  public enum LocateStatus {
    in, before, after
  }
}
