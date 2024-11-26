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
package org.apache.tsfile.write.writer;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.tsmiterator.TSMIterator;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.addCurrentIndexNodeToQueue;
import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.checkAndBuildLevelIndex;
import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.generateRootNode;
import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.splitDeviceByTable;

/**
 * TsFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter implements AutoCloseable {

  protected static final byte[] MAGIC_STRING_BYTES;
  public static final byte VERSION_NUMBER_BYTE;
  protected static final TSFileConfig TS_FILE_CONFIG = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  static {
    MAGIC_STRING_BYTES = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    VERSION_NUMBER_BYTE = TSFileConfig.VERSION_NUMBER;
  }

  /** schema of this TsFile. */
  protected Schema schema = new Schema();

  protected TsFileOutput out;
  protected boolean canWrite = true;
  protected File file;

  // current flushed Chunk
  protected ChunkMetadata currentChunkMetadata;
  // current flushed ChunkGroup
  protected List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  // all flushed ChunkGroups
  protected List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();

  private long markedPosition;
  private IDeviceID currentChunkGroupDeviceId;

  // the two longs marks the index range of operations in current MemTable
  // and are serialized after MetaMarker.OPERATION_INDEX_RANGE to recover file-level range
  private long minPlanIndex;
  private long maxPlanIndex;

  // the following variable is used for memory control
  protected long maxMetadataSize;
  protected long currentChunkMetadataSize = 0L;
  protected File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected volatile boolean hasChunkMetadataInDisk = false;
  // record the total num of path in order to make bloom filter
  protected int pathCount = 0;
  private Path lastSerializePath = null;
  protected LinkedList<Long> endPosInCMTForDevice = new LinkedList<>();
  private volatile int chunkMetadataCount = 0;
  public static final String CHUNK_METADATA_TEMP_FILE_SUFFIX = ".meta";

  private boolean generateTableSchema = false;

  protected String encryptLevel;

  protected String encryptType;

  protected String encryptKey;

  /** empty construct function. */
  protected TsFileIOWriter() {
    if (TS_FILE_CONFIG.getEncryptFlag()) {
      this.encryptLevel = "2";
      this.encryptType = TS_FILE_CONFIG.getEncryptType();
      this.encryptKey = EncryptUtils.normalKeyStr;
    } else {
      this.encryptLevel = "0";
      this.encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      this.encryptKey = null;
    }
  }

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this(file, TS_FILE_CONFIG);
  }

  /** for test only */
  public TsFileIOWriter(File file, TSFileConfig conf) throws IOException {
    this.out = FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), false);
    this.file = file;
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} writer is opened.", file.getName());
    }
    if (conf.getEncryptFlag()) {
      this.encryptLevel = "2";
      this.encryptType = conf.getEncryptType();
      this.encryptKey = EncryptUtils.normalKeyStr;
    } else {
      this.encryptLevel = "0";
      this.encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      this.encryptKey = null;
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.out = output;
    if (TS_FILE_CONFIG.getEncryptFlag()) {
      this.encryptLevel = "2";
      this.encryptType = TS_FILE_CONFIG.getEncryptType();
      this.encryptKey = EncryptUtils.normalKeyStr;
    } else {
      this.encryptLevel = "0";
      this.encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      this.encryptKey = null;
    }
    startFile();
  }

  /** for test only */
  public TsFileIOWriter(TsFileOutput output, boolean test) {
    this.out = output;
  }

  /** for write with memory control */
  public TsFileIOWriter(File file, long maxMetadataSize) throws IOException {
    this(file);
    this.maxMetadataSize = maxMetadataSize;
    chunkMetadataTempFile = new File(file.getAbsolutePath() + CHUNK_METADATA_TEMP_FILE_SUFFIX);
  }

  public void setEncryptParam(String encryptLevel, String encryptType, String encryptKey) {
    this.encryptLevel = encryptLevel;
    this.encryptType = encryptType;
    this.encryptKey = encryptKey;
  }

  /**
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes) throws IOException {
    bytes.writeTo(out.wrapAsStream());
  }

  protected void startFile() throws IOException {
    out.write(MAGIC_STRING_BYTES);
    out.write(VERSION_NUMBER_BYTE);
  }

  public int startChunkGroup(IDeviceID deviceId) throws IOException {
    this.currentChunkGroupDeviceId = deviceId;
    if (logger.isDebugEnabled()) {
      logger.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    }
    chunkMetadataList = new ArrayList<>();
    ChunkGroupHeader chunkGroupHeader = new ChunkGroupHeader(currentChunkGroupDeviceId);
    return chunkGroupHeader.serializeTo(out.wrapAsStream());
  }

  /**
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup() throws IOException {
    if (currentChunkGroupDeviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }

    ChunkGroupMetadata chunkGroupMetadata =
        new ChunkGroupMetadata(currentChunkGroupDeviceId, chunkMetadataList);
    if (generateTableSchema) {
      getSchema().updateTableSchema(chunkGroupMetadata);
    }
    chunkGroupMetadataList.add(chunkGroupMetadata);
    currentChunkGroupDeviceId = null;
    chunkMetadataList = null;
    out.flush();
  }

  /**
   * For TsFileReWriteTool / UpgradeTool. Use this method to determine if needs to start a
   * ChunkGroup.
   *
   * @return isWritingChunkGroup
   */
  public boolean isWritingChunkGroup() {
    return currentChunkGroupDeviceId != null;
  }

  /**
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param measurementId - measurementId of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @param mask - 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(
      String measurementId,
      CompressionType compressionCodecName,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics,
      int dataSize,
      int numOfPages,
      int mask)
      throws IOException {

    currentChunkMetadata =
        new ChunkMetadata(
            measurementId,
            tsDataType,
            encodingType,
            compressionCodecName,
            out.getPosition(),
            statistics);
    currentChunkMetadata.setMask((byte) mask);

    ChunkHeader header =
        new ChunkHeader(
            measurementId,
            dataSize,
            tsDataType,
            compressionCodecName,
            encodingType,
            numOfPages,
            mask);
    header.serializeTo(out.wrapAsStream());
  }

  /** Write a whole chunk in another file into this file. Providing fast merge for IoTDB. */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata =
        new ChunkMetadata(
            chunkHeader.getMeasurementID(),
            chunkHeader.getDataType(),
            chunkHeader.getEncodingType(),
            chunkHeader.getCompressionType(),
            out.getPosition(),
            chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "end flushing a chunk:{}, totalvalue:{}",
          chunkHeader.getMeasurementID(),
          chunkMetadata.getNumOfPoints());
    }
  }

  /** Write an empty value chunk into file directly. Only used for aligned timeseries. */
  public void writeEmptyValueChunk(
      String measurementId,
      CompressionType compressionType,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics)
      throws IOException {
    currentChunkMetadata =
        new ChunkMetadata(
            measurementId,
            tsDataType,
            encodingType,
            compressionType,
            out.getPosition(),
            statistics);
    currentChunkMetadata.setMask(TsFileConstant.VALUE_COLUMN_MASK);
    ChunkHeader emptyChunkHeader =
        new ChunkHeader(
            measurementId,
            0,
            tsDataType,
            compressionType,
            encodingType,
            0,
            TsFileConstant.VALUE_COLUMN_MASK);
    emptyChunkHeader.serializeTo(out.wrapAsStream());
    endCurrentChunk();
  }

  public void writeChunk(Chunk chunk) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata =
        new ChunkMetadata(
            chunkHeader.getMeasurementID(),
            chunkHeader.getDataType(),
            chunkHeader.getEncodingType(),
            chunkHeader.getCompressionType(),
            out.getPosition(),
            chunk.getChunkStatistic());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
  }

  /** end chunk and write some log. */
  public void endCurrentChunk() {
    this.currentChunkMetadataSize += currentChunkMetadata.getRetainedSizeInBytes();
    chunkMetadataCount++;
    chunkMetadataList.add(currentChunkMetadata);
    currentChunkMetadata = null;
  }

  /**
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @throws IOException if I/O error occurs
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void endFile() throws IOException {
    checkInMemoryPathCount();
    readChunkMetadataAndConstructIndexTree();

    long footerIndex = out.getPosition();
    if (logger.isDebugEnabled()) {
      logger.debug("start to flush the footer,file pos:{}", footerIndex);
    }

    // write magic string
    out.write(MAGIC_STRING_BYTES);

    // flush page cache data to disk
    out.force();
    // close file
    out.close();

    if (resourceLogger.isDebugEnabled() && file != null) {
      resourceLogger.debug("{} writer is closed.", file.getName());
    }
    if (file != null) {
      File chunkMetadataFile = new File(file.getAbsolutePath() + CHUNK_METADATA_TEMP_FILE_SUFFIX);
      if (chunkMetadataFile.exists()) {
        FileUtils.delete(chunkMetadataFile);
      }
    }
    canWrite = false;
  }

  private void checkInMemoryPathCount() {
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      pathCount += chunkGroupMetadata.getChunkMetadataList().size();
    }
  }

  private void readChunkMetadataAndConstructIndexTree() throws IOException {
    if (tempOutput != null) {
      tempOutput.close();
    }
    long metaOffset = out.getPosition();

    // serialize the SEPARATOR of MetaData
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());

    TSMIterator tsmIterator =
        hasChunkMetadataInDisk
            ? TSMIterator.getTSMIteratorInDisk(
                chunkMetadataTempFile, chunkGroupMetadataList, endPosInCMTForDevice)
            : TSMIterator.getTSMIteratorInMemory(chunkGroupMetadataList);
    Map<IDeviceID, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();
    Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
    IDeviceID currentDevice = null;
    IDeviceID prevDevice = null;
    Path currentPath = null;
    MetadataIndexNode currentIndexNode =
        new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
    int seriesIdxForCurrDevice = 0;
    BloomFilter filter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), pathCount);

    while (tsmIterator.hasNext()) {
      // read in all chunk metadata of one series
      // construct the timeseries metadata for this series
      Pair<Path, TimeseriesMetadata> timeseriesMetadataPair = tsmIterator.next();
      TimeseriesMetadata timeseriesMetadata = timeseriesMetadataPair.right;
      currentPath = timeseriesMetadataPair.left;

      // build bloom filter
      filter.add(currentPath.getFullPath());
      // construct the index tree node for the series
      currentDevice = currentPath.getIDeviceID();
      if (!currentDevice.equals(prevDevice)) {
        if (prevDevice != null) {
          addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
          deviceMetadataIndexMap.put(
              prevDevice,
              generateRootNode(
                  measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
          currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
        }
        measurementMetadataIndexQueue = new ArrayDeque<>();
        seriesIdxForCurrDevice = 0;
      }

      if (seriesIdxForCurrDevice % TS_FILE_CONFIG.getMaxDegreeOfIndexNode() == 0) {
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
          currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
        }
        if (timeseriesMetadata.getTsDataType() != TSDataType.VECTOR) {
          currentIndexNode.addEntry(
              new MeasurementMetadataIndexEntry(currentPath.getMeasurement(), out.getPosition()));
        } else {
          currentIndexNode.addEntry(new MeasurementMetadataIndexEntry("", out.getPosition()));
        }
      }

      prevDevice = currentDevice;
      seriesIdxForCurrDevice++;
      // serialize the timeseries metadata to file
      timeseriesMetadata.serializeTo(out.wrapAsStream());
    }

    addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
    if (prevDevice != null) {
      deviceMetadataIndexMap.put(
          prevDevice,
          generateRootNode(
              measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
    }

    Map<String, Map<IDeviceID, MetadataIndexNode>> tableDeviceNodesMap =
        splitDeviceByTable(deviceMetadataIndexMap);

    // build an index root for each table
    Map<String, MetadataIndexNode> tableNodesMap = new TreeMap<>();
    for (Entry<String, Map<IDeviceID, MetadataIndexNode>> entry : tableDeviceNodesMap.entrySet()) {
      tableNodesMap.put(entry.getKey(), checkAndBuildLevelIndex(entry.getValue(), out));
    }

    TsFileMetadata tsFileMetadata = new TsFileMetadata();
    tsFileMetadata.setTableMetadataIndexNodeMap(tableNodesMap);
    tsFileMetadata.setTableSchemaMap(schema.getTableSchemaMap());
    tsFileMetadata.setMetaOffset(metaOffset);
    tsFileMetadata.setBloomFilter(filter);
    tsFileMetadata.addProperty("encryptLevel", encryptLevel);
    tsFileMetadata.addProperty("encryptType", encryptType);
    tsFileMetadata.addProperty("encryptKey", encryptKey);

    int size = tsFileMetadata.serializeTo(out.wrapAsStream());

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException {
    return out.getPosition();
  }

  // device -> ChunkMetadataList
  public Map<IDeviceID, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<IDeviceID, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      deviceChunkMetadataMap
          .computeIfAbsent(chunkGroupMetadata.getDevice(), k -> new ArrayList<>())
          .addAll(chunkGroupMetadata.getChunkMetadataList());
    }
    return deviceChunkMetadataMap;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public void mark() throws IOException {
    markedPosition = getPos();
  }

  public void reset() throws IOException {
    out.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    out.close();
    if (tempOutput != null) {
      this.tempOutput.close();
    }
  }

  void writeSeparatorMaskForTest() throws IOException {
    out.write(new byte[] {MetaMarker.SEPARATOR});
  }

  void writeChunkGroupMarkerForTest() throws IOException {
    out.write(new byte[] {MetaMarker.CHUNK_GROUP_HEADER});
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public void writePlanIndices() throws IOException {
    ReadWriteIOUtils.write(MetaMarker.OPERATION_INDEX_RANGE, out.wrapAsStream());
    ReadWriteIOUtils.write(minPlanIndex, out.wrapAsStream());
    ReadWriteIOUtils.write(maxPlanIndex, out.wrapAsStream());
    out.flush();
  }

  public void truncate(long offset) throws IOException {
    out.truncate(offset);
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return out;
  }

  /**
   * This method should be called before flushing chunk group metadata list, otherwise, it will
   * return null.
   */
  public List<ChunkMetadata> getChunkMetadataListOfCurrentDeviceInMemory() {
    return chunkMetadataList;
  }

  /**
   * this function is for Upgrade Tool and Split Tool.
   *
   * @return DeviceTimeseriesMetadataMap
   */
  public Map<IDeviceID, List<TimeseriesMetadata>> getDeviceTimeseriesMetadataMap() {
    Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap = new TreeMap<>();
    Map<IDeviceID, Map<String, List<IChunkMetadata>>> chunkMetadataMap = new TreeMap<>();
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        chunkMetadataMap
            .computeIfAbsent(chunkGroupMetadata.getDevice(), x -> new TreeMap<>())
            .computeIfAbsent(chunkMetadata.getMeasurementUid(), x -> new ArrayList<>())
            .add(chunkMetadata);
      }
    }
    for (IDeviceID device : chunkMetadataMap.keySet()) {
      Map<String, List<IChunkMetadata>> seriesToChunkMetadataMap = chunkMetadataMap.get(device);
      for (Map.Entry<String, List<IChunkMetadata>> entry : seriesToChunkMetadataMap.entrySet()) {
        try {
          deviceTimeseriesMetadataMap
              .computeIfAbsent(device, x -> new ArrayList<>())
              .add(TSMIterator.constructOneTimeseriesMetadata(entry.getKey(), entry.getValue()));
        } catch (IOException e) {
          logger.error("Failed to get device timeseries metadata map", e);
          return null;
        }
      }
    }

    return deviceTimeseriesMetadataMap;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }

  /**
   * Check if the size of chunk metadata in memory is greater than the given threshold. If so, the
   * chunk metadata will be written to a temp files. <b>Notice! If you are writing a aligned device
   * in row, you should make sure all data of current writing device has been written before this
   * method is called. For writing not aligned series or writing aligned series in column, you
   * should make sure that all data of one series is written before you call this function.</b>
   *
   * @throws IOException
   */
  public int checkMetadataSizeAndMayFlush() throws IOException {
    // This function should be called after all data of an aligned device has been written
    if (currentChunkMetadataSize > maxMetadataSize) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Flushing chunk metadata, total size is {}, count is {}, avg size is {}",
              currentChunkMetadataSize,
              chunkMetadataCount,
              currentChunkMetadataSize / chunkMetadataCount);
        }
        return sortAndFlushChunkMetadata();
      } catch (IOException e) {
        logger.error("Meets exception when flushing metadata to temp file for {}", file, e);
        throw e;
      }
    } else {
      return 0;
    }
  }

  /**
   * Sort the chunk metadata by the lexicographical order and the start time of the chunk, then
   * flush them to a temp file.
   *
   * @throws IOException
   */
  protected int sortAndFlushChunkMetadata() throws IOException {
    int writtenSize = 0;
    // group by series
    List<Pair<Path, List<IChunkMetadata>>> sortedChunkMetadataList =
        TSMIterator.sortChunkMetadata(
            chunkGroupMetadataList, currentChunkGroupDeviceId, chunkMetadataList);
    if (tempOutput == null) {
      tempOutput = new LocalTsFileOutput(new FileOutputStream(chunkMetadataTempFile));
    }
    hasChunkMetadataInDisk = true;
    for (Pair<Path, List<IChunkMetadata>> pair : sortedChunkMetadataList) {
      Path seriesPath = pair.left;
      boolean isNewPath = !seriesPath.equals(lastSerializePath);
      if (isNewPath) {
        // record the count of path to construct bloom filter later
        pathCount++;
      }
      List<IChunkMetadata> iChunkMetadataList = pair.right;
      writtenSize += writeChunkMetadataToTempFile(iChunkMetadataList, seriesPath, isNewPath);
      lastSerializePath = seriesPath;
      logger.debug("Flushing {}", seriesPath);
    }
    // clear the cache metadata to release the memory
    chunkGroupMetadataList.clear();
    if (chunkMetadataList != null) {
      chunkMetadataList.clear();
    }
    chunkMetadataCount = 0;
    currentChunkMetadataSize = 0;
    return writtenSize;
  }

  private int writeChunkMetadataToTempFile(
      List<IChunkMetadata> iChunkMetadataList, Path seriesPath, boolean isNewPath)
      throws IOException {
    int writtenSize = 0;
    // [DeviceId] measurementId datatype size chunkMetadataBuffer
    if (lastSerializePath == null
        || !seriesPath.getIDeviceID().equals(lastSerializePath.getIDeviceID())) {
      // mark the end position of last device
      endPosInCMTForDevice.add(tempOutput.getPosition());
      // serialize the device
      // for each device, we only serialize it once, in order to save io
      writtenSize += seriesPath.getIDeviceID().serialize(tempOutput.wrapAsStream());
    }
    if (isNewPath && !iChunkMetadataList.isEmpty()) {
      // serialize the public info of this measurement
      writtenSize +=
          ReadWriteIOUtils.writeVar(seriesPath.getMeasurement(), tempOutput.wrapAsStream());
      writtenSize +=
          ReadWriteIOUtils.write(
              iChunkMetadataList.get(0).getDataType(), tempOutput.wrapAsStream());
    }
    PublicBAOS buffer = new PublicBAOS();
    int totalSize = 0;
    for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
      totalSize += chunkMetadata.serializeTo(buffer, true);
    }
    writtenSize += ReadWriteIOUtils.write(totalSize, tempOutput.wrapAsStream());
    buffer.writeTo(tempOutput);
    writtenSize += buffer.size();
    return writtenSize;
  }

  public List<ChunkGroupMetadata> getChunkGroupMetadataList() {
    return chunkGroupMetadataList;
  }

  public void flush() throws IOException {
    out.flush();
  }

  public TsFileOutput getTsFileOutput() {
    return this.out;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public boolean isGenerateTableSchema() {
    return generateTableSchema;
  }

  public void setGenerateTableSchema(boolean generateTableSchema) {
    this.generateTableSchema = generateTableSchema;
  }
}
