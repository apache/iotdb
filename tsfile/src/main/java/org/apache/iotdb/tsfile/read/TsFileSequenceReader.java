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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.exception.TsFileStatisticsMistakesException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TsFileSequenceReader implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSequenceReader.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final String METADATA_INDEX_NODE_DESERIALIZE_ERROR =
      "Something error happened while deserializing MetadataIndexNode of file {}";
  private static final int MAX_READ_BUFFER_SIZE = 4 * 1024 * 1024;
  protected String file;
  protected TsFileInput tsFileInput;
  protected long fileMetadataPos;
  protected int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  protected volatile TsFileMetadata tsFileMetaData;
  // device -> measurement -> TimeseriesMetadata
  private Map<String, Map<String, TimeseriesMetadata>> cachedDeviceMetadata =
      new ConcurrentHashMap<>();
  private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private boolean cacheDeviceMetadata;
  private long minPlanIndex = Long.MAX_VALUE;
  private long maxPlanIndex = Long.MIN_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
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
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
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
    this.file = input.getFilePath();
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
   * @param input the input of a tsfile. The current position should be a marker and then a chunk
   *     Header, rather than the magic number
   * @param fileMetadataPos the position of the file metadata in the TsFileInput from the beginning
   *     of the input to the current position
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
      tsFileInput.read(
          metadataSize,
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES);
      metadataSize.flip();
      // read file metadata size and position
      fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
      fileMetadataPos =
          tsFileInput.size()
              - TSFileConfig.MAGIC_STRING.getBytes().length
              - Integer.BYTES
              - fileMetadataSize;
    }
  }

  public long getFileMetadataPos() {
    return fileMetadataPos;
  }

  public int getTsFileMetadataSize() {
    return fileMetadataSize;
  }

  /**
   * Return the whole meta data size of this tsfile, including ChunkMetadata, TimeseriesMetadata and
   * etc.
   */
  public long getFileMetadataSize() throws IOException {
    return tsFileInput.size() - getFileMetadataPos();
  }

  /** this function does not modify the position of the file reader. */
  public String readTailMagic() throws IOException {
    long totalSize = tsFileInput.size();
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /** whether the file is a complete TsFile: only if the head magic and tail magic string exists. */
  public boolean isComplete() throws IOException {
    long size = tsFileInput.size();
    // TSFileConfig.MAGIC_STRING.getBytes().length * 2 for two magic string
    // Byte.BYTES for the file version number
    if (size >= TSFileConfig.MAGIC_STRING.getBytes().length * 2 + Byte.BYTES) {
      String tailMagic = readTailMagic();
      String headMagic = readHeadMagic();
      return tailMagic.equals(headMagic);
    } else {
      return false;
    }
  }

  /** this function does not modify the position of the file reader. */
  public String readHeadMagic() throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, 0);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /** this function reads version number and checks compatibility of TsFile. */
  public byte readVersionNumber() throws IOException {
    ByteBuffer versionNumberByte = ByteBuffer.allocate(Byte.BYTES);
    tsFileInput.read(versionNumberByte, TSFileConfig.MAGIC_STRING.getBytes().length);
    versionNumberByte.flip();
    return versionNumberByte.get();
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public TsFileMetadata readFileMetadata() throws IOException {
    try {
      if (tsFileMetaData == null) {
        synchronized (this) {
          if (tsFileMetaData == null) {
            tsFileMetaData =
                TsFileMetadata.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
          }
        }
      }
    } catch (Exception e) {
      logger.error("Something error happened while reading file metadata of file {}", file);
      throw e;
    }
    return tsFileMetaData;
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public BloomFilter readBloomFilter() throws IOException {
    readFileMetadata();
    return tsFileMetaData.getBloomFilter();
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
    List<TimeseriesMetadata> timeseriesMetadataList =
        getDeviceTimeseriesMetadataWithoutChunkMetadata(device);
    Map<String, TimeseriesMetadata> deviceMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      deviceMetadata.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }
    return deviceMetadata;
  }

  public TimeseriesMetadata readTimeseriesMetadata(Path path, boolean ignoreNotExists)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      if (ignoreNotExists) {
        return null;
      }
      throw new IOException("Device {" + path.getDevice() + "} is not in tsFileMetaData");
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      } catch (Exception e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    if (metadataIndexPair == null) {
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      try {
        timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
      } catch (Exception e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
    }
    // return null if path does not exist in the TsFile
    int searchResult =
        binarySearchInTimeseriesMetadataList(timeseriesMetadataList, path.getMeasurement());
    return searchResult >= 0 ? timeseriesMetadataList.get(searchResult) : null;
  }

  // This method is only used for TsFile
  public ITimeSeriesMetadata readITimeseriesMetadata(Path path, boolean ignoreNotExists)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      if (ignoreNotExists) {
        return null;
      }
      throw new IOException("Device {" + path.getDevice() + "} is not in tsFileMetaData");
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode;
    TimeseriesMetadata firstTimeseriesMetadata;
    try {
      // next layer MeasurementNode of the specific DeviceNode
      metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
    } catch (Exception e) {
      logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
      throw e;
    }
    firstTimeseriesMetadata = tryToGetFirstTimeseriesMetadata(metadataIndexNode);
    metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);

    if (metadataIndexPair == null) {
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      try {
        timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
      } catch (Exception e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
    }
    // return null if path does not exist in the TsFile
    int searchResult =
        binarySearchInTimeseriesMetadataList(timeseriesMetadataList, path.getMeasurement());
    if (searchResult >= 0) {
      if (firstTimeseriesMetadata != null) {
        List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
        valueTimeseriesMetadataList.add(timeseriesMetadataList.get(searchResult));
        return new AlignedTimeSeriesMetadata(firstTimeseriesMetadata, valueTimeseriesMetadataList);
      } else {
        return timeseriesMetadataList.get(searchResult);
      }
    } else {
      return null;
    }
  }

  /* Find the leaf node that contains path, return all the sensors in that leaf node which are also in allSensors set */
  public List<TimeseriesMetadata> readTimeseriesMetadata(Path path, Set<String> allSensors)
      throws IOException {
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getLeafMetadataIndexPair(path);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();

    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata;
      try {
        timeseriesMetadata = TimeseriesMetadata.deserializeFrom(buffer, true);
      } catch (Exception e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
      if (allSensors.contains(timeseriesMetadata.getMeasurementId())) {
        timeseriesMetadataList.add(timeseriesMetadata);
      }
    }
    return timeseriesMetadataList;
  }

  /* Get leaf MetadataIndexPair which contains path */
  private Pair<MetadataIndexEntry, Long> getLeafMetadataIndexPair(Path path) throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      return null;
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      } catch (Exception e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    return metadataIndexPair;
  }

  // This method is only used for TsFile
  public List<ITimeSeriesMetadata> readITimeseriesMetadata(String device, Set<String> measurements)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, device, true, false);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<ITimeSeriesMetadata> resultTimeseriesMetadataList = new ArrayList<>();
    List<String> measurementList = new ArrayList<>(measurements);
    Set<String> measurementsHadFound = new HashSet<>();
    // the content of next Layer MeasurementNode of the specific device's DeviceNode
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Pair<MetadataIndexEntry, Long> measurementMetadataIndexPair = metadataIndexPair;
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();

    // next layer MeasurementNode of the specific DeviceNode
    MetadataIndexNode measurementMetadataIndexNode;
    try {
      measurementMetadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
    } catch (Exception e) {
      logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
      throw e;
    }
    // Get the first timeseriesMetadata of the device
    TimeseriesMetadata firstTimeseriesMetadata =
        tryToGetFirstTimeseriesMetadata(measurementMetadataIndexNode);

    for (int i = 0; i < measurementList.size(); i++) {
      if (measurementsHadFound.contains(measurementList.get(i))) {
        continue;
      }
      timeseriesMetadataList.clear();
      measurementMetadataIndexPair =
          getMetadataAndEndOffset(
              measurementMetadataIndexNode, measurementList.get(i), false, false);

      if (measurementMetadataIndexPair == null) {
        continue;
      }
      // the content of TimeseriesNode of the specific MeasurementLeafNode
      buffer =
          readData(
              measurementMetadataIndexPair.left.getOffset(), measurementMetadataIndexPair.right);
      while (buffer.hasRemaining()) {
        try {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
        } catch (Exception e) {
          logger.error(
              "Something error happened while deserializing TimeseriesMetadata of file {}", file);
          throw e;
        }
      }
      for (int j = i; j < measurementList.size(); j++) {
        String current = measurementList.get(j);
        if (!measurementsHadFound.contains(current)) {
          int searchResult = binarySearchInTimeseriesMetadataList(timeseriesMetadataList, current);
          if (searchResult >= 0) {
            if (firstTimeseriesMetadata != null) {
              List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();
              valueTimeseriesMetadataList.add(timeseriesMetadataList.get(searchResult));
              resultTimeseriesMetadataList.add(
                  new AlignedTimeSeriesMetadata(
                      firstTimeseriesMetadata, valueTimeseriesMetadataList));
            } else {
              resultTimeseriesMetadataList.add(timeseriesMetadataList.get(searchResult));
            }
            measurementsHadFound.add(current);
          }
        }
        if (measurementsHadFound.size() == measurements.size()) {
          return resultTimeseriesMetadataList;
        }
      }
    }
    return resultTimeseriesMetadataList;
  }

  protected int binarySearchInTimeseriesMetadataList(
      List<TimeseriesMetadata> timeseriesMetadataList, String key) {
    int low = 0;
    int high = timeseriesMetadataList.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      TimeseriesMetadata midVal = timeseriesMetadataList.get(mid);
      int cmp = midVal.getMeasurementId().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -1; // key not found
  }

  public List<String> getAllDevices() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    return getAllDevices(tsFileMetaData.getMetadataIndex());
  }

  private List<String> getAllDevices(MetadataIndexNode metadataIndexNode) throws IOException {
    List<String> deviceList = new ArrayList<>();
    // if metadataIndexNode is LEAF_DEVICE, put all devices in node entry into the list
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      deviceList.addAll(
          metadataIndexNode.getChildren().stream()
              .map(x -> x.getName().intern())
              .collect(Collectors.toList()));
      return deviceList;
    }

    int metadataIndexListSize = metadataIndexNode.getChildren().size();
    for (int i = 0; i < metadataIndexListSize; i++) {
      long endOffset = metadataIndexNode.getEndOffset();
      if (i != metadataIndexListSize - 1) {
        endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
      MetadataIndexNode node = MetadataIndexNode.deserializeFrom(buffer);
      deviceList.addAll(getAllDevices(node));
    }
    return deviceList;
  }

  /**
   * @return an iterator of "device, isAligned" list, in which names of devices are ordered in
   *     dictionary order, and isAligned represents whether the device is aligned. Only read devices
   *     on one device leaf node each time to save memory.
   */
  public TsFileDeviceIterator getAllDevicesIteratorWithIsAligned() throws IOException {
    readFileMetadata();
    Queue<Pair<String, long[]>> queue = new LinkedList<>();
    List<long[]> leafDeviceNodeOffsets = new ArrayList<>();
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      // the first node of index tree is device leaf node, then get the devices directly
      getDevicesOfLeafNode(metadataIndexNode, queue);
    } else {
      // get all device leaf node offset
      getAllDeviceLeafNodeOffset(metadataIndexNode, leafDeviceNodeOffsets);
    }

    return new TsFileDeviceIterator(this, leafDeviceNodeOffsets, queue);
  }

  /**
   * Get devices and first measurement node offset.
   *
   * @param startOffset start offset of device leaf node
   * @param endOffset end offset of device leaf node
   * @param measurementNodeOffsetQueue device -> first measurement node offset
   */
  public void getDevicesAndEntriesOfOneLeafNode(
      Long startOffset, Long endOffset, Queue<Pair<String, long[]>> measurementNodeOffsetQueue)
      throws IOException {
    try {
      ByteBuffer nextBuffer = readData(startOffset, endOffset);
      MetadataIndexNode deviceLeafNode = MetadataIndexNode.deserializeFrom(nextBuffer);
      getDevicesOfLeafNode(deviceLeafNode, measurementNodeOffsetQueue);
    } catch (Exception e) {
      logger.error("Something error happened while getting all devices of file {}", file);
      throw e;
    }
  }

  /**
   * Get all devices and its corresponding entries on the specific device leaf node.
   *
   * @param deviceLeafNode this node must be device leaf node
   */
  private void getDevicesOfLeafNode(
      MetadataIndexNode deviceLeafNode, Queue<Pair<String, long[]>> measurementNodeOffsetQueue) {
    if (!deviceLeafNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      throw new RuntimeException("the first param should be device leaf node.");
    }
    List<MetadataIndexEntry> childrenEntries = deviceLeafNode.getChildren();
    for (int i = 0; i < childrenEntries.size(); i++) {
      MetadataIndexEntry deviceEntry = childrenEntries.get(i);
      long childStartOffset = deviceEntry.getOffset();
      long childEndOffset =
          i == childrenEntries.size() - 1
              ? deviceLeafNode.getEndOffset()
              : childrenEntries.get(i + 1).getOffset();
      long[] offset = {childStartOffset, childEndOffset};
      measurementNodeOffsetQueue.add(new Pair<>(deviceEntry.getName(), offset));
    }
  }

  /**
   * Get the device leaf node offset under the specific device internal node.
   *
   * @param deviceInternalNode this node must be device internal node
   */
  private void getAllDeviceLeafNodeOffset(
      MetadataIndexNode deviceInternalNode, List<long[]> leafDeviceNodeOffsets) throws IOException {
    if (!deviceInternalNode.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE)) {
      throw new RuntimeException("the first param should be device internal node.");
    }
    try {
      int metadataIndexListSize = deviceInternalNode.getChildren().size();
      boolean isCurrentLayerLeafNode = false;
      for (int i = 0; i < metadataIndexListSize; i++) {
        MetadataIndexEntry entry = deviceInternalNode.getChildren().get(i);
        long startOffset = entry.getOffset();
        long endOffset = deviceInternalNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = deviceInternalNode.getChildren().get(i + 1).getOffset();
        }
        if (i == 0) {
          // check is current layer device leaf node or device internal node. Just need to check the
          // first entry, because the rest are the same
          MetadataIndexNodeType nodeType =
              MetadataIndexNodeType.deserialize(
                  ReadWriteIOUtils.readByte(readData(endOffset - 1, endOffset)));
          isCurrentLayerLeafNode = nodeType.equals(MetadataIndexNodeType.LEAF_DEVICE);
        }
        if (isCurrentLayerLeafNode) {
          // is device leaf node
          long[] offset = {startOffset, endOffset};
          leafDeviceNodeOffsets.add(offset);
          continue;
        }
        ByteBuffer nextBuffer = readData(startOffset, endOffset);
        getAllDeviceLeafNodeOffset(
            MetadataIndexNode.deserializeFrom(nextBuffer), leafDeviceNodeOffsets);
      }
    } catch (Exception e) {
      logger.error("Something error happened while getting all devices of file {}", file);
      throw e;
    }
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
    readFileMetadata();
    List<TimeseriesMetadata> timeseriesMetadataMap = getDeviceTimeseriesMetadata(device);
    if (timeseriesMetadataMap.isEmpty()) {
      return new HashMap<>();
    }
    Map<String, List<ChunkMetadata>> seriesMetadata = new LinkedHashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap) {
      seriesMetadata.put(
          timeseriesMetadata.getMeasurementId(),
          timeseriesMetadata.getChunkMetadataList().stream()
              .map(chunkMetadata -> ((ChunkMetadata) chunkMetadata))
              .collect(Collectors.toList()));
    }
    return seriesMetadata;
  }

  /**
   * this function return all timeseries names
   *
   * @return list of Paths
   * @throws IOException io error
   */
  public List<Path> getAllPaths() throws IOException {
    List<Path> paths = new ArrayList<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (String measurementId : timeseriesMetadataMap.keySet()) {
        paths.add(new Path(device, measurementId, true));
      }
    }
    return paths;
  }

  /**
   * @return an iterator of timeseries list, in which names of timeseries are ordered in dictionary
   *     order
   * @throws IOException io error
   */
  public Iterator<List<Path>> getPathsIterator() throws IOException {
    readFileMetadata();

    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
    Queue<Pair<String, Pair<Long, Long>>> queue = new LinkedList<>();
    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = metadataIndexNode.getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
      getAllPaths(metadataIndexEntry, buffer, null, metadataIndexNode.getNodeType(), queue);
    }
    return new Iterator<List<Path>>() {
      @Override
      public boolean hasNext() {
        return !queue.isEmpty();
      }

      @Override
      public List<Path> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Pair<String, Pair<Long, Long>> startEndPair = queue.remove();
        List<Path> paths = new ArrayList<>();
        try {
          ByteBuffer nextBuffer = readData(startEndPair.right.left, startEndPair.right.right);
          while (nextBuffer.hasRemaining()) {
            paths.add(
                new Path(
                    startEndPair.left,
                    TimeseriesMetadata.deserializeFrom(nextBuffer, false).getMeasurementId(),
                    true));
          }
          return paths;
        } catch (IOException e) {
          throw new TsFileRuntimeException(
              "Error occurred while reading a time series metadata block.");
        }
      }
    };
  }

  private void getAllPaths(
      MetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      String deviceId,
      MetadataIndexNodeType type,
      Queue<Pair<String, Pair<Long, Long>>> queue)
      throws IOException {
    try {
      if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
        deviceId = metadataIndex.getName();
      }
      MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      int metadataIndexListSize = metadataIndexNode.getChildren().size();
      for (int i = 0; i < metadataIndexListSize; i++) {
        long startOffset = metadataIndexNode.getChildren().get(i).getOffset();
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          queue.add(new Pair<>(deviceId, new Pair<>(startOffset, endOffset)));
          continue;
        }
        ByteBuffer nextBuffer =
            readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
        getAllPaths(
            metadataIndexNode.getChildren().get(i),
            nextBuffer,
            deviceId,
            metadataIndexNode.getNodeType(),
            queue);
      }
    } catch (Exception e) {
      logger.error("Something error happened while getting all paths of file {}", file);
      throw e;
    }
  }

  /**
   * Check whether the deivce is aligned or not.
   *
   * @param measurementNode the next measurement layer node of specific device node
   */
  public boolean isAlignedDevice(MetadataIndexNode measurementNode) {
    return "".equals(measurementNode.getChildren().get(0).getName());
  }

  TimeseriesMetadata tryToGetFirstTimeseriesMetadata(MetadataIndexNode measurementNode)
      throws IOException {
    // Not aligned timeseries
    if (!"".equals(measurementNode.getChildren().get(0).getName())) {
      return null;
    }

    // Aligned timeseries
    if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      ByteBuffer buffer;
      if (measurementNode.getChildren().size() > 1) {
        buffer =
            readData(
                measurementNode.getChildren().get(0).getOffset(),
                measurementNode.getChildren().get(1).getOffset());
      } else {
        buffer =
            readData(
                measurementNode.getChildren().get(0).getOffset(), measurementNode.getEndOffset());
      }
      return TimeseriesMetadata.deserializeFrom(buffer, true);
    } else if (measurementNode.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT)) {
      ByteBuffer buffer =
          readData(
              measurementNode.getChildren().get(0).getOffset(),
              measurementNode.getChildren().get(1).getOffset());
      MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      return tryToGetFirstTimeseriesMetadata(metadataIndexNode);
    }
    return null;
  }

  /**
   * Get the measurements of current device by its first measurement node. Also get the chunk
   * metadata list and timeseries metadata offset.
   *
   * @param measurementNode first measurement node of the device
   * @param excludedMeasurementIds do not deserialize chunk metadatas whose measurementId is in the
   *     set. Notice: It only takes effect when the needChunkMetadata parameter is true.
   * @param needChunkMetadata need to deserialize chunk metadatas or not
   * @return measurement -> chunk metadata list -> timeseries metadata <startOffset, endOffset>
   */
  public Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>>
      getTimeseriesMetadataOffsetByDevice(
          MetadataIndexNode measurementNode,
          Set<String> excludedMeasurementIds,
          boolean needChunkMetadata)
          throws IOException {
    Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new HashMap<>();
    List<MetadataIndexEntry> childrenEntryList = measurementNode.getChildren();
    for (int i = 0; i < childrenEntryList.size(); i++) {
      long startOffset = childrenEntryList.get(i).getOffset();
      long endOffset =
          i == childrenEntryList.size() - 1
              ? measurementNode.getEndOffset()
              : childrenEntryList.get(i + 1).getOffset();
      ByteBuffer nextBuffer = readData(startOffset, endOffset);
      if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        // leaf measurement node
        while (nextBuffer.hasRemaining()) {
          int metadataStartOffset = nextBuffer.position();
          TimeseriesMetadata timeseriesMetadata =
              TimeseriesMetadata.deserializeFrom(
                  nextBuffer, excludedMeasurementIds, needChunkMetadata);
          timeseriesMetadataOffsetMap.put(
              timeseriesMetadata.getMeasurementId(),
              new Pair<>(
                  timeseriesMetadata.getChunkMetadataList(),
                  new Pair<>(
                      startOffset + metadataStartOffset, startOffset + nextBuffer.position())));
        }

      } else {
        // internal measurement node
        MetadataIndexNode nextLayerMeasurementNode = MetadataIndexNode.deserializeFrom(nextBuffer);
        timeseriesMetadataOffsetMap.putAll(
            getTimeseriesMetadataOffsetByDevice(
                nextLayerMeasurementNode, excludedMeasurementIds, needChunkMetadata));
      }
    }
    return timeseriesMetadataOffsetMap;
  }

  /**
   * Get chunk metadata list by the start offset and end offset of the timeseries metadata.
   *
   * @param startOffset the start offset of timeseries metadata
   * @param endOffset the end offset of timeseries metadata
   */
  public List<IChunkMetadata> getChunkMetadataListByTimeseriesMetadataOffset(
      long startOffset, long endOffset) throws IOException {
    ByteBuffer timeseriesMetadataBuffer = readData(startOffset, endOffset);

    TimeseriesMetadata timeseriesMetadata =
        TimeseriesMetadata.deserializeFrom(timeseriesMetadataBuffer, true);
    return timeseriesMetadata.getChunkMetadataList();
  }

  /**
   * Get timeseries metadata under the measurementNode and put them into timeseriesMetadataList.
   * Skip timeseries whose measurementId is in the excludedMeasurementIds.
   *
   * @param measurementNode next layer measurement node of specific device leaf node
   * @param excludedMeasurementIds skip timeseries whose measurementId is in the set
   */
  public void getDeviceTimeseriesMetadata(
      List<TimeseriesMetadata> timeseriesMetadataList,
      MetadataIndexNode measurementNode,
      Set<String> excludedMeasurementIds,
      boolean needChunkMetadata)
      throws IOException {
    int metadataIndexListSize = measurementNode.getChildren().size();
    for (int i = 0; i < metadataIndexListSize; i++) {
      long endOffset = measurementNode.getEndOffset();
      if (i != metadataIndexListSize - 1) {
        endOffset = measurementNode.getChildren().get(i + 1).getOffset();
      }
      ByteBuffer nextBuffer = readData(measurementNode.getChildren().get(i).getOffset(), endOffset);
      if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        // leaf measurement node
        while (nextBuffer.hasRemaining()) {
          TimeseriesMetadata timeseriesMetadata =
              TimeseriesMetadata.deserializeFrom(
                  nextBuffer, excludedMeasurementIds, needChunkMetadata);
          if (!excludedMeasurementIds.contains(timeseriesMetadata.getMeasurementId())) {
            timeseriesMetadataList.add(timeseriesMetadata);
          }
        }
      } else {
        // internal measurement node
        MetadataIndexNode nextLayerMeasurementNode = MetadataIndexNode.deserializeFrom(nextBuffer);
        getDeviceTimeseriesMetadata(
            timeseriesMetadataList,
            nextLayerMeasurementNode,
            excludedMeasurementIds,
            needChunkMetadata);
      }
    }
  }

  /**
   * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
   *
   * @param metadataIndex MetadataIndexEntry
   * @param buffer byte buffer
   * @param deviceId String
   * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
   * @param needChunkMetadata deserialize chunk metadata list or not
   */
  private void generateMetadataIndex(
      MetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      String deviceId,
      MetadataIndexNodeType type,
      Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap,
      boolean needChunkMetadata)
      throws IOException {
    try {
      if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata));
        }
        timeseriesMetadataMap
            .computeIfAbsent(deviceId, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
      } else {
        // deviceId should be determined by LEAF_DEVICE node
        if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
          deviceId = metadataIndex.getName();
        }
        MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          ByteBuffer nextBuffer =
              readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
          generateMetadataIndex(
              metadataIndexNode.getChildren().get(i),
              nextBuffer,
              deviceId,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap,
              needChunkMetadata);
        }
      }
    } catch (Exception e) {
      logger.error("Something error happened while generating MetadataIndex of file {}", file);
      throw e;
    }
  }

  /* TimeseriesMetadata don't need deserialize chunk metadata list */
  public Map<String, List<TimeseriesMetadata>> getAllTimeseriesMetadata(boolean needChunkMetadata)
      throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new HashMap<>();
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = metadataIndexNode.getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
      generateMetadataIndex(
          metadataIndexEntry,
          buffer,
          null,
          metadataIndexNode.getNodeType(),
          timeseriesMetadataMap,
          needChunkMetadata);
    }
    return timeseriesMetadataMap;
  }

  /* This method will only deserialize the TimeseriesMetadata, not including chunk metadata list */
  private List<TimeseriesMetadata> getDeviceTimeseriesMetadataWithoutChunkMetadata(String device)
      throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        false);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  /* This method will not only deserialize the TimeseriesMetadata, but also all the chunk metadata list meanwhile. */
  private List<TimeseriesMetadata> getDeviceTimeseriesMetadata(String device) throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        true);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  /**
   * Get target MetadataIndexEntry and its end offset
   *
   * @param metadataIndex given MetadataIndexNode
   * @param name target device / measurement name
   * @param isDeviceLevel whether target MetadataIndexNode is device level
   * @param exactSearch whether is in exact search mode, return null when there is no entry with
   *     name; or else return the nearest MetadataIndexEntry before it (for deeper search)
   * @return target MetadataIndexEntry, endOffset pair
   */
  protected Pair<MetadataIndexEntry, Long> getMetadataAndEndOffset(
      MetadataIndexNode metadataIndex, String name, boolean isDeviceLevel, boolean exactSearch)
      throws IOException {
    try {
      // When searching for a device node, return when it is not INTERNAL_DEVICE
      // When searching for a measurement node, return when it is not INTERNAL_MEASUREMENT
      if ((isDeviceLevel
              && !metadataIndex.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE))
          || (!isDeviceLevel
              && !metadataIndex.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT))) {
        return metadataIndex.getChildIndexEntry(name, exactSearch);
      } else {
        Pair<MetadataIndexEntry, Long> childIndexEntry =
            metadataIndex.getChildIndexEntry(name, false);
        ByteBuffer buffer = readData(childIndexEntry.left.getOffset(), childIndexEntry.right);
        return getMetadataAndEndOffset(
            MetadataIndexNode.deserializeFrom(buffer), name, isDeviceLevel, exactSearch);
      }
    } catch (Exception e) {
      logger.error("Something error happened while deserializing MetadataIndex of file {}", file);
      throw e;
    }
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupHeader() throws IOException {
    return ChunkGroupHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER.
   *
   * @param position the offset of the chunk group footer in the file
   * @param markerRead true if the offset does not contains the marker , otherwise false
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupHeader(long position, boolean markerRead)
      throws IOException {
    return ChunkGroupHeader.deserializeFrom(tsFileInput, position, markerRead);
  }

  public void readPlanIndex() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    minPlanIndex = buffer.getLong();
    buffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    maxPlanIndex = buffer.getLong();
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader(byte chunkType) throws IOException {
    try {
      return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), chunkType);
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading chunk header of {}", t.getMessage(), file);
      throw t;
    }
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize) throws IOException {
    try {
      return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize);
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading chunk header of {}", t.getMessage(), file);
      throw t;
    }
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  public ByteBuffer readChunk(long position, int dataSize) throws IOException {
    try {
      return readData(position, dataSize);
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading chunk of {}", t.getMessage(), file);
      throw t;
    }
  }

  /**
   * read memory chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
    try {
      int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());
      ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize);
      ByteBuffer buffer =
          readChunk(
              metaData.getOffsetOfChunkHeader() + header.getSerializedSize(), header.getDataSize());
      return new Chunk(header, buffer, metaData.getDeleteIntervalList(), metaData.getStatistics());
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading chunk of {}", t.getMessage(), file);
      throw t;
    }
  }

  /**
   * read memory chunk.
   *
   * @param chunkCacheKey given key of chunk LRUCache
   * @return chunk
   */
  public Chunk readMemChunk(CachedChunkLoaderImpl.ChunkCacheKey chunkCacheKey) throws IOException {
    int chunkHeadSize = ChunkHeader.getSerializedSize(chunkCacheKey.getMeasurementUid());
    ChunkHeader header = readChunkHeader(chunkCacheKey.getOffsetOfChunkHeader(), chunkHeadSize);
    ByteBuffer buffer =
        readChunk(
            chunkCacheKey.getOffsetOfChunkHeader() + header.getSerializedSize(),
            header.getDataSize());
    return new Chunk(
        header, buffer, chunkCacheKey.getDeleteIntervalList(), chunkCacheKey.getStatistics());
  }

  /**
   * read the {@link CompressionType} and {@link TSEncoding} of a timeseries. This method will skip
   * the measurement id, and data type. This method will change the position of this reader.
   *
   * @param timeseriesMetadata timeseries' metadata
   * @return a pair of {@link CompressionType} and {@link TSEncoding} of given timeseries.
   * @throws IOException
   */
  public Pair<CompressionType, TSEncoding> readTimeseriesCompressionTypeAndEncoding(
      TimeseriesMetadata timeseriesMetadata) throws IOException {

    String measurementId = timeseriesMetadata.getMeasurementId();
    int measurementIdLength = measurementId.getBytes(TSFileConfig.STRING_CHARSET).length;
    position(
        timeseriesMetadata.getChunkMetadataList().get(0).getOffsetOfChunkHeader()
            + Byte.BYTES // chunkType
            + ReadWriteForEncodingUtils.varIntSize(measurementIdLength) // measurementID length
            + measurementIdLength); // measurementID
    return ChunkHeader.deserializeCompressionTypeAndEncoding(tsFileInput.wrapAsInputStream());
  }

  /** Get measurement schema by chunkMetadatas. */
  public MeasurementSchema getMeasurementSchema(List<IChunkMetadata> chunkMetadataList)
      throws IOException {
    if (chunkMetadataList.isEmpty()) {
      return null;
    }
    IChunkMetadata lastChunkMetadata = chunkMetadataList.get(chunkMetadataList.size() - 1);
    int chunkHeadSize = ChunkHeader.getSerializedSize(lastChunkMetadata.getMeasurementUid());
    ChunkHeader header = readChunkHeader(lastChunkMetadata.getOffsetOfChunkHeader(), chunkHeadSize);
    return new MeasurementSchema(
        lastChunkMetadata.getMeasurementUid(),
        header.getDataType(),
        header.getEncodingType(),
        header.getCompressionType());
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type, boolean hasStatistic) throws IOException {
    try {
      return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type, hasStatistic);
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading page header of {}", t.getMessage(), file);
      throw t;
    }
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

  public ByteBuffer readCompressedPage(PageHeader header) throws IOException {
    return readData(-1, header.getCompressedSize());
  }

  public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
    ByteBuffer buffer = readData(-1, header.getCompressedSize());
    if (header.getUncompressedSize() == 0 || type == CompressionType.UNCOMPRESSED) {
      return buffer;
    } // FIXME if the buffer is not array-implemented.
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
    unCompressor.uncompress(
        buffer.array(), buffer.position(), buffer.remaining(), uncompressedBuffer.array(), 0);
    return uncompressedBuffer;
  }

  /**
   * read one byte from the input. <br>
   * this method is not thread safe
   */
  public byte readMarker() throws IOException {
    markerBuffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, markerBuffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    markerBuffer.flip();
    return markerBuffer.get();
  }

  @Override
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

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position. <br>
   * if position = -1, the tsFileInput's position will be changed to the current position + real
   * data size that been read. Other wise, the tsFileInput's position is not changed.
   *
   * @param position the start position of data in the tsFileInput, or the current position if
   *     position = -1
   * @param totalSize the size of data that want to read
   * @return data that been read.
   */
  protected ByteBuffer readData(long position, int totalSize) throws IOException {
    int allocateSize = Math.min(MAX_READ_BUFFER_SIZE, totalSize);
    int allocateNum = (int) Math.ceil((double) totalSize / allocateSize);
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);
    int bufferLimit = 0;
    for (int i = 0; i < allocateNum; i++) {
      if (i == allocateNum - 1) {
        allocateSize = totalSize - allocateSize * (allocateNum - 1);
      }
      bufferLimit += allocateSize;
      buffer.limit(bufferLimit);
      if (position < 0) {
        if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != allocateSize) {
          throw new IOException("reach the end of the data");
        }
      } else {
        long actualReadSize =
            ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, allocateSize);
        if (actualReadSize != allocateSize) {
          throw new IOException(
              String.format(
                  "reach the end of the data. Size of data that want to read: %s,"
                      + "actual read size: %s, position: %s",
                  allocateSize, actualReadSize, position));
        }
        position += allocateSize;
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
   *     = -1
   * @param end the end position of data that want to read
   * @return data that been read.
   */
  protected ByteBuffer readData(long start, long end) throws IOException {
    try {
      return readData(start, (int) (end - start));
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading data of {}", t.getMessage(), file);
      throw t;
    }
  }

  /** notice, the target bytebuffer are not flipped. */
  public int readRaw(long position, int length, ByteBuffer target) throws IOException {
    return ReadWriteIOUtils.readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   *
   * @param newSchema the schema on each time series in the file
   * @param chunkGroupMetadataList ChunkGroupMetadata List
   * @param fastFinish if true and the file is complete, then newSchema and chunkGroupMetadataList
   *     parameter will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   *     be truncated.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public long selfCheck(
      Map<Path, IMeasurementSchema> newSchema,
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      boolean fastFinish)
      throws IOException {
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
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();

    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
    if (fileSize < headerLength) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic())
        || (TSFileConfig.VERSION_NUMBER != readVersionNumber())) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    tsFileInput.position(headerLength);
    boolean isComplete = isComplete();
    if (fileSize == headerLength) {
      return headerLength;
    } else if (isComplete) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }
    // if not a complete file, we will recover it...
    long truncatedSize = headerLength;
    byte marker;
    List<long[]> timeBatch = new ArrayList<>();
    String lastDeviceId = null;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            fileOffsetOfChunk = this.position() - 1;
            // if there is something wrong with a chunk, we will drop the whole ChunkGroup
            // as different chunks may be created by the same insertions(sqls), and partial
            // insertion is not tolerable
            ChunkHeader chunkHeader = this.readChunkHeader(marker);
            measurementID = chunkHeader.getMeasurementID();
            IMeasurementSchema measurementSchema =
                new MeasurementSchema(
                    measurementID,
                    chunkHeader.getDataType(),
                    chunkHeader.getEncodingType(),
                    chunkHeader.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            dataType = chunkHeader.getDataType();
            if (chunkHeader.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }
            Statistics<? extends Serializable> chunkStatistics =
                Statistics.getStatsByType(dataType);
            int dataSize = chunkHeader.getDataSize();

            if (dataSize > 0) {
              if (((byte) (chunkHeader.getChunkType() & 0x3F))
                  == MetaMarker
                      .CHUNK_HEADER) { // more than one page, we could use page statistics to
                // generate chunk statistic
                while (dataSize > 0) {
                  // a new Page
                  PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), true);
                  if (pageHeader.getUncompressedSize() != 0) {
                    // not empty page
                    chunkStatistics.mergeStatistics(pageHeader.getStatistics());
                  }
                  this.skipPageData(pageHeader);
                  dataSize -= pageHeader.getSerializedPageSize();
                  chunkHeader.increasePageNums(1);
                }
              } else { // only one page without statistic, we need to iterate each point to generate
                // chunk statistic
                PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), false);
                Decoder valueDecoder =
                    Decoder.getDecoderByType(
                        chunkHeader.getEncodingType(), chunkHeader.getDataType());
                ByteBuffer pageData = readPage(pageHeader, chunkHeader.getCompressionType());
                Decoder timeDecoder =
                    Decoder.getDecoderByType(
                        TSEncoding.valueOf(
                            TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                        TSDataType.INT64);

                if ((chunkHeader.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                    == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk with only one page

                  TimePageReader timePageReader =
                      new TimePageReader(pageHeader, pageData, timeDecoder);
                  long[] currentTimeBatch = timePageReader.getNextTimeBatch();
                  timeBatch.add(currentTimeBatch);
                  for (long currentTime : currentTimeBatch) {
                    chunkStatistics.update(currentTime);
                  }
                } else if ((chunkHeader.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                    == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk with only one page

                  ValuePageReader valuePageReader =
                      new ValuePageReader(
                          pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
                  TsPrimitiveType[] valueBatch = valuePageReader.nextValueBatch(timeBatch.get(0));

                  if (valueBatch != null && valueBatch.length != 0) {
                    for (int i = 0; i < valueBatch.length; i++) {
                      TsPrimitiveType value = valueBatch[i];
                      if (value == null) {
                        continue;
                      }
                      long timeStamp = timeBatch.get(0)[i];
                      switch (dataType) {
                        case INT32:
                          chunkStatistics.update(timeStamp, value.getInt());
                          break;
                        case INT64:
                          chunkStatistics.update(timeStamp, value.getLong());
                          break;
                        case FLOAT:
                          chunkStatistics.update(timeStamp, value.getFloat());
                          break;
                        case DOUBLE:
                          chunkStatistics.update(timeStamp, value.getDouble());
                          break;
                        case BOOLEAN:
                          chunkStatistics.update(timeStamp, value.getBoolean());
                          break;
                        case TEXT:
                          chunkStatistics.update(timeStamp, value.getBinary());
                          break;
                        default:
                          throw new IOException("Unexpected type " + dataType);
                      }
                    }
                  }

                } else { // NonAligned Chunk with only one page
                  PageReader reader =
                      new PageReader(
                          pageHeader,
                          pageData,
                          chunkHeader.getDataType(),
                          valueDecoder,
                          timeDecoder,
                          null);
                  BatchData batchData = reader.getAllSatisfiedPageData();
                  while (batchData.hasCurrent()) {
                    switch (dataType) {
                      case INT32:
                        chunkStatistics.update(batchData.currentTime(), batchData.getInt());
                        break;
                      case INT64:
                        chunkStatistics.update(batchData.currentTime(), batchData.getLong());
                        break;
                      case FLOAT:
                        chunkStatistics.update(batchData.currentTime(), batchData.getFloat());
                        break;
                      case DOUBLE:
                        chunkStatistics.update(batchData.currentTime(), batchData.getDouble());
                        break;
                      case BOOLEAN:
                        chunkStatistics.update(batchData.currentTime(), batchData.getBoolean());
                        break;
                      case TEXT:
                        chunkStatistics.update(batchData.currentTime(), batchData.getBinary());
                        break;
                      default:
                        throw new IOException("Unexpected type " + dataType);
                    }
                    batchData.next();
                  }
                }
                chunkHeader.increasePageNums(1);
              }
            }
            currentChunk =
                new ChunkMetadata(measurementID, dataType, fileOffsetOfChunk, chunkStatistics);
            chunkMetadataList.add(currentChunk);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // if there is something wrong with the ChunkGroup Header, we will drop this ChunkGroup
            // because we can not guarantee the correctness of the deviceId.
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId(), true), tsSchema);
                }
              }
              measurementSchemaList = new ArrayList<>();
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
            }
            // this is a chunk group
            chunkMetadataList = new ArrayList<>();
            ChunkGroupHeader chunkGroupHeader = this.readChunkGroupHeader();
            lastDeviceId = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId(), true), tsSchema);
                }
              }
              measurementSchemaList = new ArrayList<>();
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
              lastDeviceId = null;
            }
            readPlanIndex();
            truncatedSize = this.position();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
      // now we read the tail of the data section, so we are sure that the last
      // ChunkGroupFooter is complete.
      if (lastDeviceId != null) {
        // schema of last chunk group
        if (newSchema != null) {
          for (IMeasurementSchema tsSchema : measurementSchemaList) {
            newSchema.putIfAbsent(
                new Path(lastDeviceId, tsSchema.getMeasurementId(), true), tsSchema);
          }
        }
        // last chunk group Metadata
        chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
      }
      if (isComplete) {
        truncatedSize = TsFileCheckStatus.COMPLETE_FILE;
      } else {
        truncatedSize = this.position() - 1;
      }
    } catch (Exception e) {
      logger.warn(
          "TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
          file,
          this.position(),
          e.getMessage());
    }
    // Despite the completeness of the data section, we will discard current FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedSize;
  }

  /**
   * Self Check the file and return whether the file is safe.
   *
   * @param filename the path of file
   * @param fastFinish if true, the method will only check the format of head (Magic String TsFile,
   *     Version Number) and tail (Magic String TsFile) of TsFile.
   * @return the status of TsFile
   */
  public long selfCheckWithInfo(
      String filename,
      boolean fastFinish,
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap)
      throws IOException, TsFileStatisticsMistakesException {
    String message = " exists statistics mistakes at position ";
    File checkFile = FSFactoryProducer.getFSFactory().getFile(filename);
    if (!checkFile.exists()) {
      return TsFileCheckStatus.FILE_NOT_FOUND;
    }
    long fileSize = checkFile.length();
    logger.info("file length: " + fileSize);

    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
    if (fileSize < headerLength) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    try {
      if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic())
          || (TSFileConfig.VERSION_NUMBER != readVersionNumber())) {
        return TsFileCheckStatus.INCOMPATIBLE_FILE;
      }
      tsFileInput.position(headerLength);
      if (isComplete()) {
        loadMetadataSize();
        if (fastFinish) {
          return TsFileCheckStatus.COMPLETE_FILE;
        }
      }
    } catch (IOException e) {
      logger.error("Error occurred while fast checking TsFile.");
      throw e;
    }
    for (Map.Entry<Long, Pair<Path, TimeseriesMetadata>> entry : timeseriesMetadataMap.entrySet()) {
      TimeseriesMetadata timeseriesMetadata = entry.getValue().right;
      TSDataType dataType = timeseriesMetadata.getTSDataType();
      Statistics<? extends Serializable> timeseriesMetadataSta = timeseriesMetadata.getStatistics();
      Statistics<? extends Serializable> chunkMetadatasSta = Statistics.getStatsByType(dataType);
      for (IChunkMetadata chunkMetadata : getChunkMetadataList(entry.getValue().left)) {
        long tscheckStatus = TsFileCheckStatus.COMPLETE_FILE;
        try {
          tscheckStatus = checkChunkAndPagesStatistics(chunkMetadata);
        } catch (IOException e) {
          logger.error("Error occurred while checking the statistics of chunk and its pages");
          throw e;
        }
        if (tscheckStatus == TsFileCheckStatus.FILE_EXISTS_MISTAKES) {
          throw new TsFileStatisticsMistakesException(
              "Chunk" + message + chunkMetadata.getOffsetOfChunkHeader());
        }
        chunkMetadatasSta.mergeStatistics(chunkMetadata.getStatistics());
      }
      if (!timeseriesMetadataSta.equals(chunkMetadatasSta)) {
        long timeseriesMetadataPos = entry.getKey();
        throw new TsFileStatisticsMistakesException(
            "TimeseriesMetadata" + message + timeseriesMetadataPos);
      }
    }
    return TsFileCheckStatus.COMPLETE_FILE;
  }

  public long checkChunkAndPagesStatistics(IChunkMetadata chunkMetadata) throws IOException {
    long offsetOfChunkHeader = chunkMetadata.getOffsetOfChunkHeader();
    tsFileInput.position(offsetOfChunkHeader);
    byte marker = this.readMarker();
    ChunkHeader chunkHeader = this.readChunkHeader(marker);
    TSDataType dataType = chunkHeader.getDataType();
    Statistics<? extends Serializable> chunkStatistics = Statistics.getStatsByType(dataType);
    int dataSize = chunkHeader.getDataSize();
    if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.CHUNK_HEADER) {
      while (dataSize > 0) {
        // a new Page
        PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), true);
        chunkStatistics.mergeStatistics(pageHeader.getStatistics());
        this.skipPageData(pageHeader);
        dataSize -= pageHeader.getSerializedPageSize();
        chunkHeader.increasePageNums(1);
      }
    } else {
      // only one page without statistic, we need to iterate each point to generate
      // statistic
      PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), false);
      Decoder valueDecoder =
          Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
      ByteBuffer pageData = readPage(pageHeader, chunkHeader.getCompressionType());
      Decoder timeDecoder =
          Decoder.getDecoderByType(
              TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
              TSDataType.INT64);
      PageReader reader =
          new PageReader(
              pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
      BatchData batchData = reader.getAllSatisfiedPageData();
      while (batchData.hasCurrent()) {
        switch (dataType) {
          case INT32:
            chunkStatistics.update(batchData.currentTime(), batchData.getInt());
            break;
          case INT64:
            chunkStatistics.update(batchData.currentTime(), batchData.getLong());
            break;
          case FLOAT:
            chunkStatistics.update(batchData.currentTime(), batchData.getFloat());
            break;
          case DOUBLE:
            chunkStatistics.update(batchData.currentTime(), batchData.getDouble());
            break;
          case BOOLEAN:
            chunkStatistics.update(batchData.currentTime(), batchData.getBoolean());
            break;
          case TEXT:
            chunkStatistics.update(batchData.currentTime(), batchData.getBinary());
            break;
          default:
            throw new IOException("Unexpected type " + dataType);
        }
        batchData.next();
      }
      chunkHeader.increasePageNums(1);
    }
    if (chunkMetadata.getStatistics().equals(chunkStatistics)) {
      return TsFileCheckStatus.COMPLETE_FILE;
    }
    return TsFileCheckStatus.FILE_EXISTS_MISTAKES;
  }

  /**
   * get ChunkMetaDatas of given path, and throw exception if path not exists
   *
   * @param path timeseries path
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> getChunkMetadataList(Path path, boolean ignoreNotExists)
      throws IOException {
    TimeseriesMetadata timeseriesMetaData = readTimeseriesMetadata(path, ignoreNotExists);
    if (timeseriesMetaData == null) {
      return Collections.emptyList();
    }
    List<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetaData);
    chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));
    return chunkMetadataList;
  }

  // This method is only used for TsFile
  public List<IChunkMetadata> getIChunkMetadataList(Path path) throws IOException {
    ITimeSeriesMetadata timeseriesMetaData = readITimeseriesMetadata(path, true);
    if (timeseriesMetaData == null) {
      return Collections.emptyList();
    }
    List<IChunkMetadata> chunkMetadataList = readIChunkMetaDataList(timeseriesMetaData);
    chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));
    return chunkMetadataList;
  }

  public List<ChunkMetadata> getChunkMetadataList(Path path) throws IOException {
    return getChunkMetadataList(path, false);
  }

  /**
   * Get AlignedChunkMetadata of sensors under one device
   *
   * @param device device name
   */
  public List<AlignedChunkMetadata> getAlignedChunkMetadata(String device) throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      throw new IOException("Device {" + device + "} is not in tsFileMetaData");
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode;
    TimeseriesMetadata firstTimeseriesMetadata;
    try {
      // next layer MeasurementNode of the specific DeviceNode
      metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
    } catch (Exception e) {
      logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
      throw e;
    }
    firstTimeseriesMetadata = tryToGetFirstTimeseriesMetadata(metadataIndexNode);
    if (firstTimeseriesMetadata == null) {
      throw new IOException("Timeseries of device {" + device + "} are not aligned");
    }

    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();

    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = metadataIndexNode.getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      buffer = readData(metadataIndexEntry.getOffset(), endOffset);
      if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
        }
        timeseriesMetadataMap
            .computeIfAbsent(device, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
      } else {
        generateMetadataIndex(
            metadataIndexEntry,
            buffer,
            device,
            metadataIndexNode.getNodeType(),
            timeseriesMetadataMap,
            true);
      }
    }

    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      TimeseriesMetadata timeseriesMetadata = timeseriesMetadataList.get(0);
      List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();

      for (int i = 1; i < timeseriesMetadataList.size(); i++) {
        valueTimeseriesMetadataList.add(timeseriesMetadataList.get(i));
      }

      AlignedTimeSeriesMetadata alignedTimeSeriesMetadata =
          new AlignedTimeSeriesMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
      List<AlignedChunkMetadata> chunkMetadataList = new ArrayList<>();
      for (IChunkMetadata chunkMetadata : readIChunkMetaDataList(alignedTimeSeriesMetadata)) {
        chunkMetadataList.add((AlignedChunkMetadata) chunkMetadata);
      }
      // only one timeseriesMetadataList in one device
      return chunkMetadataList;
    }

    throw new IOException(
        String.format(
            "Error when reading timeseriesMetadata of device %s in file %s", device, file));
  }

  /**
   * get ChunkMetaDatas in given TimeseriesMetaData
   *
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> readChunkMetaDataList(TimeseriesMetadata timeseriesMetaData)
      throws IOException {
    return timeseriesMetaData.getChunkMetadataList().stream()
        .map(chunkMetadata -> (ChunkMetadata) chunkMetadata)
        .collect(Collectors.toList());
  }

  // This method is only used for TsFile
  public List<IChunkMetadata> readIChunkMetaDataList(ITimeSeriesMetadata timeseriesMetaData) {
    if (timeseriesMetaData instanceof AlignedTimeSeriesMetadata) {
      return new ArrayList<>(
          ((AlignedTimeSeriesMetadata) timeseriesMetaData).getChunkMetadataList());
    } else {
      return new ArrayList<>(((TimeseriesMetadata) timeseriesMetaData).getChunkMetadataList());
    }
  }

  /**
   * get all measurements in this file
   *
   * @return measurement -> datatype
   */
  public Map<String, TSDataType> getAllMeasurements() throws IOException {
    Map<String, TSDataType> result = new HashMap<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      }
    }
    return result;
  }

  public Map<String, List<String>> getDeviceMeasurementsMap() throws IOException {
    Map<String, List<String>> result = new HashMap<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result
            .computeIfAbsent(device, d -> new ArrayList<>())
            .add(timeseriesMetadata.getMeasurementId());
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
    for (String device : getAllDevices()) {
      Map<String, List<ChunkMetadata>> seriesMetadataMap = readChunkMetadataInDevice(device);
      if (hasDataInPartition(seriesMetadataMap, start, end)) {
        res.add(device);
      }
    }
    return res;
  }

  /**
   * get metadata index node
   *
   * @param startOffset start read offset
   * @param endOffset end read offset
   * @return MetadataIndexNode
   */
  public MetadataIndexNode getMetadataIndexNode(long startOffset, long endOffset)
      throws IOException {
    return MetadataIndexNode.deserializeFrom(readData(startOffset, endOffset));
  }

  /**
   * Check if the device has at least one Chunk in this partition
   *
   * @param seriesMetadataMap chunkMetaDataList of each measurement
   * @param start the start position of the space partition
   * @param end the end position of the space partition
   */
  private boolean hasDataInPartition(
      Map<String, List<ChunkMetadata>> seriesMetadataMap, long start, long end) {
    for (List<ChunkMetadata> chunkMetadataList : seriesMetadataMap.values()) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        LocateStatus location =
            MetadataQuerierByFileImpl.checkLocateStatus(chunkMetadata, start, end);
        if (location == LocateStatus.in) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The location of a chunkGroupMetaData with respect to a space partition constraint.
   *
   * <p>in - the middle point of the chunkGroupMetaData is located in the current space partition.
   * before - the middle point of the chunkGroupMetaData is located before the current space
   * partition. after - the middle point of the chunkGroupMetaData is located after the current
   * space partition.
   */
  public enum LocateStatus {
    in,
    before,
    after
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  /**
   * @return An iterator of linked hashmaps ( measurement -> chunk metadata list ). When traversing
   *     the linked hashmap, you will get chunk metadata lists according to the lexicographic order
   *     of the measurements. The first measurement of the linked hashmap of each iteration is
   *     always larger than the last measurement of the linked hashmap of the previous iteration in
   *     lexicographic order.
   */
  public Iterator<Map<String, List<ChunkMetadata>>> getMeasurementChunkMetadataListMapIterator(
      String device) throws IOException {
    readFileMetadata();

    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);

    if (metadataIndexPair == null) {
      return new Iterator<Map<String, List<ChunkMetadata>>>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public LinkedHashMap<String, List<ChunkMetadata>> next() {
          throw new NoSuchElementException();
        }
      };
    }

    Queue<Pair<Long, Long>> queue = new LinkedList<>();
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    collectEachLeafMeasurementNodeOffsetRange(buffer, queue);

    return new Iterator<Map<String, List<ChunkMetadata>>>() {

      @Override
      public boolean hasNext() {
        return !queue.isEmpty();
      }

      @Override
      public LinkedHashMap<String, List<ChunkMetadata>> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Pair<Long, Long> startEndPair = queue.remove();
        LinkedHashMap<String, List<ChunkMetadata>> measurementChunkMetadataList =
            new LinkedHashMap<>();
        try {
          List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
          ByteBuffer nextBuffer = readData(startEndPair.left, startEndPair.right);
          while (nextBuffer.hasRemaining()) {
            timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(nextBuffer, true));
          }
          for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
            List<ChunkMetadata> list =
                measurementChunkMetadataList.computeIfAbsent(
                    timeseriesMetadata.getMeasurementId(), m -> new ArrayList<>());
            for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
              list.add((ChunkMetadata) chunkMetadata);
            }
          }
          return measurementChunkMetadataList;
        } catch (IOException e) {
          throw new TsFileRuntimeException(
              "Error occurred while reading a time series metadata block.");
        }
      }
    };
  }

  private void collectEachLeafMeasurementNodeOffsetRange(
      ByteBuffer buffer, Queue<Pair<Long, Long>> queue) throws IOException {
    try {
      final MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      final MetadataIndexNodeType metadataIndexNodeType = metadataIndexNode.getNodeType();
      final int metadataIndexListSize = metadataIndexNode.getChildren().size();
      for (int i = 0; i < metadataIndexListSize; ++i) {
        long startOffset = metadataIndexNode.getChildren().get(i).getOffset();
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        if (metadataIndexNodeType.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          queue.add(new Pair<>(startOffset, endOffset));
          continue;
        }
        collectEachLeafMeasurementNodeOffsetRange(readData(startOffset, endOffset), queue);
      }
    } catch (Exception e) {
      logger.error(
          "Error occurred while collecting offset ranges of measurement nodes of file {}", file);
      throw e;
    }
  }

  @Override
  public int hashCode() {
    return file.hashCode();
  }
}
