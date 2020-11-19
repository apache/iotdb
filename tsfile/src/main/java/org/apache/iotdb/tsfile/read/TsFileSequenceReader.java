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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.BloomFilter;
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
  protected TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  private int totalChunkNum;
  private TsFileMetadata tsFileMetaData;
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
   * @param file             -given file name
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
      fileMetadataPos = tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length
          - Integer.BYTES - fileMetadataSize;
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
    ByteBuffer magicStringBytes = ByteBuffer
        .allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
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
        .equals(TSFileConfig.VERSION_NUMBER_V1));
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readHeadMagic() throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer
        .allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, 0);
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
    List<TimeseriesMetadata> timeseriesMetadataList = getDeviceTimeseriesMetadata(device);
    Map<String, TimeseriesMetadata> deviceMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      deviceMetadata.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }
    return deviceMetadata;
  }

  public TimeseriesMetadata readTimeseriesMetadata(Path path) throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getMetadataAndEndOffset(
        deviceMetadataIndexNode, path.getDevice(), MetadataIndexNodeType.INTERNAL_DEVICE, true);
    if (metadataIndexPair == null) {
      return null;
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      metadataIndexPair = getMetadataAndEndOffset(metadataIndexNode,
          path.getMeasurement(), MetadataIndexNodeType.INTERNAL_MEASUREMENT, false);
    }
    if (metadataIndexPair == null) {
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer));
    }
    // return null if path does not exist in the TsFile
    int searchResult = binarySearchInTimeseriesMetadataList(timeseriesMetadataList,
        path.getMeasurement());
    return searchResult >= 0 ? timeseriesMetadataList.get(searchResult) : null;
  }

  public List<TimeseriesMetadata> readTimeseriesMetadata(String device, Set<String> measurements)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getMetadataAndEndOffset(
        deviceMetadataIndexNode, device, MetadataIndexNodeType.INTERNAL_DEVICE, false);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> resultTimeseriesMetadataList = new ArrayList<>();
    List<String> measurementList = new ArrayList<>(measurements);
    Set<String> measurementsHadFound = new HashSet<>();
    for (int i = 0; i < measurementList.size(); i++) {
      if (measurementsHadFound.contains(measurementList.get(i))) {
        continue;
      }
      ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
      Pair<MetadataIndexEntry, Long> measurementMetadataIndexPair = metadataIndexPair;
      List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
      MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
      if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        measurementMetadataIndexPair = getMetadataAndEndOffset(metadataIndexNode,
            measurementList.get(i), MetadataIndexNodeType.INTERNAL_MEASUREMENT, false);
      }
      if (measurementMetadataIndexPair == null) {
        return Collections.emptyList();
      }
      buffer = readData(measurementMetadataIndexPair.left.getOffset(),
          measurementMetadataIndexPair.right);
      while (buffer.hasRemaining()) {
        timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer));
      }
      for (int j = i; j < measurementList.size(); j++) {
        String current = measurementList.get(j);
        if (!measurementsHadFound.contains(current)) {
          int searchResult = binarySearchInTimeseriesMetadataList(timeseriesMetadataList, current);
          if (searchResult >= 0) {
            resultTimeseriesMetadataList.add(timeseriesMetadataList.get(searchResult));
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

  /**
   * Traverse and read TimeseriesMetadata of specific measurements in one device. This method need
   * to deserialize all TimeseriesMetadata and then judge, in order to avoid frequent I/O when the
   * number of queried measurements is too large. Attention: This method is not used currently
   *
   * @param timeseriesMetadataList TimeseriesMetadata list, to store the result
   * @param type                   MetadataIndexNode type
   * @param metadataIndexPair      <MetadataIndexEntry, offset> pair
   * @param measurements           measurements to be queried
   * @throws IOException io error
   */
  private void traverseAndReadTimeseriesMetadataInOneDevice(
      List<TimeseriesMetadata> timeseriesMetadataList, MetadataIndexNodeType type,
      Pair<MetadataIndexEntry, Long> metadataIndexPair, Set<String> measurements)
      throws IOException {
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    switch (type) {
      case LEAF_DEVICE:
      case INTERNAL_MEASUREMENT:
        MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          traverseAndReadTimeseriesMetadataInOneDevice(timeseriesMetadataList,
              metadataIndexNode.getNodeType(),
              new Pair<>(metadataIndexNode.getChildren().get(i), endOffset), measurements);
        }
        break;
      case LEAF_MEASUREMENT:
        while (buffer.hasRemaining()) {
          TimeseriesMetadata timeseriesMetadata = TimeseriesMetadata.deserializeFrom(buffer);
          if (measurements.contains(timeseriesMetadata.getMeasurementId())) {
            timeseriesMetadataList.add(timeseriesMetadata);
          }
        }
        break;
      default:
        throw new IOException("Failed to traverse and read TimeseriesMetadata in device: " +
            metadataIndexPair.left.getName() + ". Wrong MetadataIndexEntry type.");
    }
  }

  private int binarySearchInTimeseriesMetadataList(List<TimeseriesMetadata> timeseriesMetadataList,
      String key) {
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
    return -1;  // key not found
  }

  public List<String> getAllDevices() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    return getAllDevices(tsFileMetaData.getMetadataIndex());
  }

  private List<String> getAllDevices(MetadataIndexNode metadataIndexNode) throws IOException {
    List<String> deviceList = new ArrayList<>();
    int metadataIndexListSize = metadataIndexNode.getChildren().size();
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT)) {
      for (MetadataIndexEntry index : metadataIndexNode.getChildren()) {
        deviceList.add(index.getName());
      }
    } else {
      for (int i = 0; i < metadataIndexListSize; i++) {
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        ByteBuffer buffer = readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
        MetadataIndexNode node = MetadataIndexNode.deserializeFrom(buffer);
        if (node.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
          // if node in next level is LEAF_DEVICE, put all devices in node entry into the set
          deviceList.addAll(node.getChildren().stream().map(MetadataIndexEntry::getName).collect(
              Collectors.toList()));
        } else {
          // keep traversing
          deviceList.addAll(getAllDevices(node));
        }
      }
    }
    return deviceList;
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
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (String measurementId : timeseriesMetadataMap.keySet()) {
        paths.add(new Path(device, measurementId));
      }
    }
    return paths;
  }

  /**
   * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
   *
   * @param metadataIndex         MetadataIndexEntry
   * @param buffer                byte buffer
   * @param deviceId              String
   * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
   */
  private void generateMetadataIndex(MetadataIndexEntry metadataIndex, ByteBuffer buffer,
      String deviceId, MetadataIndexNodeType type,
      Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap) throws IOException {
    switch (type) {
      case INTERNAL_DEVICE:
      case LEAF_DEVICE:
      case INTERNAL_MEASUREMENT:
        deviceId = metadataIndex.getName();
        MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          ByteBuffer nextBuffer = readData(metadataIndexNode.getChildren().get(i).getOffset(),
              endOffset);
          generateMetadataIndex(metadataIndexNode.getChildren().get(i), nextBuffer, deviceId,
              metadataIndexNode.getNodeType(), timeseriesMetadataMap);
        }
        break;
      case LEAF_MEASUREMENT:
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer));
        }
        timeseriesMetadataMap.computeIfAbsent(deviceId, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
        break;
    }
  }

  public Map<String, List<TimeseriesMetadata>> getAllTimeseriesMetadata() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new HashMap<>();
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = tsFileMetaData.getMetadataIndex().getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
      generateMetadataIndex(metadataIndexEntry, buffer, null,
          metadataIndexNode.getNodeType(), timeseriesMetadataMap);
    }
    return timeseriesMetadataMap;
  }

  private List<TimeseriesMetadata> getDeviceTimeseriesMetadata(String device) throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getMetadataAndEndOffset(
        metadataIndexNode, device, MetadataIndexNodeType.INTERNAL_DEVICE, false);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(metadataIndexPair.left, buffer, device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT, timeseriesMetadataMap);
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
   * @param name          target device / measurement name
   * @param type          target MetadataIndexNodeType, either INTERNAL_DEVICE or
   *                      INTERNAL_MEASUREMENT. When searching for a device node,  return when it is
   *                      not INTERNAL_DEVICE. Likewise, when searching for a measurement node,
   *                      return when it is not INTERNAL_MEASUREMENT. This works for the situation
   *                      when the index tree does NOT have the device level and ONLY has the
   *                      measurement level.
   * @param exactSearch   if is in exact search mode, return null when there is no entry with name;
   *                      or else return the nearest MetadataIndexEntry before it (for deeper
   *                      search)
   * @return target MetadataIndexEntry, endOffset pair
   */
  private Pair<MetadataIndexEntry, Long> getMetadataAndEndOffset(MetadataIndexNode metadataIndex,
      String name, MetadataIndexNodeType type, boolean exactSearch) throws IOException {
    Pair<MetadataIndexEntry, Long> childIndexEntry = metadataIndex
        .getChildIndexEntry(name, exactSearch);
    if (childIndexEntry == null) {
      return null;
    }
    if (!metadataIndex.getNodeType().equals(type)) {
      return childIndexEntry;
    }
    ByteBuffer buffer = readData(childIndexEntry.left.getOffset(), childIndexEntry.right);
    return getMetadataAndEndOffset(MetadataIndexNode.deserializeFrom(buffer), name, type,
        exactSearch);
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
   * @param position        the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead      true if the offset does not contains the marker , otherwise false
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
    return new Chunk(header, buffer, metaData.getDeleteIntervalList());
  }

  /**
   * read all Chunks of given device.
   * <p>
   * note that this method loads all the chunks into memory, so it needs to be invoked carefully.
   *
   * @param device name
   * @return measurement -> chunks list
   */
  public Map<String, List<Chunk>> readChunksInDevice(String device) throws IOException {
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    Map<String, List<ChunkMetadata>> chunkMetadataInDevice = readChunkMetadataInDevice(device);
    for (List<ChunkMetadata> chunkMetadataListInDevice : chunkMetadataInDevice.values()) {
      chunkMetadataList.addAll(chunkMetadataListInDevice);
    }

    Map<String, List<Chunk>> chunksInDevice = new HashMap<>();
    chunkMetadataList.sort(Comparator.comparing(ChunkMetadata::getOffsetOfChunkHeader));
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {
      Chunk chunk = readMemChunk(chunkMetadata);
      String measurement = chunk.getHeader().getMeasurementID();
      if (!chunksInDevice.containsKey(measurement)) {
        chunksInDevice.put(measurement, new ArrayList<>());
      }
      chunksInDevice.get(measurement).add(chunk);
    }
    return chunksInDevice;
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
    if (position < 0) {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != size) {
        throw new IOException("reach the end of the data");
      }
    } else {
      long actualReadSize = ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, size);
      if (actualReadSize != size) {
        throw new IOException(
            String.format("reach the end of the data. Size of data that want to read: %s,"
                + "actual read size: %s, posiotion: %s", size, actualReadSize, position));
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
   *              = -1
   * @param end   the end position of data that want to read
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
   * @param newSchema              the schema on each time series in the file
   * @param chunkGroupMetadataList ChunkGroupMetadata List
   * @param versionInfo            version pair List
   * @param fastFinish             if true and the file is complete, then newSchema and
   *                               chunkGroupMetadataList parameter will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   * be truncated.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
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
    if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic()) || !TSFileConfig.VERSION_NUMBER
        .equals(readVersionNumber())) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    tsFileInput.position(headerLength);
    if (fileSize == headerLength) {
      return headerLength;
    } else if (isComplete()) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }
    boolean newChunkGroup = true;
    // not a complete file, we will recover it...
    long truncatedSize = headerLength;
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
            truncatedSize = this.position();

            totalChunkNum += chunkCnt;
            chunkCnt = 0;
            measurementSchemaList = new ArrayList<>();
            break;
          case MetaMarker.VERSION:
            long version = readVersion();
            versionInfo.add(new Pair<>(position(), version));
            truncatedSize = this.position();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
      // now we read the tail of the data section, so we are sure that the last
      // ChunkGroupFooter is complete.
      truncatedSize = this.position() - 1;
    } catch (Exception e) {
      logger.info("TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
          file, this.position(), e.getMessage());
    }
    // Despite the completeness of the data section, we will discard current FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedSize;
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
    TimeseriesMetadata timeseriesMetaData = readTimeseriesMetadata(path);
    if (timeseriesMetaData == null) {
      return Collections.emptyList();
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
    readFileMetadata();
    List<Pair<Long, Long>> versionInfo = tsFileMetaData.getVersionInfo();
    ArrayList<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    long startOffsetOfChunkMetadataList = timeseriesMetaData.getOffsetOfChunkMetaDataList();
    int dataSizeOfChunkMetadataList = timeseriesMetaData.getDataSizeOfChunkMetaDataList();

    ByteBuffer buffer = readData(startOffsetOfChunkMetadataList, dataSizeOfChunkMetadataList);
    while (buffer.hasRemaining()) {
      chunkMetadataList.add(ChunkMetadata.deserializeFrom(buffer));
    }

    VersionUtils.applyVersion(chunkMetadataList, versionInfo);

    // minimize the storage of an ArrayList instance.
    chunkMetadataList.trimToSize();
    return chunkMetadataList;
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

  /**
   * get device names which has valid chunks in [start, end)
   *
   * @param start start of the partition
   * @param end   end of the partition
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
   * Check if the device has at least one Chunk in this partition
   *
   * @param seriesMetadataMap chunkMetaDataList of each measurement
   * @param start             the start position of the space partition
   * @param end               the end position of the space partition
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
