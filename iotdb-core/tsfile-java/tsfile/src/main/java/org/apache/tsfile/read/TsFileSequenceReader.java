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

package org.apache.tsfile.read;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.compatibility.CompatibilityUtils;
import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotCompatibleTsFileException;
import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.exception.TsFileStatisticsMistakesException;
import org.apache.tsfile.exception.read.FileVersionTooOldException;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableDeviceMetadata;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
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

  @SuppressWarnings("squid:S3077")
  protected volatile TsFileMetadata tsFileMetaData;

  // device -> measurement -> TimeseriesMetadata
  private Map<IDeviceID, Map<String, TimeseriesMetadata>> cachedDeviceMetadata =
      new ConcurrentHashMap<>();
  private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private boolean cacheDeviceMetadata;
  private long minPlanIndex = Long.MAX_VALUE;
  private long maxPlanIndex = Long.MIN_VALUE;

  private byte fileVersion;

  private DeserializeConfig deserializeConfig = new DeserializeConfig();

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
    this(file, null);
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param file the data file
   * @param ioSizeRecorder can be null
   * @throws IOException If some I/O error occurs
   */
  public TsFileSequenceReader(String file, LongConsumer ioSizeRecorder) throws IOException {
    this(file, true, ioSizeRecorder);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
    this(file, loadMetadataSize, null);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   * @param ioSizeRecorder can be null
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize, LongConsumer ioSizeRecorder)
      throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is opened. {}", file, getClass().getName());
    }
    this.file = file;
    tsFileInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(file);

    try {
      loadFileVersion(ioSizeRecorder);
      if (loadMetadataSize) {
        loadMetadataSize(ioSizeRecorder);
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

  // ioSizeRecorder can be null
  private void loadFileVersion(LongConsumer ioSizeRecorder) throws IOException {
    try {
      tsFileInput.position(TSFileConfig.MAGIC_STRING.getBytes(TSFileConfig.STRING_CHARSET).length);
      final ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES);
      if (ioSizeRecorder != null) {
        ioSizeRecorder.accept(Byte.BYTES);
      }
      tsFileInput.read(buffer);
      buffer.flip();
      fileVersion = buffer.get();

      checkFileVersion();
      configDeserializer();

      tsFileInput.position(0);
    } catch (Exception e) {
      tsFileInput.close();
      throw new NotCompatibleTsFileException(e);
    }
  }

  private void configDeserializer() {
    if (fileVersion == TSFileConfig.VERSION_NUMBER_V3) {
      deserializeConfig = CompatibilityUtils.v3DeserializeConfig;
    }
  }

  private void checkFileVersion() throws FileVersionTooOldException {
    if (TSFileConfig.VERSION_NUMBER - fileVersion > 1) {
      throw new FileVersionTooOldException(fileVersion, (byte) (TSFileConfig.VERSION_NUMBER - 1));
    }
  }

  public void loadMetadataSize() throws IOException {
    loadMetadataSize(null);
  }

  /**
   * @param ioSizeRecorder can be null
   */
  public void loadMetadataSize(LongConsumer ioSizeRecorder) throws IOException {
    int readSize = Integer.BYTES;
    ByteBuffer metadataSize = ByteBuffer.allocate(readSize);
    if (readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      if (ioSizeRecorder != null) {
        ioSizeRecorder.accept(readSize);
      }
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

  /** Return the tsfile meta data size of this tsfile. */
  public long getFileMetadataSize() throws IOException {
    return tsFileInput.size() - getFileMetadataPos();
  }

  /**
   * Return the whole meta data size of this tsfile, including ChunkMetadata, TimeseriesMetadata and
   * etc.
   */
  public long getAllMetadataSize() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    return tsFileInput.size() - tsFileMetaData.getMetaOffset();
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
    tsFileInput.read(
        versionNumberByte, TSFileConfig.MAGIC_STRING.getBytes(TSFileConfig.STRING_CHARSET).length);
    versionNumberByte.flip();
    return versionNumberByte.get();
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public TsFileMetadata readFileMetadata() throws IOException {
    return readFileMetadata(null);
  }

  /**
   * @param ioSizeRecorder can be null
   */
  public TsFileMetadata readFileMetadata(LongConsumer ioSizeRecorder) throws IOException {
    try {
      if (tsFileMetaData == null) {
        synchronized (this) {
          if (tsFileMetaData == null) {
            tsFileMetaData =
                deserializeConfig.tsFileMetadataBufferDeserializer.deserialize(
                    readData(fileMetadataPos, fileMetadataSize, ioSizeRecorder), deserializeConfig);
          }
        }
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Something error happened while reading file metadata of file {}", file, e);
      throw e;
    }
    return tsFileMetaData;
  }

  /**
   * This function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public BloomFilter readBloomFilter() throws IOException {
    return readBloomFilter(null);
  }

  /**
   * @param ioSizeRecorder can be null
   */
  public BloomFilter readBloomFilter(LongConsumer ioSizeRecorder) throws IOException {
    readFileMetadata(ioSizeRecorder);
    return tsFileMetaData.getBloomFilter();
  }

  /**
   * Retrieves the decryptor for the TsFile. This method reads the file metadata to obtain the
   * decryptor information. If an error occurs while reading the metadata, it logs the error and
   * attempts to retrieve the decryptor based on the configuration settings.
   *
   * @return the decryptor for the TsFile
   * @throws IOException if an I/O error occurs while reading the file metadata
   */
  public EncryptParameter getEncryptParam() throws IOException {
    return getEncryptParam(null);
  }

  /**
   * @param ioSizeRecorder can be null
   */
  public EncryptParameter getEncryptParam(LongConsumer ioSizeRecorder) throws IOException {
    if (fileMetadataSize != 0) {
      readFileMetadata(ioSizeRecorder);
      return tsFileMetaData.getEncryptParam();
    }
    return EncryptUtils.encryptParam;
  }

  /**
   * this function reads measurements and TimeseriesMetaDatas in given device Thread Safe
   *
   * @param device name
   * @return the map measurementId -> TimeseriesMetaData in one device
   * @throws IOException io error
   */
  public Map<String, TimeseriesMetadata> readDeviceMetadata(IDeviceID device) throws IOException {
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

  public void clearCachedDeviceMetadata() {
    cachedDeviceMetadata.clear();
  }

  private Map<String, TimeseriesMetadata> readDeviceMetadataFromDisk(IDeviceID device)
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

  public TimeseriesMetadata readTimeseriesMetadata(
      IDeviceID device, String measurement, boolean ignoreNotExists) throws IOException {
    return readTimeseriesMetadata(device, measurement, ignoreNotExists, null);
  }

  public TimeseriesMetadata readTimeseriesMetadata(
      IDeviceID device,
      String measurement,
      boolean ignoreNotExistDevice,
      LongConsumer ioSizeConsumer)
      throws IOException {
    readFileMetadata(ioSizeConsumer);
    MetadataIndexNode deviceMetadataIndexNode =
        tsFileMetaData.getTableMetadataIndexNode(device.getTableName());
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetOfDeviceNode(deviceMetadataIndexNode, device, true, ioSizeConsumer);
    if (metadataIndexPair == null) {
      if (ignoreNotExistDevice) {
        return null;
      }
      throw new IOException("Device {" + device + "} is not in tsFileMetaData of " + file);
    }
    ByteBuffer buffer =
        readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right, ioSizeConsumer);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode =
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                buffer, deserializeConfig);
      } catch (Exception e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffsetOfMeasurementNode(
              metadataIndexNode, measurement, false, ioSizeConsumer);
    }
    if (metadataIndexPair == null) {
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    if (metadataIndexPair.right - metadataIndexPair.left.getOffset() < Integer.MAX_VALUE) {
      buffer =
          readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right, ioSizeConsumer);
      while (buffer.hasRemaining()) {
        try {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
        } catch (Exception e) {
          logger.error(
              "Something error happened while deserializing TimeseriesMetadata of file {}", file);
          throw e;
        }
      }
    } else {
      if (ioSizeConsumer != null) {
        ioSizeConsumer.accept(metadataIndexPair.right - metadataIndexPair.left.getOffset());
      }
      // when the buffer length is over than Integer.MAX_VALUE,
      // using tsFileInput to get timeseriesMetadataList
      tsFileInput.position(metadataIndexPair.left.getOffset());
      while (tsFileInput.position() < metadataIndexPair.right) {
        try {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(tsFileInput, true));
        } catch (Exception e1) {
          logger.error(
              "Something error happened while deserializing TimeseriesMetadata of file {}", file);
          throw e1;
        }
      }
    }

    // return null if path does not exist in the TsFile
    int searchResult = binarySearchInTimeseriesMetadataList(timeseriesMetadataList, measurement);
    return searchResult >= 0 ? timeseriesMetadataList.get(searchResult) : null;
  }

  // This method is only used for TsFile
  public ITimeSeriesMetadata readITimeseriesMetadata(Path path, boolean ignoreNotExistDevice)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode =
        tsFileMetaData.getTableMetadataIndexNode(path.getIDeviceID().getTableName());
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetOfDeviceNode(deviceMetadataIndexNode, path.getIDeviceID(), true);
    if (metadataIndexPair == null) {
      if (ignoreNotExistDevice) {
        return null;
      }
      throw new IOException(
          "Device {" + path.getDeviceString() + "} is not in tsFileMetaData of " + file);
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode;
    TimeseriesMetadata firstTimeseriesMetadata;
    try {
      // next layer MeasurementNode of the specific DeviceNode
      metadataIndexNode =
          deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
              buffer, deserializeConfig);
    } catch (Exception e) {
      logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
      throw e;
    }
    firstTimeseriesMetadata = getTimeColumnMetadata(metadataIndexNode);
    metadataIndexPair =
        getMetadataAndEndOffsetOfMeasurementNode(
            metadataIndexNode, path.getMeasurement(), false, null);

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

  /**
   * Find the leaf node that contains path, return all the sensors in that leaf node which are also
   * in allSensors set
   *
   * @param ignoreNotExistDevice whether throw IOException if device not found
   * @param ioSizeRecorder can be null
   */
  public List<TimeseriesMetadata> readTimeseriesMetadata(
      IDeviceID device,
      String measurement,
      Set<String> allSensors,
      boolean ignoreNotExistDevice,
      LongConsumer ioSizeRecorder)
      throws IOException {
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getLeafMetadataIndexPair(device, measurement, ioSizeRecorder);
    if (metadataIndexPair == null) {
      if (ignoreNotExistDevice) {
        return Collections.emptyList();
      }
      throw new IOException("Device {" + device + "} is not in tsFileMetaData of " + file);
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();

    if (metadataIndexPair.right - metadataIndexPair.left.getOffset() < Integer.MAX_VALUE) {
      ByteBuffer buffer =
          readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right, ioSizeRecorder);
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
    } else {
      // when the buffer length is over than Integer.MAX_VALUE,
      // using tsFileInput to get timeseriesMetadataList
      synchronized (this) {
        if (ioSizeRecorder != null) {
          ioSizeRecorder.accept(metadataIndexPair.right - metadataIndexPair.left.getOffset());
        }
        tsFileInput.position(metadataIndexPair.left.getOffset());
        while (tsFileInput.position() < metadataIndexPair.right) {
          TimeseriesMetadata timeseriesMetadata;
          try {
            timeseriesMetadata = TimeseriesMetadata.deserializeFrom(tsFileInput, true);
          } catch (StopReadTsFileByInterruptException e) {
            throw e;
          } catch (Exception e1) {
            logger.error(
                "Something error happened while deserializing TimeseriesMetadata of file {}", file);
            throw e1;
          }
          if (allSensors.contains(timeseriesMetadata.getMeasurementId())) {
            timeseriesMetadataList.add(timeseriesMetadata);
          }
        }
      }
    }
    return timeseriesMetadataList;
  }

  /* Get leaf MetadataIndexPair which contains path */
  private Pair<IMetadataIndexEntry, Long> getLeafMetadataIndexPair(
      IDeviceID device, String measurement, LongConsumer ioSizeRecorder) throws IOException {
    readFileMetadata(ioSizeRecorder);
    MetadataIndexNode deviceMetadataIndexNode =
        tsFileMetaData.getTableMetadataIndexNode(device.getTableName());
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetOfDeviceNode(deviceMetadataIndexNode, device, true, ioSizeRecorder);
    if (metadataIndexPair == null) {
      return null;
    }
    ByteBuffer buffer =
        readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right, ioSizeRecorder);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode =
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                buffer, deserializeConfig);
      } catch (Exception e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffsetOfMeasurementNode(
              metadataIndexNode, measurement, false, ioSizeRecorder);
    }
    return metadataIndexPair;
  }

  private MetadataIndexNode getTableRootNode(String tableName) throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getTableMetadataIndexNode(tableName);
    if (metadataIndexNode == null && fileVersion < TSFileConfig.VERSION_NUMBER) {
      // this file if from an old version, and all its metadata should have an anonymous root
      metadataIndexNode = tsFileMetaData.getTableMetadataIndexNode("");
    }
    return metadataIndexNode;
  }

  /**
   * Searching from the start node and try to find the root node of the deviceID.
   *
   * @param deviceID desired device
   * @param startNode start of the search, if not provided, start from the table root
   * @return MetadataIndexNode which is the root of deviceID
   */
  private MetadataIndexNode getDeviceRootNode(IDeviceID deviceID, MetadataIndexNode startNode)
      throws IOException {
    readFileMetadata();
    startNode = startNode != null ? startNode : getTableRootNode(deviceID.getTableName());
    if (startNode == null) {
      return null;
    }

    MetadataIndexNode measurementMetadataIndexNode;
    ByteBuffer buffer;
    if (startNode.isDeviceLevel()) {
      Pair<IMetadataIndexEntry, Long> metadataIndexPair =
          getMetadataAndEndOffsetOfDeviceNode(startNode, deviceID, true);
      if (metadataIndexPair == null) {
        return null;
      }

      // the content of next Layer MeasurementNode of the specific device's DeviceNode
      buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
      // next layer MeasurementNode of the specific DeviceNode
      try {
        measurementMetadataIndexNode =
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                buffer, deserializeConfig);
      } catch (Exception e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
    } else {
      measurementMetadataIndexNode = startNode;
    }
    return measurementMetadataIndexNode;
  }

  /**
   * Read the TimeseriesMetadata of the given measurement and its successors from the index node
   * into the list.
   *
   * @param timeseriesMetadataList result holder
   * @param node index node to be read from
   * @param measurement the desired measurement
   * @return true if the measurement exists
   * @throws IOException when read fails
   */
  public boolean readITimeseriesMetadata(
      List<TimeseriesMetadata> timeseriesMetadataList, MetadataIndexNode node, String measurement)
      throws IOException {
    Pair<IMetadataIndexEntry, Long> measurementMetadataIndexPair =
        getMetadataAndEndOffsetOfMeasurementNode(node, measurement, false, null);

    if (measurementMetadataIndexPair == null) {
      return false;
    }
    // the content of TimeseriesNode of the specific MeasurementLeafNode
    ByteBuffer buffer =
        readData(measurementMetadataIndexPair.left.getOffset(), measurementMetadataIndexPair.right);
    while (buffer.hasRemaining()) {
      try {
        timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
      } catch (Exception e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
    }
    return true;
  }

  /**
   * Read TimeSeriesMetadata of the given device. This method is only used for TsFile.
   *
   * @param device deviceId to be read
   * @param measurements measurements to be read
   * @param root search start node, if not provided, use the root node of the table of the device
   * @param mergeAlignedSeries see @return
   * @return when the device is not aligned, or mergeAlignedSeries is false, each result correspond
   *     to one series in the provided measurements (if exists); otherwise, all columns in the
   *     aligned device will be merged into one AlignedTimeSeriesMetadata.
   * @throws IOException if read fails
   */
  public List<ITimeSeriesMetadata> readITimeseriesMetadata(
      IDeviceID device,
      Set<String> measurements,
      MetadataIndexNode root,
      boolean mergeAlignedSeries)
      throws IOException {
    // find the index node associated with the device
    final MetadataIndexNode measurementMetadataIndexNode = getDeviceRootNode(device, root);
    if (measurementMetadataIndexNode == null) {
      return Collections.emptyList();
    }

    // Get the time column metadata if the device is aligned
    TimeseriesMetadata timeColumnMetadata = getTimeColumnMetadata(measurementMetadataIndexNode);
    List<TimeseriesMetadata> valueTimeseriesMetadataList =
        timeColumnMetadata != null ? new ArrayList<>() : null;

    List<ITimeSeriesMetadata> resultTimeseriesMetadataList = new ArrayList<>();
    List<String> measurementList = new ArrayList<>(measurements);
    measurementList.sort(null);
    boolean[] measurementFound = new boolean[measurements.size()];
    int measurementFoundCnt = 0;

    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    for (int i = 0;
        i < measurementList.size() && measurementFoundCnt < measurementList.size();
        i++) {
      final String measurementName = measurementList.get(i);
      timeseriesMetadataList.clear();
      // read the leaf node that may contain the i-th measurement into a list
      if (measurementFound[i]
          || !readITimeseriesMetadata(
              timeseriesMetadataList, measurementMetadataIndexNode, measurementName)) {
        continue;
      }
      // in the list, search for the all measurements that are not found
      measurementFoundCnt +=
          searchInTimeseriesList(
              measurementList,
              i,
              measurementFound,
              timeseriesMetadataList,
              resultTimeseriesMetadataList,
              timeColumnMetadata,
              valueTimeseriesMetadataList,
              mergeAlignedSeries);
    }
    if (valueTimeseriesMetadataList != null && !valueTimeseriesMetadataList.isEmpty()) {
      resultTimeseriesMetadataList.add(
          new AlignedTimeSeriesMetadata(timeColumnMetadata, valueTimeseriesMetadataList));
    }
    return resultTimeseriesMetadataList;
  }

  private int searchInTimeseriesList(
      List<String> measurementList,
      int startIndex,
      boolean[] measurementFound,
      List<TimeseriesMetadata> timeseriesMetadataList,
      List<ITimeSeriesMetadata> resultTimeseriesMetadataList,
      TimeseriesMetadata timeColumnMetadata,
      List<TimeseriesMetadata> valueTimeseriesMetadataList,
      boolean mergeAlignedSeries) {
    int numOfFoundMeasurements = 0;
    for (int j = startIndex; j < measurementList.size(); j++) {
      int searchResult;
      if (measurementFound[j]
          || (searchResult =
                  binarySearchInTimeseriesMetadataList(
                      timeseriesMetadataList, measurementList.get(j)))
              < 0) {
        continue;
      }

      final TimeseriesMetadata valueColumnMetadata = timeseriesMetadataList.get(searchResult);
      if (timeColumnMetadata != null) {
        if (!mergeAlignedSeries) {
          resultTimeseriesMetadataList.add(
              new AlignedTimeSeriesMetadata(
                  timeColumnMetadata, Collections.singletonList(valueColumnMetadata)));
        } else if (valueTimeseriesMetadataList != null) {
          valueTimeseriesMetadataList.add(valueColumnMetadata);
        }
      } else {
        resultTimeseriesMetadataList.add(valueColumnMetadata);
      }
      measurementFound[j] = true;
      numOfFoundMeasurements++;
    }
    return numOfFoundMeasurements;
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

  public List<IDeviceID> getAllDevices() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    List<IDeviceID> deviceIDS = new ArrayList<>();
    for (Entry<String, MetadataIndexNode> entry :
        tsFileMetaData.getTableMetadataIndexNodeMap().entrySet()) {
      deviceIDS.addAll(getAllDevices(entry.getValue()));
    }
    return deviceIDS;
  }

  public List<IDeviceID> getAllDevices(MetadataIndexNode metadataIndexNode) throws IOException {
    List<IDeviceID> deviceList = new ArrayList<>();
    // if metadataIndexNode is LEAF_DEVICE, put all devices in node entry into the list
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      deviceList.addAll(
          metadataIndexNode.getChildren().stream()
              .map(entry -> ((DeviceMetadataIndexEntry) entry).getDeviceID())
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
      MetadataIndexNode node =
          deserializeConfig.deviceMetadataIndexNodeBufferDeserializer.deserialize(
              buffer, deserializeConfig);
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
    return new TsFileDeviceIterator(this);
  }

  /**
   * read all ChunkMetaDatas of given device
   *
   * @param device name
   * @return measurement -> ChunkMetadata list
   * @throws IOException io error
   */
  public Map<String, List<ChunkMetadata>> readChunkMetadataInDevice(IDeviceID device)
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
    for (IDeviceID device : getAllDevices()) {
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

    Queue<Pair<IDeviceID, Pair<Long, Long>>> queue = new LinkedList<>();
    for (MetadataIndexNode metadataIndexNode :
        tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
      List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
      for (int i = 0; i < metadataIndexEntryList.size(); i++) {
        IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexEntryList.size() - 1) {
          endOffset = metadataIndexEntryList.get(i + 1).getOffset();
        }
        ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
        getAllPaths(metadataIndexEntry, buffer, null, metadataIndexNode.getNodeType(), queue);
      }
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
        Pair<IDeviceID, Pair<Long, Long>> startEndPair = queue.remove();
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
      IMetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      IDeviceID deviceId,
      MetadataIndexNodeType type,
      Queue<Pair<IDeviceID, Pair<Long, Long>>> queue)
      throws IOException {
    try {
      if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
        deviceId = ((DeviceMetadataIndexEntry) metadataIndex).getDeviceID();
      }
      boolean currentChildLevelIsDevice = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
      MetadataIndexNode metadataIndexNode =
          deserializeConfig.deserializeMetadataIndexNode(buffer, currentChildLevelIsDevice);

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
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Something error happened while getting all paths of file {}", file);
      throw e;
    }
  }

  /**
   * Check whether the device is aligned or not.
   *
   * @param measurementNode the next measurement layer node of specific device node
   */
  public boolean isAlignedDevice(MetadataIndexNode measurementNode) {
    IMetadataIndexEntry entry = measurementNode.getChildren().get(0);
    return "".equals(((MeasurementMetadataIndexEntry) entry).getName());
  }

  TimeseriesMetadata getTimeColumnMetadata(MetadataIndexNode measurementNode) throws IOException {
    // Not aligned timeseries
    if (!isAlignedDevice(measurementNode)) {
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
      MetadataIndexNode metadataIndexNode =
          deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
              buffer, deserializeConfig);
      return getTimeColumnMetadata(metadataIndexNode);
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
        new LinkedHashMap<>();
    List<IMetadataIndexEntry> childrenEntryList = measurementNode.getChildren();
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
        MetadataIndexNode nextLayerMeasurementNode =
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                nextBuffer, deserializeConfig);
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
        MetadataIndexNode nextLayerMeasurementNode =
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                nextBuffer, deserializeConfig);
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
   * @param deviceId IDeviceID
   * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
   * @param needChunkMetadata deserialize chunk metadata list or not
   */
  private void generateMetadataIndex(
      IMetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      IDeviceID deviceId,
      MetadataIndexNodeType type,
      Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap,
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
          deviceId = ((DeviceMetadataIndexEntry) metadataIndex).getDeviceID();
        }
        boolean currentChildLevelIsDevice = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
        MetadataIndexNode metadataIndexNode =
            deserializeConfig.deserializeMetadataIndexNode(buffer, currentChildLevelIsDevice);

        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          if (endOffset - metadataIndexNode.getChildren().get(i).getOffset() < Integer.MAX_VALUE) {
            ByteBuffer nextBuffer =
                readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
            generateMetadataIndex(
                metadataIndexNode.getChildren().get(i),
                nextBuffer,
                deviceId,
                metadataIndexNode.getNodeType(),
                timeseriesMetadataMap,
                needChunkMetadata);
          } else {
            // when the buffer length is over than Integer.MAX_VALUE,
            // using tsFileInput to get timeseriesMetadataList
            generateMetadataIndexUsingTsFileInput(
                metadataIndexNode.getChildren().get(i),
                metadataIndexNode.getChildren().get(i).getOffset(),
                endOffset,
                deviceId,
                metadataIndexNode.getNodeType(),
                timeseriesMetadataMap,
                needChunkMetadata);
          }
        }
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Something error happened while generating MetadataIndex of file {}", file);
      throw e;
    }
  }

  private void generateMetadataIndexUsingTsFileInput(
      IMetadataIndexEntry metadataIndex,
      long start,
      long end,
      IDeviceID deviceId,
      MetadataIndexNodeType type,
      Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap,
      boolean needChunkMetadata)
      throws IOException {
    try {
      tsFileInput.position(start);
      if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (tsFileInput.position() < end) {
          timeseriesMetadataList.add(
              TimeseriesMetadata.deserializeFrom(tsFileInput, needChunkMetadata));
        }
        timeseriesMetadataMap
            .computeIfAbsent(deviceId, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
      } else {
        // deviceId should be determined by LEAF_DEVICE node
        if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
          deviceId = ((DeviceMetadataIndexEntry) metadataIndex).getDeviceID();
        }
        boolean currentChildLevelIsDevice = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
        MetadataIndexNode metadataIndexNode =
            deserializeConfig.deserializeMetadataIndexNode(
                tsFileInput.wrapAsInputStream(), currentChildLevelIsDevice);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          generateMetadataIndexUsingTsFileInput(
              metadataIndexNode.getChildren().get(i),
              metadataIndexNode.getChildren().get(i).getOffset(),
              endOffset,
              deviceId,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap,
              needChunkMetadata);
        }
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Something error happened while generating MetadataIndex of file {}", file);
      throw e;
    }
  }

  /* TimeseriesMetadata don't need deserialize chunk metadata list */
  public Map<IDeviceID, List<TimeseriesMetadata>> getAllTimeseriesMetadata(
      boolean needChunkMetadata) throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap = new HashMap<>();
    for (MetadataIndexNode metadataIndexNode :
        tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
      List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
      for (int i = 0; i < metadataIndexEntryList.size(); i++) {
        IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexEntryList.size() - 1) {
          endOffset = metadataIndexEntryList.get(i + 1).getOffset();
        }
        if (endOffset - metadataIndexEntry.getOffset() < Integer.MAX_VALUE) {
          ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
          generateMetadataIndex(
              metadataIndexEntry,
              buffer,
              null,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap,
              needChunkMetadata);
        } else {
          generateMetadataIndexUsingTsFileInput(
              metadataIndexNode.getChildren().get(i),
              metadataIndexNode.getChildren().get(i).getOffset(),
              endOffset,
              null,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap,
              needChunkMetadata);
        }
      }
    }

    return timeseriesMetadataMap;
  }

  /* This method will only deserialize the TimeseriesMetadata, not including chunk metadata list */
  public List<TimeseriesMetadata> getDeviceTimeseriesMetadataWithoutChunkMetadata(IDeviceID device)
      throws IOException {
    return getDeviceTimeseriesMetadata(device, false);
  }

  /* This method will not only deserialize the TimeseriesMetadata, but also all the chunk metadata list meanwhile. */
  public List<TimeseriesMetadata> getDeviceTimeseriesMetadata(IDeviceID device) throws IOException {
    return getDeviceTimeseriesMetadata(device, true);
  }

  private List<TimeseriesMetadata> getDeviceTimeseriesMetadata(
      IDeviceID device, boolean needChunkMetadata) throws IOException {
    MetadataIndexNode metadataIndexNode =
        tsFileMetaData.getTableMetadataIndexNode(device.getTableName());
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetOfDeviceNode(metadataIndexNode, device, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        needChunkMetadata);
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
   * @param deviceID target device
   * @param exactSearch whether is in exact search mode, return null when there is no entry with
   *     name; or else return the nearest MetadataIndexEntry before it (for deeper search)
   * @return target MetadataIndexEntry, endOffset pair
   */
  protected Pair<IMetadataIndexEntry, Long> getMetadataAndEndOffsetOfDeviceNode(
      MetadataIndexNode metadataIndex, IDeviceID deviceID, boolean exactSearch) throws IOException {
    return getMetadataAndEndOffsetOfDeviceNode(metadataIndex, deviceID, exactSearch, null);
  }

  /**
   * @param ioSizeRecorder can be null
   */
  protected Pair<IMetadataIndexEntry, Long> getMetadataAndEndOffsetOfDeviceNode(
      MetadataIndexNode metadataIndex,
      IDeviceID deviceID,
      boolean exactSearch,
      LongConsumer ioSizeRecorder)
      throws IOException {
    if (metadataIndex == null) {
      return null;
    }
    if (MetadataIndexNodeType.INTERNAL_MEASUREMENT.equals(metadataIndex.getNodeType())
        || MetadataIndexNodeType.LEAF_MEASUREMENT.equals(metadataIndex.getNodeType())) {
      throw new IllegalArgumentException();
    }
    try {
      if (MetadataIndexNodeType.INTERNAL_DEVICE.equals(metadataIndex.getNodeType())) {
        Pair<IMetadataIndexEntry, Long> childIndexEntry =
            metadataIndex.getChildIndexEntry(deviceID, false);
        ByteBuffer buffer =
            readData(childIndexEntry.left.getOffset(), childIndexEntry.right, ioSizeRecorder);
        return getMetadataAndEndOffsetOfDeviceNode(
            deserializeConfig.deviceMetadataIndexNodeBufferDeserializer.deserialize(
                buffer, deserializeConfig),
            deviceID,
            exactSearch,
            ioSizeRecorder);
      } else {
        return metadataIndex.getChildIndexEntry(deviceID, exactSearch);
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Something error happened while deserializing MetadataIndex of file {}", file);
      throw e;
    }
  }

  /**
   * Get target MetadataIndexEntry and its end offset
   *
   * @param metadataIndex given MetadataIndexNode
   * @param measurement target measurement
   * @param exactSearch whether is in exact search mode, return null when there is no entry with
   *     name; or else return the nearest MetadataIndexEntry before it (for deeper search)
   * @param ioSizeRecorder can be null
   * @return target MetadataIndexEntry, endOffset pair
   */
  protected Pair<IMetadataIndexEntry, Long> getMetadataAndEndOffsetOfMeasurementNode(
      MetadataIndexNode metadataIndex,
      String measurement,
      boolean exactSearch,
      LongConsumer ioSizeRecorder)
      throws IOException {
    if (MetadataIndexNodeType.INTERNAL_DEVICE.equals(metadataIndex.getNodeType())
        || MetadataIndexNodeType.LEAF_DEVICE.equals(metadataIndex.getNodeType())) {
      throw new IllegalArgumentException();
    }
    try {
      if (MetadataIndexNodeType.INTERNAL_MEASUREMENT.equals(metadataIndex.getNodeType())) {
        Pair<IMetadataIndexEntry, Long> childIndexEntry =
            metadataIndex.getChildIndexEntry(measurement, false);
        ByteBuffer buffer =
            readData(childIndexEntry.left.getOffset(), childIndexEntry.right, ioSizeRecorder);
        return getMetadataAndEndOffsetOfMeasurementNode(
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                buffer, deserializeConfig),
            measurement,
            exactSearch,
            ioSizeRecorder);
      } else {
        return metadataIndex.getChildIndexEntry(measurement, exactSearch);
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
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
    return ChunkGroupHeader.deserializeFrom(
        tsFileInput.wrapAsInputStream(), true, deserializeConfig.versionNumber);
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
    return ChunkGroupHeader.deserializeFrom(
        tsFileInput, position, markerRead, deserializeConfig.versionNumber);
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
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading chunk header of {}", t.getMessage(), file);
      throw t;
    }
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param ioSizeRecorder can be null
   */
  private ChunkHeader readChunkHeader(long position, LongConsumer ioSizeRecorder)
      throws IOException {
    try {
      return ChunkHeader.deserializeFrom(tsFileInput, position, ioSizeRecorder);
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
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
    return readChunk(position, dataSize, null);
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @param ioSizeRecorder can be null
   * @return the pages of this chunk
   */
  public ByteBuffer readChunk(long position, int dataSize, LongConsumer ioSizeRecorder)
      throws IOException {
    try {
      return readData(position, dataSize, ioSizeRecorder);
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Throwable t) {
      logger.warn("Exception {} happened while reading chunk of {}", t.getMessage(), file);
      throw t;
    }
  }

  /**
   * read memory chunk.
   *
   * @return -chunk
   */
  public Chunk readMemChunk(long offset) throws IOException {
    return readMemChunk(offset, null);
  }

  /**
   * @param ioSizeRecorder can be null
   */
  public Chunk readMemChunk(long offset, LongConsumer ioSizeRecorder) throws IOException {
    try {
      ChunkHeader header = readChunkHeader(offset, ioSizeRecorder);
      ByteBuffer buffer =
          readChunk(offset + header.getSerializedSize(), header.getDataSize(), ioSizeRecorder);
      return new Chunk(header, buffer, getEncryptParam(ioSizeRecorder));
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
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
      ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), null);
      ByteBuffer buffer =
          readChunk(
              metaData.getOffsetOfChunkHeader() + header.getSerializedSize(), header.getDataSize());
      return new Chunk(
          header,
          buffer,
          metaData.getDeleteIntervalList(),
          metaData.getStatistics(),
          getEncryptParam());
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
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
    ChunkHeader header = readChunkHeader(chunkCacheKey.getOffsetOfChunkHeader(), null);
    ByteBuffer buffer =
        readChunk(
            chunkCacheKey.getOffsetOfChunkHeader() + header.getSerializedSize(),
            header.getDataSize());
    return new Chunk(
        header,
        buffer,
        chunkCacheKey.getDeleteIntervalList(),
        chunkCacheKey.getStatistics(),
        getEncryptParam());
  }

  /**
   * read the {@link CompressionType} and {@link TSEncoding} of a timeseries. This method will skip
   * the measurement id, and data type. This method will change the position of this reader.
   *
   * @param timeseriesMetadata timeseries' metadata
   * @return a pair of {@link CompressionType} and {@link TSEncoding} of given timeseries.
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
    ChunkHeader header = readChunkHeader(lastChunkMetadata.getOffsetOfChunkHeader(), null);
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
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
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
    IDecryptor decryptor = IDecryptor.getDecryptor(getEncryptParam());
    if (header.getUncompressedSize() == 0) {
      return buffer;
    }
    ByteBuffer finalBuffer = decrypt(decryptor, buffer);
    finalBuffer = uncompress(type, finalBuffer, header.getUncompressedSize());
    return finalBuffer;
  }

  private static ByteBuffer decrypt(IDecryptor decryptor, ByteBuffer buffer) {
    if (decryptor == null || decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return buffer;
    }
    return ByteBuffer.wrap(
        decryptor.decrypt(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining()));
  }

  private static ByteBuffer uncompress(
      CompressionType compressionType, ByteBuffer buffer, int uncompressedSize) throws IOException {
    if (compressionType == CompressionType.UNCOMPRESSED) {
      return buffer;
    }
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(uncompressedSize);
    try {
      unCompressor.uncompress(
          buffer.array(),
          buffer.arrayOffset() + buffer.position(),
          buffer.remaining(),
          uncompressedBuffer.array(),
          0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + uncompressedSize
              + "compressed size: "
              + buffer.remaining()
              + e.getMessage(),
          e);
    }

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
    return readData(position, totalSize, null);
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
   * @param ioSizeRecorder can be null
   * @return data that been read.
   */
  protected ByteBuffer readData(long position, int totalSize, LongConsumer ioSizeRecorder)
      throws IOException {
    if (ioSizeRecorder != null) {
      ioSizeRecorder.accept(totalSize);
    }
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
    return readData(start, end, null);
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position.
   *
   * @param start the start position of data in the tsFileInput, or the current position if position
   *     = -1
   * @param end the end position of data that want to read
   * @param ioSizeRecorder can be null
   * @return data that been read.
   */
  protected ByteBuffer readData(long start, long end, LongConsumer ioSizeRecorder)
      throws IOException {
    try {
      return readData(start, (int) (end - start), ioSizeRecorder);
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
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
   * @param schema the schema on each time series in the file
   * @param chunkGroupMetadataList ChunkGroupMetadata List
   * @param fastFinish if true and the file is complete, then newSchema and chunkGroupMetadataList
   *     parameter will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   *     be truncated.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public long selfCheck(
      Schema schema, List<ChunkGroupMetadata> chunkGroupMetadataList, boolean fastFinish)
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
    if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic())) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    fileVersion = readVersionNumber();
    checkFileVersion();

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
    IDeviceID lastDeviceId = null;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    Map<String, Integer> valueColumn2TimeBatchIndex = new HashMap<>();
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

            Statistics<? extends Serializable> chunkStatistics =
                Statistics.getStatsByType(dataType);
            int dataSize = chunkHeader.getDataSize();

            if (dataSize > 0) {
              if (marker == MetaMarker.TIME_CHUNK_HEADER) {
                timeBatch.add(null);
              }
              if (((byte) (chunkHeader.getChunkType() & 0x3F))
                  == MetaMarker
                      .CHUNK_HEADER) { // more than one page, we could use page statistics to
                if (marker == MetaMarker.VALUE_CHUNK_HEADER) {
                  int timeBatchIndex =
                      valueColumn2TimeBatchIndex.getOrDefault(chunkHeader.getMeasurementID(), 0);
                  valueColumn2TimeBatchIndex.put(
                      chunkHeader.getMeasurementID(), timeBatchIndex + 1);
                }
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
                  int timeBatchIndex =
                      valueColumn2TimeBatchIndex.getOrDefault(chunkHeader.getMeasurementID(), 0);
                  valueColumn2TimeBatchIndex.put(
                      chunkHeader.getMeasurementID(), timeBatchIndex + 1);
                  TsPrimitiveType[] valueBatch =
                      valuePageReader.nextValueBatch(timeBatch.get(timeBatchIndex));

                  if (valueBatch != null && valueBatch.length != 0) {
                    for (int i = 0; i < valueBatch.length; i++) {
                      TsPrimitiveType value = valueBatch[i];
                      if (value == null) {
                        continue;
                      }
                      long timeStamp = timeBatch.get(timeBatchIndex)[i];
                      switch (dataType) {
                        case INT32:
                        case DATE:
                          chunkStatistics.update(timeStamp, value.getInt());
                          break;
                        case INT64:
                        case TIMESTAMP:
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
                        case BLOB:
                        case STRING:
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
                          timeDecoder);
                  BatchData batchData = reader.getAllSatisfiedPageData();
                  while (batchData.hasCurrent()) {
                    switch (dataType) {
                      case INT32:
                      case DATE:
                        chunkStatistics.update(batchData.currentTime(), batchData.getInt());
                        break;
                      case INT64:
                      case TIMESTAMP:
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
                      case BLOB:
                      case STRING:
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
            } else if (marker == MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER
                || marker == MetaMarker.VALUE_CHUNK_HEADER) {
              int timeBatchIndex =
                  valueColumn2TimeBatchIndex.getOrDefault(chunkHeader.getMeasurementID(), 0);
              valueColumn2TimeBatchIndex.put(chunkHeader.getMeasurementID(), timeBatchIndex + 1);
            }
            currentChunk =
                new ChunkMetadata(
                    measurementID,
                    dataType,
                    chunkHeader.getEncodingType(),
                    chunkHeader.getCompressionType(),
                    fileOffsetOfChunk,
                    chunkStatistics);
            chunkMetadataList.add(currentChunk);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // if there is something wrong with the ChunkGroup Header, we will drop this ChunkGroup
            // because we can not guarantee the correctness of the deviceId.
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (schema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  schema.registerTimeseries(lastDeviceId, tsSchema);
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
            timeBatch.clear();
            valueColumn2TimeBatchIndex.clear();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (schema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  schema.registerTimeseries(lastDeviceId, tsSchema);
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
        if (schema != null) {
          for (IMeasurementSchema tsSchema : measurementSchemaList) {
            schema.registerTimeseries(lastDeviceId, tsSchema);
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

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      schema.updateTableSchema(chunkGroupMetadata);
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
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (IOException e) {
      logger.error("Error occurred while fast checking TsFile.");
      throw e;
    }
    for (Map.Entry<Long, Pair<Path, TimeseriesMetadata>> entry : timeseriesMetadataMap.entrySet()) {
      TimeseriesMetadata timeseriesMetadata = entry.getValue().right;
      TSDataType dataType = timeseriesMetadata.getTsDataType();
      Statistics<? extends Serializable> timeseriesMetadataSta = timeseriesMetadata.getStatistics();
      Statistics<? extends Serializable> chunkMetadatasSta = Statistics.getStatsByType(dataType);
      for (IChunkMetadata chunkMetadata : getChunkMetadataList(entry.getValue().left)) {
        long tscheckStatus = TsFileCheckStatus.COMPLETE_FILE;
        try {
          tscheckStatus = checkChunkAndPagesStatistics(chunkMetadata);
        } catch (StopReadTsFileByInterruptException e) {
          throw e;
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
              pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder);
      BatchData batchData = reader.getAllSatisfiedPageData();
      while (batchData.hasCurrent()) {
        switch (dataType) {
          case INT32:
          case DATE:
            chunkStatistics.update(batchData.currentTime(), batchData.getInt());
            break;
          case INT64:
          case TIMESTAMP:
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
          case BLOB:
          case STRING:
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
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> getChunkMetadataList(
      IDeviceID deviceID, String measurement, boolean ignoreNotExists) throws IOException {
    TimeseriesMetadata timeseriesMetaData =
        readTimeseriesMetadata(deviceID, measurement, ignoreNotExists);
    if (timeseriesMetaData == null) {
      return Collections.emptyList();
    }
    List<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetaData);
    chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));
    return chunkMetadataList;
  }

  @Deprecated
  public List<ChunkMetadata> getChunkMetadataList(Path path, boolean ignoreNotExists)
      throws IOException {
    return getChunkMetadataList(path.getIDeviceID(), path.getMeasurement(), ignoreNotExists);
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

  public List<IChunkMetadata> getIChunkMetadataList(IDeviceID deviceID, String measurementName)
      throws IOException {
    List<ITimeSeriesMetadata> timeseriesMetaData =
        readITimeseriesMetadata(deviceID, Collections.singleton(measurementName), null, false);
    if (timeseriesMetaData == null || timeseriesMetaData.isEmpty()) {
      return Collections.emptyList();
    }
    List<IChunkMetadata> chunkMetadataList = readIChunkMetaDataList(timeseriesMetaData.get(0));
    chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));
    return chunkMetadataList;
  }

  public List<List<IChunkMetadata>> getIChunkMetadataList(
      IDeviceID deviceID, Set<String> measurementNames, MetadataIndexNode root) throws IOException {
    List<ITimeSeriesMetadata> timeseriesMetaData =
        readITimeseriesMetadata(deviceID, measurementNames, root, true);
    if (timeseriesMetaData == null || timeseriesMetaData.isEmpty()) {
      return Collections.emptyList();
    }
    List<List<IChunkMetadata>> results = new ArrayList<>(timeseriesMetaData.size());
    for (ITimeSeriesMetadata timeseriesMetaDatum : timeseriesMetaData) {
      List<IChunkMetadata> chunkMetadataList = readIChunkMetaDataList(timeseriesMetaDatum);
      chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));
      results.add(chunkMetadataList);
    }
    return results;
  }

  public List<ChunkMetadata> getChunkMetadataList(Path path) throws IOException {
    return getChunkMetadataList(path, true);
  }

  /**
   * Get AlignedChunkMetadata of sensors under one device. Notice: if all the value chunks is empty
   * chunk, then return empty list.
   *
   * @param device device name
   */
  public List<AlignedChunkMetadata> getAlignedChunkMetadata(
      IDeviceID device, boolean ignoreAllNullRows) throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode =
        tsFileMetaData.getTableMetadataIndexNode(device.getTableName());
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetOfDeviceNode(deviceMetadataIndexNode, device, true);
    if (metadataIndexPair == null) {
      throw new IOException("Device {" + device + "} is not in tsFileMetaData");
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode;
    try {
      // next layer MeasurementNode of the specific DeviceNode
      metadataIndexNode =
          deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
              buffer, deserializeConfig);
    } catch (Exception e) {
      logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
      throw e;
    }
    return getAlignedChunkMetadataByMetadataIndexNode(device, metadataIndexNode, ignoreAllNullRows);
  }

  /**
   * Get AlignedChunkMetadata of sensors under one device. Notice: if all the value chunks is empty
   * chunk, then return empty list.
   *
   * @param device device name
   * @param metadataIndexNode the first measurement metadata index node of the device
   * @param ignoreAllNullRows ignore all null rows
   */
  public List<AlignedChunkMetadata> getAlignedChunkMetadataByMetadataIndexNode(
      IDeviceID device, MetadataIndexNode metadataIndexNode, boolean ignoreAllNullRows)
      throws IOException {
    TimeseriesMetadata firstTimeseriesMetadata = getTimeColumnMetadata(metadataIndexNode);
    if (firstTimeseriesMetadata == null) {
      throw new IOException("Timeseries of device {" + device + "} are not aligned");
    }

    Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();

    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = metadataIndexNode.getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
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

    if (timeseriesMetadataMap.values().size() != 1) {
      throw new IOException(
          String.format(
              "Error when reading timeseriesMetadata of device %s in file %s: should only one timeseriesMetadataList in one device, actual: %d",
              device, file, timeseriesMetadataMap.values().size()));
    }

    List<TimeseriesMetadata> timeseriesMetadataList =
        timeseriesMetadataMap.values().iterator().next();
    TimeseriesMetadata timeseriesMetadata = timeseriesMetadataList.get(0);
    List<TimeseriesMetadata> valueTimeseriesMetadataList = new ArrayList<>();

    for (int i = 1; i < timeseriesMetadataList.size(); i++) {
      valueTimeseriesMetadataList.add(timeseriesMetadataList.get(i));
    }

    AbstractAlignedTimeSeriesMetadata alignedTimeSeriesMetadata;
    if (ignoreAllNullRows) {
      alignedTimeSeriesMetadata =
          new AlignedTimeSeriesMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
    } else {
      alignedTimeSeriesMetadata =
          new TableDeviceMetadata(timeseriesMetadata, valueTimeseriesMetadataList);
    }
    List<AlignedChunkMetadata> chunkMetadataList = new ArrayList<>();
    for (IChunkMetadata chunkMetadata : readIChunkMetaDataList(alignedTimeSeriesMetadata)) {
      chunkMetadataList.add((AlignedChunkMetadata) chunkMetadata);
    }
    return chunkMetadataList;
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
    if (timeseriesMetaData instanceof AbstractAlignedTimeSeriesMetadata) {
      return new ArrayList<>(
          ((AbstractAlignedTimeSeriesMetadata) timeseriesMetaData).getChunkMetadataList());
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
    for (IDeviceID device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTsDataType());
      }
    }
    return result;
  }

  /**
   * get all types of measurements in this file
   *
   * @return full path -> datatype
   */
  public Map<String, TSDataType> getFullPathDataTypeMap() throws IOException {
    final Map<String, TSDataType> result = new HashMap<>();
    for (final IDeviceID device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result.put(
            device.toString()
                + TsFileConstant.PATH_SEPARATOR
                + timeseriesMetadata.getMeasurementId(),
            timeseriesMetadata.getTsDataType());
      }
    }
    return result;
  }

  public Map<IDeviceID, List<String>> getDeviceMeasurementsMap() throws IOException {
    Map<IDeviceID, List<String>> result = new HashMap<>();
    for (IDeviceID device : getAllDevices()) {
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
  public List<IDeviceID> getDeviceNameInRange(long start, long end) throws IOException {
    List<IDeviceID> res = new ArrayList<>();
    for (IDeviceID device : getAllDevices()) {
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
   * @param isDeviceLevel is current MetadataIndexNode in device level
   * @return MetadataIndexNode
   */
  public MetadataIndexNode readMetadataIndexNode(
      long startOffset, long endOffset, boolean isDeviceLevel) throws IOException {
    MetadataIndexNode metadataIndexNode;
    final ByteBuffer buffer = readData(startOffset, endOffset);
    if (isDeviceLevel) {
      metadataIndexNode =
          deserializeConfig.deviceMetadataIndexNodeBufferDeserializer.deserialize(
              buffer, deserializeConfig);
    } else {
      metadataIndexNode =
          deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
              buffer, deserializeConfig);
    }
    return metadataIndexNode;
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
        if (location == LocateStatus.IN) {
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
    IN,
    BEFORE,
    AFTER
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
      IDeviceID device) throws IOException {
    readFileMetadata();

    MetadataIndexNode metadataIndexNode =
        tsFileMetaData.getTableMetadataIndexNode(device.getTableName());
    Pair<IMetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetOfDeviceNode(metadataIndexNode, device, true);

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

    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);

    MetadataIndexNode firstMeasurementNode =
        deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
            buffer, deserializeConfig);
    return getMeasurementChunkMetadataListMapIterator(firstMeasurementNode);
  }

  /**
   * @return An iterator of linked hashmaps ( measurement -> chunk metadata list ). When traversing
   *     the linked hashmap, you will get chunk metadata lists according to the lexicographic order
   *     of the measurements. The first measurement of the linked hashmap of each iteration is
   *     always larger than the last measurement of the linked hashmap of the previous iteration in
   *     lexicographic order.
   */
  public Iterator<Map<String, List<ChunkMetadata>>> getMeasurementChunkMetadataListMapIterator(
      MetadataIndexNode firstMeasurementMetadataIndexNodeOfDevice) throws IOException {
    Queue<Pair<Long, Long>> queue = new LinkedList<>();
    collectEachLeafMeasurementNodeOffsetRange(firstMeasurementMetadataIndexNodeOfDevice, queue);

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
      MetadataIndexNode metadataIndexNode, Queue<Pair<Long, Long>> queue) throws IOException {
    try {
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
        collectEachLeafMeasurementNodeOffsetRange(
            deserializeConfig.measurementMetadataIndexNodeBufferDeserializer.deserialize(
                readData(startOffset, endOffset), deserializeConfig),
            queue);
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error(
          "Error occurred while collecting offset ranges of measurement nodes of file {}", file);
      throw e;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TsFileSequenceReader reader = (TsFileSequenceReader) o;
    return file.equals(reader.file);
  }

  @Override
  public int hashCode() {
    return Objects.hash(file);
  }

  public DeserializeConfig getDeserializeContext() {
    return deserializeConfig;
  }
}
