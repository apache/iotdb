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
package org.apache.iotdb.tsfile.v1.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v1.file.header.HeaderUtils;
import org.apache.iotdb.tsfile.v1.file.metadata.OldChunkGroupMetaData;
import org.apache.iotdb.tsfile.v1.file.metadata.OldChunkMetadata;
import org.apache.iotdb.tsfile.v1.file.metadata.OldTsDeviceMetadata;
import org.apache.iotdb.tsfile.v1.file.metadata.OldTsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.v1.file.metadata.OldTsFileMetadata;

public class TsFileSequenceReaderForOldFile extends TsFileSequenceReader {

  private long fileMetadataPos;
  private int fileMetadataSize;
  private OldTsFileMetadata oldTsFileMetaData;
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
  public TsFileSequenceReaderForOldFile(String file) throws IOException {
    super(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReaderForOldFile(String file, boolean loadMetadataSize) throws IOException {
    super(file, loadMetadataSize);
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.getBytes().length
   * + TSFileConfig.NUMBER_VERSION.getBytes().length bytes of the file for preparing reading real
   * data.
   *
   * @param input given input
   */
  public TsFileSequenceReaderForOldFile(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReaderForOldFile(TsFileInput input, boolean loadMetadataSize) throws IOException {
    super(input, loadMetadataSize);
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
  public TsFileSequenceReaderForOldFile(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    super(input, fileMetadataPos, fileMetadataSize);
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  @Override
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
  
  public OldTsFileMetadata readOldFileMetadata() throws IOException {
    if (oldTsFileMetaData == null) {
      oldTsFileMetaData = OldTsFileMetadata.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
    }
    return oldTsFileMetaData;
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  @Override
  public BloomFilter readBloomFilter() throws IOException {
    readOldFileMetadata();
    return oldTsFileMetaData.getBloomFilter();
  }

  /**
   * this function reads measurements and TimeseriesMetaDatas in given device Thread Safe
   *
   * @param device name
   * @return the map measurementId -> TimeseriesMetaData in one device
   * @throws IOException io error
   */
  @Override
  public Map<String, TimeseriesMetadata> readDeviceMetadata(String device) throws IOException {
    if (!cacheDeviceMetadata) {
      return constructDeviceMetadataFromOldFile(device);
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
      readOldFileMetadata();
      if (!oldTsFileMetaData.containsDevice(device)) {
        return new HashMap<>();
      }
      Map<String, TimeseriesMetadata> deviceMetadata = constructDeviceMetadataFromOldFile(device);
      cachedDeviceMetadata.put(device, deviceMetadata);
      return deviceMetadata;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  /**
   * for old TsFile
   * this function does not modify the position of the file reader.
   */
  private Map<String, TimeseriesMetadata> constructDeviceMetadataFromOldFile(String device)
      throws IOException {
    Map<String, TimeseriesMetadata> newDeviceMetadata = new HashMap<>();
    readOldFileMetadata();
    OldTsDeviceMetadataIndex index = oldTsFileMetaData.getDeviceMetadataIndex(device);
    // read TsDeviceMetadata from file
    OldTsDeviceMetadata tsDeviceMetadata = readOldTsDeviceMetaData(index);
    if (tsDeviceMetadata == null) {
      return newDeviceMetadata;
    }

    // get all ChunkMetaData of this path included in all ChunkGroups of this device
    for (OldChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroupMetaDataList()) {
      List<OldChunkMetadata> chunkMetaDataListInOneChunkGroup = chunkGroupMetaData
          .getChunkMetaDataList();
      for (OldChunkMetadata oldChunkMetadata : chunkMetaDataListInOneChunkGroup) {
        oldChunkMetadata.setVersion(chunkGroupMetaData.getVersion());
        newDeviceMetadata.computeIfAbsent(oldChunkMetadata.getMeasurementUid(), key -> new TimeseriesMetadata())
        .addChunkMetadata(oldChunkMetadata.upgradeToChunkMetadata());
      }
    }
    return newDeviceMetadata;
  }

  /**
   * for old TsFile
   * this function does not modify the position of the file reader.
   */
  private OldTsDeviceMetadata readOldTsDeviceMetaData(OldTsDeviceMetadataIndex index) 
      throws IOException {
    if (index == null) {
      return null;
    }
    return OldTsDeviceMetadata.deserializeFrom(readData(index.getOffset(), index.getLen()));
  }

  public TimeseriesMetadata readTimeseriesMetadata(Path path) throws IOException {
    return getTimeseriesMetadataFromOldFile(path);
  }

  public List<TimeseriesMetadata> readTimeseriesMetadata(String device, Set<String> measurements)
      throws IOException {
    return getTimeseriesMetadataFromOldFile(device, measurements);
  }

  /*
   *  for 0.9.x TsFile
   */
  private TimeseriesMetadata getTimeseriesMetadataFromOldFile(Path path) throws IOException {
    Map<String, TimeseriesMetadata> deviceMetadata = 
        constructDeviceMetadataFromOldFile(path.getDevice());
    return deviceMetadata.get(path.getMeasurement());
  }

  /*
   *  for 0.9.x TsFile
   */
  private List<TimeseriesMetadata> getTimeseriesMetadataFromOldFile(String device, Set<String> measurements)
      throws IOException {
    Map<String, TimeseriesMetadata> deviceMetadata = 
        constructDeviceMetadataFromOldFile(device);
    List<TimeseriesMetadata> resultTimeseriesMetadataList = new ArrayList<>();
    for (Entry<String, TimeseriesMetadata> entry : deviceMetadata.entrySet()) {
      if (measurements.contains(entry.getKey())) {
        resultTimeseriesMetadataList.add(entry.getValue());
      }
    }
    return resultTimeseriesMetadataList;
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br> This
   * method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  @Override
  public ChunkHeader readChunkHeader() throws IOException {
    return HeaderUtils.deserializeOldChunkHeader(tsFileInput.wrapAsInputStream(), true);
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
    return HeaderUtils.deserializeOldChunkHeader(tsFileInput, position, chunkHeaderSize, markerRead);
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
  @Override
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
    int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());
    chunkHeadSize += Long.BYTES; // maxTombstoneTime
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize, false);
    ByteBuffer buffer = readChunk(metaData.getOffsetOfChunkHeader() + header.getSerializedSize(),
        header.getDataSize());
    return new Chunk(header, buffer, metaData.getDeletedAt());
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  @Override
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return HeaderUtils.deserializeOldPageHeader(tsFileInput.wrapAsInputStream(), type);
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
   * get ChunkMetaDatas of given path
   *
   * @param path timeseries path
   * @return List of ChunkMetaData
   */
  @Override
  public List<ChunkMetadata> getChunkMetadataList(Path path) throws IOException {
    return getChunkMetadataListFromOldFile(path);
  }

  /**
   *  For old TsFile
   */
  private List<ChunkMetadata> getChunkMetadataListFromOldFile(Path path) throws IOException {
    readOldFileMetadata();
    if (!oldTsFileMetaData.containsDevice(path.getDevice())) {
      return new ArrayList<>();
    }

    // get the index information of TsDeviceMetadata
    OldTsDeviceMetadataIndex index = oldTsFileMetaData.getDeviceMetadataIndex(path.getDevice());

    // read TsDeviceMetadata from file
    OldTsDeviceMetadata tsDeviceMetadata = readOldTsDeviceMetaData(index);
    if (tsDeviceMetadata == null) {
      return new ArrayList<>();
    }

    // get all ChunkMetaData of this path included in all ChunkGroups of this device
    List<OldChunkMetadata> oldChunkMetaDataList = new ArrayList<>();
    for (OldChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroupMetaDataList()) {
      List<OldChunkMetadata> chunkMetaDataListInOneChunkGroup = chunkGroupMetaData
          .getChunkMetaDataList();
      for (OldChunkMetadata chunkMetaData : chunkMetaDataListInOneChunkGroup) {
        if (path.getMeasurement().equals(chunkMetaData.getMeasurementUid())) {
          chunkMetaData.setVersion(chunkGroupMetaData.getVersion());
          oldChunkMetaDataList.add(chunkMetaData);
        }
      }
    }
    oldChunkMetaDataList.sort(Comparator.comparingLong(OldChunkMetadata::getStartTime));
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    for (OldChunkMetadata oldChunkMetaData : oldChunkMetaDataList) {
      chunkMetadataList.add(oldChunkMetaData.upgradeToChunkMetadata());
    }
    return chunkMetadataList;
  }
}
