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
package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Metadata of one chunk. */
public class ChunkMetadata implements IChunkMetadata {

  private String measurementUid; // do not need to be serialized

  /**
   * Byte offset of the corresponding data in the file Notice: include the chunk header and marker.
   */
  private long offsetOfChunkHeader;

  private TSDataType tsDataType; // do not need to be serialized

  /**
   * version is used to define the order of operations(insertion, deletion, update). version is set
   * according to its belonging ChunkGroup only when being queried, so it is not persisted.
   */
  private long version;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private boolean modified;

  /** ChunkLoader of metadata, used to create ChunkReaderWrap */
  private IChunkLoader chunkLoader;

  private Statistics<? extends Serializable> statistics;

  private final boolean isFromOldTsFile = false;

  private static final int CHUNK_METADATA_FIXED_RAM_SIZE = 93;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;
  private boolean isClosed;
  private String filePath;

  // 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
  private byte mask;

  // used for ChunkCache, Eg:"root.sg1/0/0"
  private String tsFilePrefixPath;
  // high 32 bit is compaction level, low 32 bit is merge count
  private long compactionVersion;

  /**
   * 1 means this chunk has more than one page, so each page has its own page statistic. 5 means
   * this chunk has only one page, and this page has no page statistic.
   *
   * <p>if the 8th bit of this byte is 1 means this chunk is a time chunk of one vector if the 7th
   * bit of this byte is 1 means this chunk is a value chunk of one vector
   */
  private byte chunkType;

  private int dataSize;
  private CompressionType compressionType;
  private TSEncoding encodingType;

  private int numOfPages; // do not need to be serialized
  private int serializedSize; // do not need to be serialized

  public ChunkMetadata() {}

  // FIXME
  public ChunkMetadata(
      String measurementUid,
      TSDataType tsDataType,
      long fileOffset,
      Statistics<? extends Serializable> statistics) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.statistics = statistics;
    this.serializedSize = getSerializedSize(dataSize);
  }

  public ChunkMetadata(
      String measurementUid,
      TSDataType tsDataType,
      long fileOffset,
      Statistics<? extends Serializable> statistics,
      int dataSize,
      CompressionType compressionType,
      TSEncoding encoding,
      int numOfPages,
      int mask) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.statistics = statistics;
    this.chunkType =
        (byte)
            ((numOfPages <= 1 ? MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER : MetaMarker.CHUNK_HEADER)
                | (byte) mask);
    this.dataSize = dataSize;
    this.compressionType = compressionType;
    this.encodingType = encoding;
    this.numOfPages = numOfPages;
    this.serializedSize = getSerializedSize(dataSize);
  }

  @Override
  public String toString() {
    return "CHUNK_METADATA{"
        + "measurementID='"
        + measurementUid
        + '\''
        + ", version="
        + version
        + ", statistics="
        + statistics
        + ", deleteIntervalList="
        + deleteIntervalList
        + ", filePath="
        + filePath
        + ", dataSize="
        + dataSize
        + ", dataType="
        + tsDataType
        + ", compressionType="
        + compressionType
        + ", encodingType="
        + encodingType
        + ", numOfPages="
        + numOfPages
        + ", serializedSize="
        + serializedSize
        + '}';
  }

  public long getNumOfPoints() {
    return statistics.getCount();
  }

  /**
   * get offset of chunk header.
   *
   * @return Byte offset of header of this chunk (includes the marker)
   */
  @Override
  public long getOffsetOfChunkHeader() {
    return offsetOfChunkHeader;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public long getStartTime() {
    return statistics.getStartTime();
  }

  public long getEndTime() {
    return statistics.getEndTime();
  }

  public TSDataType getDataType() {
    return tsDataType;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream, boolean serializeStatistic) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, outputStream);
    if (serializeStatistic) {
      byteLen += statistics.serialize(outputStream);
    }
    byteLen += ReadWriteIOUtils.write(chunkType, outputStream);
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(dataSize, outputStream);
    return byteLen;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetadata deserializeFrom(
      ByteBuffer buffer, TimeseriesMetadata timeseriesMetadata) {
    ChunkMetadata chunkMetaData = new ChunkMetadata();

    chunkMetaData.measurementUid = timeseriesMetadata.getMeasurementId();
    chunkMetaData.tsDataType = timeseriesMetadata.getTSDataType();
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    // if the TimeSeriesMetadataType is not 0, it means it has more than one chunk
    // and each chunk's metadata has its own statistics
    if ((timeseriesMetadata.getTimeSeriesMetadataType() & 0x3F) != 0) {
      chunkMetaData.statistics = Statistics.deserialize(buffer, chunkMetaData.tsDataType);
    } else {
      // if the TimeSeriesMetadataType is 0, it means it has only one chunk
      // and that chunk's metadata has no statistic
      chunkMetaData.statistics = timeseriesMetadata.getStatistics();
    }
    chunkMetaData.chunkType = ReadWriteIOUtils.readByte(buffer);
    chunkMetaData.dataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    chunkMetaData.serializedSize = chunkMetaData.getSerializedSize(chunkMetaData.getDataSize());

    chunkMetaData.compressionType = timeseriesMetadata.getCompressionType();
    chunkMetaData.encodingType = timeseriesMetadata.getEncodingType();
    return chunkMetaData;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @return ChunkMetaData object
   */
  public static ChunkMetadata deserializeFrom(
      InputStream inputStream, TimeseriesMetadata timeseriesMetadata) throws IOException {
    ChunkMetadata chunkMetaData = new ChunkMetadata();

    chunkMetaData.measurementUid = timeseriesMetadata.getMeasurementId();
    chunkMetaData.tsDataType = timeseriesMetadata.getTSDataType();
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(inputStream);
    // if the TimeSeriesMetadataType is not 0, it means it has more than one chunk
    // and each chunk's metadata has its own statistics
    if ((timeseriesMetadata.getTimeSeriesMetadataType() & 0x3F) != 0) {
      chunkMetaData.statistics = Statistics.deserialize(inputStream, chunkMetaData.tsDataType);
    } else {
      // if the TimeSeriesMetadataType is 0, it means it has only one chunk
      // and that chunk's metadata has no statistic
      chunkMetaData.statistics = timeseriesMetadata.getStatistics();
    }
    chunkMetaData.chunkType = ReadWriteIOUtils.readByte(inputStream);
    chunkMetaData.dataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    chunkMetaData.serializedSize = chunkMetaData.getSerializedSize(chunkMetaData.getDataSize());

    chunkMetaData.compressionType = timeseriesMetadata.getCompressionType();
    chunkMetaData.encodingType = timeseriesMetadata.getEncodingType();
    return chunkMetaData;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public void setVersion(long version) {
    this.version = version;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public void insertIntoSortedDeletions(long startTime, long endTime) {
    List<TimeRange> resultInterval = new ArrayList<>();
    if (deleteIntervalList != null) {
      for (TimeRange interval : deleteIntervalList) {
        if (interval.getMax() < startTime) {
          resultInterval.add(interval);
        } else if (interval.getMin() > endTime) {
          resultInterval.add(new TimeRange(startTime, endTime));
          startTime = interval.getMin();
          endTime = interval.getMax();
        } else if (interval.getMax() >= startTime || interval.getMin() <= endTime) {
          startTime = Math.min(interval.getMin(), startTime);
          endTime = Math.max(interval.getMax(), endTime);
        }
      }
    }

    resultInterval.add(new TimeRange(startTime, endTime));
    deleteIntervalList = resultInterval;
  }

  public IChunkLoader getChunkLoader() {
    return chunkLoader;
  }

  @Override
  public boolean needSetChunkLoader() {
    return chunkLoader == null;
  }

  public void setChunkLoader(IChunkLoader chunkLoader) {
    this.chunkLoader = chunkLoader;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChunkMetadata that = (ChunkMetadata) o;
    return offsetOfChunkHeader == that.offsetOfChunkHeader
        && version == that.version
        && compactionVersion == that.compactionVersion
        && tsFilePrefixPath.equals(that.tsFilePrefixPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tsFilePrefixPath, version, compactionVersion, offsetOfChunkHeader);
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public void setModified(boolean modified) {
    this.modified = modified;
  }

  public boolean isFromOldTsFile() {
    return isFromOldTsFile;
  }

  public long calculateRamSize() {
    return CHUNK_METADATA_FIXED_RAM_SIZE
        + RamUsageEstimator.sizeOf(tsFilePrefixPath)
        + RamUsageEstimator.sizeOf(measurementUid)
        + statistics.calculateRamSize();
  }

  public static long calculateRamSize(String measurementId, TSDataType dataType) {
    return CHUNK_METADATA_FIXED_RAM_SIZE
        + RamUsageEstimator.sizeOf(measurementId)
        + Statistics.getSizeByType(dataType);
  }

  public void mergeChunkMetadata(ChunkMetadata chunkMetadata) {
    Statistics<? extends Serializable> statistics = chunkMetadata.getStatistics();
    this.statistics.mergeStatistics(statistics);
    this.dataSize += chunkMetadata.getDataSize();
    this.numOfPages += chunkMetadata.getNumOfPages();
  }

  @Override
  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  @Override
  public boolean isSeq() {
    return isSeq;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void setClosed(boolean closed) {
    isClosed = closed;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;

    Pair<String, long[]> tsFilePrefixPathAndTsFileVersionPair =
        FilePathUtils.getTsFilePrefixPathAndTsFileVersionPair(filePath);
    // set tsFilePrefixPath
    tsFilePrefixPath = tsFilePrefixPathAndTsFileVersionPair.left;
    this.version = tsFilePrefixPathAndTsFileVersionPair.right[0];
    this.compactionVersion = tsFilePrefixPathAndTsFileVersionPair.right[1];
  }

  @Override
  public byte getMask() {
    return mask;
  }

  public void setMask(byte mask) {
    this.mask = mask;
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public int getDataSize() {
    return dataSize;
  }

  public int getNumOfPages() {
    return numOfPages;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public void setDataSize(int dataSize) {
    this.dataSize = dataSize;
  }

  public byte getChunkType() {
    return chunkType;
  }

  public void setChunkType(byte chunkType) {
    this.chunkType = chunkType;
  }

  public void increasePageNums(int i) {
    numOfPages += i;
  }

  /** the exact serialized size of chunk header */
  public int getSerializedSize(int dataSize) {
    return Long.BYTES // offsetOfChunkHeader
        + statistics.getSerializedSize()
        + Byte.BYTES // chunkType
        + ReadWriteForEncodingUtils.uVarIntSize(dataSize); // dataSize
  }
}
