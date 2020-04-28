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

package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure.
 */
public class TsFileMetadata {

  // fields below are IoTDB extensions and they does not affect TsFile's
  // stand-alone functionality
  private int totalChunkNum;
  // invalid means a chunk has been rewritten by merge and the chunk's data is in
  // another new chunk
  private int invalidChunkNum;

  // bloom filter
  private BloomFilter bloomFilter;

  // List of <name, offset, childMetadataIndexType>
  private MetadataIndexNode metadataIndex;

  // offset -> version
  private List<Pair<Long, Long>> versionInfo;

  // offset of MetaMarker.SEPARATOR
  private long metaOffset;

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetadata deserializeFrom(ByteBuffer buffer) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    // metadataIndex
    fileMetaData.metadataIndex = MetadataIndexNode.deserializeFrom(buffer);
    fileMetaData.totalChunkNum = ReadWriteIOUtils.readInt(buffer);
    fileMetaData.invalidChunkNum = ReadWriteIOUtils.readInt(buffer);

    // versionInfo
    List<Pair<Long, Long>> versionInfo = new ArrayList<>();
    int versionSize = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < versionSize; i++) {
      long versionPos = ReadWriteIOUtils.readLong(buffer);
      long version = ReadWriteIOUtils.readLong(buffer);
      versionInfo.add(new Pair<>(versionPos, version));
    }
    fileMetaData.setVersionInfo(versionInfo);

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer).array();
      int filterSize = ReadWriteIOUtils.readInt(buffer);
      int hashFunctionSize = ReadWriteIOUtils.readInt(buffer);
      fileMetaData.bloomFilter = BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
    }

    return fileMetaData;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    // metadataIndex
    if (metadataIndex != null) {
      byteLen += metadataIndex.serializeTo(outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }

    // totalChunkNum, invalidChunkNum
    byteLen += ReadWriteIOUtils.write(totalChunkNum, outputStream);
    byteLen += ReadWriteIOUtils.write(invalidChunkNum, outputStream);

    // versionInfo
    byteLen += ReadWriteIOUtils.write(versionInfo.size(), outputStream);
    for (Pair<Long, Long> versionPair : versionInfo) {
      byteLen += ReadWriteIOUtils.write(versionPair.left, outputStream);
      byteLen += ReadWriteIOUtils.write(versionPair.right, outputStream);
    }

    // metaOffset
    byteLen += ReadWriteIOUtils.write(metaOffset, outputStream);

    return byteLen;
  }

  /**
   * use the given outputStream to serialize bloom filter.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeBloomFilter(OutputStream outputStream, Set<Path> paths)
      throws IOException {
    int byteLen = 0;
    BloomFilter filter = buildBloomFilter(paths);

    byte[] bytes = filter.serialize();
    byteLen += ReadWriteIOUtils.write(bytes.length, outputStream);
    outputStream.write(bytes);
    byteLen += bytes.length;
    byteLen += ReadWriteIOUtils.write(filter.getSize(), outputStream);
    byteLen += ReadWriteIOUtils.write(filter.getHashFunctionSize(), outputStream);
    return byteLen;
  }

  /**
   * build bloom filter
   *
   * @return bloom filter
   */
  private BloomFilter buildBloomFilter(Set<Path> paths) {
    BloomFilter filter = BloomFilter
        .getEmptyBloomFilter(TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
            paths.size());
    for (Path path : paths) {
      filter.add(path.toString());
    }
    return filter;
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  public void setTotalChunkNum(int totalChunkNum) {
    this.totalChunkNum = totalChunkNum;
  }

  public int getInvalidChunkNum() {
    return invalidChunkNum;
  }

  public void setInvalidChunkNum(int invalidChunkNum) {
    this.invalidChunkNum = invalidChunkNum;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public void setMetaOffset(long metaOffset) {
    this.metaOffset = metaOffset;
  }

  public MetadataIndexNode getMetadataIndex() {
    return metadataIndex;
  }

  public void setMetadataIndex(MetadataIndexNode metadataIndex) {
    this.metadataIndex = metadataIndex;
  }

  public void setVersionInfo(List<Pair<Long, Long>> versionInfo) {
    this.versionInfo = versionInfo;
  }

  public List<Pair<Long, Long>> getVersionInfo() {
    return versionInfo;
  }
}
