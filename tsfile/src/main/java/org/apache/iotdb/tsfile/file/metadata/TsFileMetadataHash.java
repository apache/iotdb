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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

/** TSFileMetaData collects all metadata info and saves in its data structure. */
public class TsFileMetadataHash {

  // bloom filter
  private BloomFilter bloomFilter;

  // List of <name, offset, childMetadataIndexType>
  private MetadataIndexBucket[] metadataIndexBuckets;

  private int bucketSize;

  private int bucketNum;

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetadataHash deserializeFrom(ByteBuffer buffer) {
    TsFileMetadataHash fileMetaData = new TsFileMetadataHash();

    // metadataIndex
    fileMetaData.bucketNum = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    fileMetaData.bucketSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);

    // read bloom filter
    if (buffer.hasRemaining()) {
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      fileMetaData.bloomFilter = BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
    }

    return fileMetaData;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  public int serializeBuckets(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    if (metadataIndexBuckets.length > 0) {
      for (MetadataIndexBucket bucket : metadataIndexBuckets) {
        ByteBuffer buffer = ByteBuffer.allocate(bucketSize);
        byteLen += bucket.serializeTo(buffer);
        outputStream.write(buffer.array());
      }
    }

    return byteLen;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = ReadWriteForEncodingUtils.writeUnsignedVarInt(bucketNum, outputStream);
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bucketSize, outputStream);

    return byteLen;
  }

  /**
   * use the given outputStream to serialize bloom filter.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeBloomFilter(OutputStream outputStream, Set<Path> paths) throws IOException {
    int byteLen = 0;
    BloomFilter filter = buildBloomFilter(paths);

    byte[] bytes = filter.serialize();
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    outputStream.write(bytes);
    byteLen += bytes.length;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getSize(), outputStream);
    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getHashFunctionSize(), outputStream);
    return byteLen;
  }

  /**
   * build bloom filter
   *
   * @return bloom filter
   */
  private BloomFilter buildBloomFilter(Set<Path> paths) {
    BloomFilter filter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), paths.size());
    for (Path path : paths) {
      filter.add(path.toString());
    }
    return filter;
  }

  public MetadataIndexBucket[] getMetadataIndexBuckets() {
    return metadataIndexBuckets;
  }

  public void setMetadataIndexBuckets(MetadataIndexBucket[] metadataIndexBuckets) {
    this.metadataIndexBuckets = metadataIndexBuckets;
  }

  public int getBucketSize() {
    return bucketSize;
  }

  public void setBucketSize(int bucketSize) {
    this.bucketSize = bucketSize;
  }

  public int getBucketNum() {
    return bucketNum;
  }

  public void setBucketNum(int bucketNum) {
    this.bucketNum = bucketNum;
  }
}
