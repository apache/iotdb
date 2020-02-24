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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure.
 */
public class TsFileMetaData {

  // fields below are IoTDB extensions and they does not affect TsFile's
  // stand-alone functionality
  private int totalChunkNum;
  // invalid means a chunk has been rewritten by merge and the chunk's data is in
  // another new chunk
  private int invalidChunkNum;

  // bloom filter
  private BloomFilter bloomFilter;

  private Map<String, Pair<Long, Integer>> deviceMetaDataMap;

  public TsFileMetaData() {
  }

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
    TsFileMetaData fileMetaData = new TsFileMetaData();
    int deviceNum = ReadWriteIOUtils.readInt(buffer);
    if (deviceNum > 0) {
      Map<String, Pair<Long, Integer>> deviceMetaDataMap = new HashMap<>();
      for (int i = 0; i < deviceNum; i++) {
        String deviceId = ReadWriteIOUtils.readString(buffer);
        long offset = ReadWriteIOUtils.readLong(buffer);
        int length = ReadWriteIOUtils.readInt(buffer);
        deviceMetaDataMap.put(deviceId, new Pair<>(offset, length));
      }
      fileMetaData.setDeviceMetaDataMap(deviceMetaDataMap);
    }

    fileMetaData.totalChunkNum = ReadWriteIOUtils.readInt(buffer);
    fileMetaData.invalidChunkNum = ReadWriteIOUtils.readInt(buffer);
    
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
    if (deviceMetaDataMap != null) {
      byteLen += ReadWriteIOUtils.write(deviceMetaDataMap.size(), outputStream);
      for (Map.Entry<String, Pair<Long, Integer>> entry : deviceMetaDataMap.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += ReadWriteIOUtils.write(entry.getValue().left, outputStream);
        byteLen += ReadWriteIOUtils.write(entry.getValue().right, outputStream);
      }
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }
    byteLen += ReadWriteIOUtils.write(totalChunkNum, outputStream);
    byteLen += ReadWriteIOUtils.write(invalidChunkNum, outputStream);

    return byteLen;
  }

  /**
   * use the given outputStream to serialize bloom filter.
   *
   * @param outputStream      -output stream to determine byte length
   * @param schemaDescriptors
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
   * @param schemaDescriptors
   * @return bloom filter
   */
  private BloomFilter buildBloomFilter(Set<Path> paths) {
    BloomFilter bloomFilter = BloomFilter
        .getEmptyBloomFilter(TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
            paths.size());
    for (Path path : paths) {
      bloomFilter.add(path.toString());
    }
    return bloomFilter;
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

  public Map<String, Pair<Long, Integer>> getDeviceMetaDataMap() {
    return deviceMetaDataMap;
  }

  public void setDeviceMetaDataMap(Map<String, Pair<Long, Integer>> deviceMetaDataMap) {
    this.deviceMetaDataMap = deviceMetaDataMap;
  }

}
