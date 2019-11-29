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

package org.apache.iotdb.tsfile.file.metadataV2;



import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.TreeMap;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure.
 */
public class TsFileMetadataV2 {

  // fields below are IoTDB extensions and they does not affect TsFile's stand-alone functionality
  private int totalChunkNum;
  // invalid means a chunk has been rewritten by merge and the chunk's data is in
  // another new chunk
  private int invalidChunkNum;

  // bloom filter
  private BloomFilter bloomFilter;

  private long[] seriesMetadataIndex;

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetadataV2 deserializeFrom(ByteBuffer buffer, boolean isOldVersion)
      throws IOException {
    TsFileMetadataV2 fileMetaData = new TsFileMetadataV2();



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

    byteLen += ReadWriteIOUtils.write(totalChunkNum, outputStream);
    byteLen += ReadWriteIOUtils.write(invalidChunkNum, outputStream);

    return byteLen;
  }

  /**
   * use the given outputStream to serialize bloom filter.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeBloomFilter(OutputStream outputStream,
      List<ChunkMetaData> chunkGroupMetaDataList)
      throws IOException {
    int byteLen = 0;
    BloomFilter filter = buildBloomFilter(chunkGroupMetaDataList);

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
  private BloomFilter buildBloomFilter(List<ChunkMetaData> chunkGroupMetaDataList) {
    List<String> paths = getAllPath(chunkGroupMetaDataList);
    BloomFilter bloomFilter = BloomFilter
        .getEmptyBloomFilter(TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
            paths.size());
    for (String path : paths) {
      bloomFilter.add(path);
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

}
