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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure.
 */
public class OldTsFileMetadata {

  private Map<String, OldTsDeviceMetadataIndex> deviceIndexMap = new HashMap<>();

  /**
   * TSFile schema for this file. This schema contains metadata for all the measurements.
   */
  private Map<String, MeasurementSchema> measurementSchema = new HashMap<>();

  /**
   * String for application that wrote this file. This should be in the format [Application] version
   * [App Version](build [App Build Hash]). e.g. impala version 1.0 (build SHA-1_hash_code)
   */
  private String createdBy;

  // fields below are IoTDB extensions and they does not affect TsFile's stand-alone functionality
  private int totalChunkNum;
  // invalid means a chunk has been rewritten by merge and the chunk's data is in
  // another new chunk
  private int invalidChunkNum;

  // bloom filter
  private BloomFilter bloomFilter;

  public OldTsFileMetadata() {
    //do nothing
  }

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static OldTsFileMetadata deserializeFrom(ByteBuffer buffer) {
    OldTsFileMetadata fileMetaData = new OldTsFileMetadata();

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      Map<String, OldTsDeviceMetadataIndex> deviceMap = new HashMap<>();
      String key;
      OldTsDeviceMetadataIndex value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = OldTsDeviceMetadataIndex.deserializeFrom(buffer);
        deviceMap.put(key, value);
      }
      fileMetaData.deviceIndexMap = deviceMap;
    }

    size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      fileMetaData.measurementSchema = new HashMap<>();
      String key;
      MeasurementSchema value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = MeasurementSchema.deserializeFrom(buffer);
        fileMetaData.measurementSchema.put(key, value);
      }
    }

    if (ReadWriteIOUtils.readIsNull(buffer)) {
      fileMetaData.createdBy = ReadWriteIOUtils.readString(buffer);
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

  public Map<String, OldTsDeviceMetadataIndex> getDeviceMap() {
    return deviceIndexMap;
  }

  public boolean containsDevice(String deltaObjUid) {
    return this.deviceIndexMap.containsKey(deltaObjUid);
  }

  public OldTsDeviceMetadataIndex getDeviceMetadataIndex(String deviceUid) {
    return this.deviceIndexMap.get(deviceUid);
  }

}
