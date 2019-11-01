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

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure.
 */
public class TsFileMetaData {

  private Map<String, TsDeviceMetadataIndex> deviceIndexMap = new HashMap<>();

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

  public TsFileMetaData() {
    //do nothing
  }

  /**
   * construct function for TsFileMetaData.
   *
   * @param measurementSchema - time series info list
   */
  public TsFileMetaData(Map<String, TsDeviceMetadataIndex> deviceMap,
      Map<String, MeasurementSchema> measurementSchema) {
    this.deviceIndexMap = deviceMap;
    this.measurementSchema = measurementSchema;
  }

  /**
   * deserialize data from the inputStream.
   *
   * @param inputStream input stream used to deserialize
   * @return an instance of TsFileMetaData
   */
  public static TsFileMetaData deserializeFrom(InputStream inputStream) throws IOException {
    TsFileMetaData fileMetaData = new TsFileMetaData();

    int size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      Map<String, TsDeviceMetadataIndex> deviceMap = new HashMap<>();
      String key;
      TsDeviceMetadataIndex value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(inputStream);
        value = TsDeviceMetadataIndex.deserializeFrom(inputStream);
        deviceMap.put(key, value);
      }
      fileMetaData.deviceIndexMap = deviceMap;
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      fileMetaData.measurementSchema = new HashMap<>();
      String key;
      MeasurementSchema value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(inputStream);
        value = MeasurementSchema.deserializeFrom(inputStream);
        fileMetaData.measurementSchema.put(key, value);
      }
    }

    if (ReadWriteIOUtils.readIsNull(inputStream)) {
      fileMetaData.createdBy = ReadWriteIOUtils.readString(inputStream);
    }
    fileMetaData.totalChunkNum = ReadWriteIOUtils.readInt(inputStream);
    fileMetaData.invalidChunkNum = ReadWriteIOUtils.readInt(inputStream);
    // read bloom filter
    if (!ReadWriteIOUtils.checkIfMagicString(inputStream)) {
      byte[] bytes = ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream);
      int filterSize = ReadWriteIOUtils.readInt(inputStream);
      int hashFunctionSize = ReadWriteIOUtils.readInt(inputStream);
      fileMetaData.bloomFilter = BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
    }

    return fileMetaData;
  }

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
    TsFileMetaData fileMetaData = new TsFileMetaData();

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      Map<String, TsDeviceMetadataIndex> deviceMap = new HashMap<>();
      String key;
      TsDeviceMetadataIndex value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = TsDeviceMetadataIndex.deserializeFrom(buffer);
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

  /**
   * add time series metadata to list. THREAD NOT SAFE
   *
   * @param measurementSchema series metadata to add
   */
  public void addMeasurementSchema(MeasurementSchema measurementSchema) {
    this.measurementSchema.put(measurementSchema.getMeasurementId(), measurementSchema);
  }

  @Override
  public String toString() {
    return "TsFileMetaData{" + "deviceIndexMap=" + deviceIndexMap + ", measurementSchema="
        + measurementSchema + ", createdBy='" + createdBy + '\'' + '}';
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public Map<String, TsDeviceMetadataIndex> getDeviceMap() {
    return deviceIndexMap;
  }

  public void setDeviceMap(Map<String, TsDeviceMetadataIndex> deviceMap) {
    this.deviceIndexMap = deviceMap;
  }

  public boolean containsDevice(String deltaObjUid) {
    return this.deviceIndexMap.containsKey(deltaObjUid);
  }

  public TsDeviceMetadataIndex getDeviceMetadataIndex(String deviceUid) {
    return this.deviceIndexMap.get(deviceUid);
  }

  public boolean containsMeasurement(String measurement) {
    return measurementSchema.containsKey(measurement);
  }

  /**
   * return the type of the measurement.
   *
   * @param measurement -measurement
   * @return -type of the measurement
   */
  public TSDataType getType(String measurement) {
    if (containsMeasurement(measurement)) {
      return measurementSchema.get(measurement).getType();
    } else {
      return null;
    }
  }

  public Map<String, MeasurementSchema> getMeasurementSchema() {
    return measurementSchema;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(deviceIndexMap.size(), outputStream);
    for (Map.Entry<String, TsDeviceMetadataIndex> entry : deviceIndexMap.entrySet()) {
      byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
      byteLen += entry.getValue().serializeTo(outputStream);
    }

    byteLen += ReadWriteIOUtils.write(measurementSchema.size(), outputStream);
    for (Map.Entry<String, MeasurementSchema> entry : measurementSchema.entrySet()) {
      byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
      byteLen += entry.getValue().serializeTo(outputStream);
    }

    byteLen += ReadWriteIOUtils.writeIsNull(createdBy, outputStream);
    if (createdBy != null) {
      byteLen += ReadWriteIOUtils.write(createdBy, outputStream);
    }

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
      List<ChunkGroupMetaData> chunkGroupMetaDataList)
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
   * get all path in this tsfile
   *
   * @return all path in set
   */
  private List<String> getAllPath(List<ChunkGroupMetaData> chunkGroupMetaDataList) {
    List<String> res = new ArrayList<>();
    for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
      String deviceId = chunkGroupMetaData.getDeviceID();
      for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
        res.add(deviceId + PATH_SEPARATOR + chunkMetaData.getMeasurementUid());
      }
    }

    return res;
  }

  /**
   * build bloom filter
   *
   * @return bloom filter
   */
  private BloomFilter buildBloomFilter(List<ChunkGroupMetaData> chunkGroupMetaDataList) {
    List<String> paths = getAllPath(chunkGroupMetaDataList);
    BloomFilter bloomFilter = BloomFilter
        .getEmptyBloomFilter(TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
            paths.size());
    for (String path : paths) {
      bloomFilter.add(path);
    }
    return bloomFilter;
  }


  /**
   * use the given buffer to serialize.
   *
   * @param buffer -buffer to determine byte length
   * @return -byte length
   */
  public int serializeTo(ByteBuffer buffer) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(deviceIndexMap.size(), buffer);
    for (Map.Entry<String, TsDeviceMetadataIndex> entry : deviceIndexMap.entrySet()) {
      byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
      byteLen += entry.getValue().serializeTo(buffer);
    }

    byteLen += ReadWriteIOUtils.write(measurementSchema.size(), buffer);
    for (Map.Entry<String, MeasurementSchema> entry : measurementSchema.entrySet()) {
      byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
      byteLen += entry.getValue().serializeTo(buffer);
    }

    byteLen += ReadWriteIOUtils.writeIsNull(createdBy, buffer);
    if (createdBy != null) {
      byteLen += ReadWriteIOUtils.write(createdBy, buffer);
    }

    byteLen += ReadWriteIOUtils.write(totalChunkNum, buffer);
    byteLen += ReadWriteIOUtils.write(invalidChunkNum, buffer);

    return byteLen;
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

  public List<MeasurementSchema> getMeasurementSchemaList() {
    return new ArrayList<MeasurementSchema>(measurementSchema.values());
  }
}