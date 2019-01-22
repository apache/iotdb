/**
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure.
 */
public class TsFileMetaData {

  private Map<String, TsDeviceMetadataIndex> deviceIndexMap = new HashMap<>();

  /**
   * TSFile schema for this file. This schema contains metadata for all the time series.
   */
  private Map<String, MeasurementSchema> measurementSchema = new HashMap<>();

  /**
   * Version of this file.
   */
  private int currentVersion;

  /**
   * String for application that wrote this file. This should be in the format [Application] version
   * [App Version](build [App Build Hash]). e.g. impala version 1.0 (build SHA-1_hash_code)
   */
  private String createdBy;

  public TsFileMetaData() {
  }

  /**
   * construct function for TsFileMetaData.
   *
   * @param measurementSchema - time series info list
   * @param currentVersion - current version
   */
  public TsFileMetaData(Map<String, TsDeviceMetadataIndex> deviceMap,
      Map<String, MeasurementSchema> measurementSchema, int currentVersion) {
    this.deviceIndexMap = deviceMap;
    this.measurementSchema = measurementSchema;
    this.currentVersion = currentVersion;
  }

  /**
   * deserialize data from the inputStream.
   *
   * @param inputStream -input stream use to deserialize
   * @return -a instance of TsFileMetaData
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

    fileMetaData.currentVersion = ReadWriteIOUtils.readInt(inputStream);

    if (ReadWriteIOUtils.readIsNull(inputStream)) {
      fileMetaData.createdBy = ReadWriteIOUtils.readString(inputStream);
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

    fileMetaData.currentVersion = ReadWriteIOUtils.readInt(buffer);

    if (ReadWriteIOUtils.readIsNull(buffer)) {
      fileMetaData.createdBy = ReadWriteIOUtils.readString(buffer);
    }

    return fileMetaData;
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
        + measurementSchema
        + ", currentVersion=" + currentVersion + ", createdBy='" + createdBy + '\'' + '}';
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
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

  public TsDeviceMetadataIndex getDeviceMetadataIndex(String deltaObjUid) {
    return this.deviceIndexMap.get(deltaObjUid);
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

    byteLen += ReadWriteIOUtils.write(currentVersion, outputStream);

    byteLen += ReadWriteIOUtils.writeIsNull(createdBy, outputStream);
    if (createdBy != null) {
      byteLen += ReadWriteIOUtils.write(createdBy, outputStream);
    }

    return byteLen;
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

    byteLen += ReadWriteIOUtils.write(currentVersion, buffer);

    byteLen += ReadWriteIOUtils.writeIsNull(createdBy, buffer);
    if (createdBy != null) {
      byteLen += ReadWriteIOUtils.write(createdBy, buffer);
    }

    return byteLen;
  }

}
