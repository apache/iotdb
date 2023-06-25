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

package org.apache.iotdb.db.metadata.template.alter;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TemplateExtendInfo extends TemplateAlterInfo {

  private List<String> measurements;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private List<CompressionType> compressors;

  public TemplateExtendInfo() {}

  public TemplateExtendInfo(String templateName) {
    this.templateName = templateName;
  }

  public TemplateExtendInfo(
      String templateName,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors) {
    this.templateName = templateName;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
  }

  @Override
  public String getTemplateName() {
    return templateName;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void addMeasurement(
      String measurement,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressionType) {
    if (measurements == null) {
      measurements = new ArrayList<>();
    }
    measurements.add(measurement);

    if (dataTypes == null) {
      dataTypes = new ArrayList<>();
    }
    dataTypes.add(dataType);

    if (encodings == null) {
      encodings = new ArrayList<>();
    }
    encodings.add(encoding);

    if (compressors == null) {
      compressors = new ArrayList<>();
    }
    compressors.add(compressionType);
  }

  // if there's duplicate measurements, return the first one, otherwise return null
  public String getFirstDuplicateMeasurement() {
    if (measurements != null) {
      Set<String> set = new HashSet<>();
      for (String measurement : measurements) {
        if (set.contains(measurement)) {
          return measurement;
        } else {
          set.add(measurement);
        }
      }
    }
    return null;
  }

  // deduplicate the measurements with same name, keep the first one
  public TemplateExtendInfo deduplicate() {
    if (measurements == null || measurements.isEmpty()) {
      return new TemplateExtendInfo();
    }
    Set<String> set = new HashSet<>();
    TemplateExtendInfo result = new TemplateExtendInfo();
    for (int i = 0; i < measurements.size(); i++) {
      if (set.contains(measurements.get(i))) {
        continue;
      }
      set.add(measurements.get(i));
      result.addMeasurement(
          measurements.get(i), dataTypes.get(i), encodings.get(i), compressors.get(i));
    }
    return result;
  }

  /**
   * Updates this to be the difference of set between this and targetMeasurementSet. Returns the
   * intersection.
   *
   * @param targetMeasurementSet The set to compare with measurement set of this.
   * @return A list of elements representing the intersection between measurement set of this and
   *     the targetMeasurementSet.
   */
  public List<String> updateAsDifferenceAndGetIntersection(Set<String> targetMeasurementSet) {
    List<String> removedMeasurements = new ArrayList<>();

    List<String> updatedMeasurements = new ArrayList<>();
    List<TSDataType> updatedDataTypes = new ArrayList<>();
    List<TSEncoding> updatedEncodings = this.encodings == null ? null : new ArrayList<>();
    List<CompressionType> updatedCompressors = this.compressors == null ? null : new ArrayList<>();

    for (int i = 0; i < this.measurements.size(); i++) {
      if (targetMeasurementSet.contains(this.measurements.get(i))) {
        removedMeasurements.add(this.measurements.get(i));
        continue;
      }
      updatedMeasurements.add(this.measurements.get(i));
      updatedDataTypes.add(this.dataTypes.get(i));
      if (this.encodings != null) {
        updatedEncodings.add(this.encodings.get(i));
      }
      if (this.compressors != null) {
        updatedCompressors.add(this.compressors.get(i));
      }
    }

    this.measurements = updatedMeasurements;
    this.dataTypes = updatedDataTypes;
    this.encodings = updatedEncodings;
    this.compressors = updatedCompressors;

    return removedMeasurements;
  }

  public boolean isEmpty() {
    return measurements == null || measurements.isEmpty();
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    ReadWriteIOUtils.write(measurements.size(), outputStream);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, outputStream);
    }
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType, outputStream);
    }

    if (encodings == null || encodings.isEmpty()) {
      ReadWriteIOUtils.write((byte) 0, outputStream);
    } else {
      ReadWriteIOUtils.write((byte) 1, outputStream);
      for (TSEncoding encoding : encodings) {
        ReadWriteIOUtils.write(encoding, outputStream);
      }
    }

    if (compressors == null || compressors.isEmpty()) {
      ReadWriteIOUtils.write((byte) 0, outputStream);
    } else {
      ReadWriteIOUtils.write((byte) 1, outputStream);
      for (CompressionType compressionType : compressors) {
        ReadWriteIOUtils.write(compressionType, outputStream);
      }
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    super.deserialize(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    this.measurements = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      measurements.add(ReadWriteIOUtils.readString(buffer));
    }

    this.dataTypes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      dataTypes.add(ReadWriteIOUtils.readDataType(buffer));
    }

    if (ReadWriteIOUtils.readByte(buffer) == 1) {
      this.encodings = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        encodings.add(ReadWriteIOUtils.readEncoding(buffer));
      }
    }

    if (ReadWriteIOUtils.readByte(buffer) == 1) {
      this.compressors = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        compressors.add(ReadWriteIOUtils.readCompressionType(buffer));
      }
    }
  }
}
