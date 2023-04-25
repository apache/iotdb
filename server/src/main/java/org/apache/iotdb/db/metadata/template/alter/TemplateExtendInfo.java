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
import java.util.List;

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
