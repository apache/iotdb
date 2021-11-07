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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AppendTemplatePlan extends PhysicalPlan {

  String name;
  boolean isAligned;
  List<String> measurements;
  List<TSDataType> dataTypes;
  List<TSEncoding> encodings;
  List<CompressionType> compressors;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public AppendTemplatePlan() {
    super(false, OperatorType.APPEND_TEMPLATE);
  }

  public AppendTemplatePlan(
      String name,
      boolean isAligned,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors) {
    super(false, OperatorType.APPEND_TEMPLATE);
    this.name = name;
    this.isAligned = isAligned;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.APPEND_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(isAligned, buffer);

    // measurements
    ReadWriteIOUtils.write(measurements.size(), buffer);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, buffer);
    }

    // datatypes
    ReadWriteIOUtils.write(dataTypes.size(), buffer);
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType.ordinal(), buffer);
    }

    // encoding
    ReadWriteIOUtils.write(encodings.size(), buffer);
    for (TSEncoding encoding : encodings) {
      ReadWriteIOUtils.write(encoding.ordinal(), buffer);
    }

    // compressor
    ReadWriteIOUtils.write(compressors.size(), buffer);
    for (CompressionType type : compressors) {
      ReadWriteIOUtils.write(type.ordinal(), buffer);
    }

    buffer.putLong(index);
  }

  @Override
  @SuppressWarnings("Duplicates")
  public void deserialize(ByteBuffer buffer) {
    name = ReadWriteIOUtils.readString(buffer);
    isAligned = ReadWriteIOUtils.readBool(buffer);

    // measurements
    int size = ReadWriteIOUtils.readInt(buffer);
    measurements = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      measurements.add(ReadWriteIOUtils.readString(buffer));
    }

    // datatypes
    size = ReadWriteIOUtils.readInt(buffer);
    dataTypes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      dataTypes.add(TSDataType.values()[ReadWriteIOUtils.readInt(buffer)]);
    }

    // encodings
    size = ReadWriteIOUtils.readInt(buffer);
    encodings = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      encodings.add(TSEncoding.values()[ReadWriteIOUtils.readInt(buffer)]);
    }

    // compressor
    size = ReadWriteIOUtils.readInt(buffer);
    compressors = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      compressors.add(CompressionType.values()[ReadWriteIOUtils.readInt(buffer)]);
    }

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.APPEND_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, stream);
    ReadWriteIOUtils.write(isAligned, stream);

    // measurements
    ReadWriteIOUtils.write(measurements.size(), stream);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, stream);
    }

    // datatype
    ReadWriteIOUtils.write(dataTypes.size(), stream);
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType.ordinal(), stream);
    }

    // encoding
    ReadWriteIOUtils.write(encodings.size(), stream);
    for (TSEncoding encoding : encodings) {
      ReadWriteIOUtils.write(encoding.ordinal(), stream);
    }

    // compressor
    ReadWriteIOUtils.write(compressors.size(), stream);
    for (CompressionType type : compressors) {
      ReadWriteIOUtils.write(type.ordinal(), stream);
    }

    stream.writeLong(index);
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
