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

public class CreateTemplatePlan extends PhysicalPlan {

  String name;
  List<String> schemaNames;
  List<List<String>> measurements;
  List<List<TSDataType>> dataTypes;
  List<List<TSEncoding>> encodings;
  List<CompressionType> compressors;

  public List<String> getSchemaNames() {
    return schemaNames;
  }

  public void setSchemaNames(List<String> schemaNames) {
    this.schemaNames = schemaNames;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<List<String>> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<List<String>> measurements) {
    this.measurements = measurements;
  }

  public List<List<TSDataType>> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<List<TSDataType>> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<List<TSEncoding>> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<List<TSEncoding>> encodings) {
    this.encodings = encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public CreateTemplatePlan() {
    super(false, OperatorType.CREATE_TEMPLATE);
  }

  public CreateTemplatePlan(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors) {
    super(false, OperatorType.CREATE_TEMPLATE);
    this.name = name;
    this.schemaNames = schemaNames;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, buffer);

    // schema names
    ReadWriteIOUtils.write(schemaNames.size(), buffer);
    for (String schemaName : schemaNames) {
      ReadWriteIOUtils.write(schemaName, buffer);
    }

    // measurements
    ReadWriteIOUtils.write(measurements.size(), buffer);
    for (List<String> measurementList : measurements) {
      ReadWriteIOUtils.write(measurementList.size(), buffer);
      for (String measurement : measurementList) {
        ReadWriteIOUtils.write(measurement, buffer);
      }
    }

    // datatype
    ReadWriteIOUtils.write(dataTypes.size(), buffer);
    for (List<TSDataType> dataTypesList : dataTypes) {
      ReadWriteIOUtils.write(dataTypesList.size(), buffer);
      for (TSDataType dataType : dataTypesList) {
        ReadWriteIOUtils.write(dataType.ordinal(), buffer);
      }
    }

    // encoding
    ReadWriteIOUtils.write(encodings.size(), buffer);
    for (List<TSEncoding> encodingList : encodings) {
      ReadWriteIOUtils.write(encodingList.size(), buffer);
      for (TSEncoding encoding : encodingList) {
        ReadWriteIOUtils.write(encoding.ordinal(), buffer);
      }
    }

    // compressor
    ReadWriteIOUtils.write(compressors.size(), buffer);
    for (CompressionType compressionType : compressors) {
      ReadWriteIOUtils.write(compressionType.ordinal(), buffer);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    name = ReadWriteIOUtils.readString(buffer);

    // schema names
    int size = ReadWriteIOUtils.readInt(buffer);
    schemaNames = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      schemaNames.add(ReadWriteIOUtils.readString(buffer));
    }

    // measurements
    size = ReadWriteIOUtils.readInt(buffer);
    measurements = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      List<String> measurementsList = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        measurementsList.add(ReadWriteIOUtils.readString(buffer));
      }
      measurements.add(measurementsList);
    }

    // datatypes
    size = ReadWriteIOUtils.readInt(buffer);
    dataTypes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      List<TSDataType> dataTypesList = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        dataTypesList.add(TSDataType.values()[ReadWriteIOUtils.readInt(buffer)]);
      }
      dataTypes.add(dataTypesList);
    }

    // encodings
    size = ReadWriteIOUtils.readInt(buffer);
    encodings = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      List<TSEncoding> encodingsList = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        encodingsList.add(TSEncoding.values()[ReadWriteIOUtils.readInt(buffer)]);
      }
      encodings.add(encodingsList);
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
    stream.writeByte((byte) PhysicalPlanType.CREATE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, stream);

    // schema names
    ReadWriteIOUtils.write(schemaNames.size(), stream);
    for (String schemaName : schemaNames) {
      ReadWriteIOUtils.write(schemaName, stream);
    }

    // measurements
    ReadWriteIOUtils.write(measurements.size(), stream);
    for (List<String> measurementList : measurements) {
      ReadWriteIOUtils.write(measurementList.size(), stream);
      for (String measurement : measurementList) {
        ReadWriteIOUtils.write(measurement, stream);
      }
    }

    // datatype
    ReadWriteIOUtils.write(dataTypes.size(), stream);
    for (List<TSDataType> dataTypesList : dataTypes) {
      ReadWriteIOUtils.write(dataTypesList.size(), stream);
      for (TSDataType dataType : dataTypesList) {
        ReadWriteIOUtils.write(dataType.ordinal(), stream);
      }
    }

    // encoding
    ReadWriteIOUtils.write(encodings.size(), stream);
    for (List<TSEncoding> encodingList : encodings) {
      ReadWriteIOUtils.write(encodingList.size(), stream);
      for (TSEncoding encoding : encodingList) {
        ReadWriteIOUtils.write(encoding.ordinal(), stream);
      }
    }

    // compressor
    ReadWriteIOUtils.write(compressors.size(), stream);
    for (CompressionType compressionType : compressors) {
      ReadWriteIOUtils.write(compressionType.ordinal(), stream);
    }

    stream.writeLong(index);
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
