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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateTemplatePlan extends PhysicalPlan {

  String name;
  List<String> schemaNames;
  List<List<String>> measurements;
  List<List<TSDataType>> dataTypes;
  List<List<TSEncoding>> encodings;
  List<List<CompressionType>> compressors;

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

  public List<List<CompressionType>> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<List<CompressionType>> compressors) {
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
      List<List<CompressionType>> compressors) {
    // original constructor
    super(false, OperatorType.CREATE_TEMPLATE);
    this.name = name;
    this.schemaNames = schemaNames;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
  }

  public CreateTemplatePlan(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors) {
    // New constructor for tree-structured template where aligned measurements get individual
    // compressors
    super(false, OperatorType.CREATE_TEMPLATE);

    this.name = name;
    this.schemaNames = new ArrayList<>();
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
  }

  public static CreateTemplatePlan deserializeFromReq(ByteBuffer buffer) throws MetadataException {
    Map<String, List<String>> alignedPrefix = new HashMap<>();
    Map<String, List<TSDataType>> alignedDataTypes = new HashMap<>();
    Map<String, List<TSEncoding>> alignedEncodings = new HashMap<>();
    Map<String, List<CompressionType>> alignedCompressions = new HashMap<>();

    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<List<TSEncoding>> encodings = new ArrayList<>();
    List<List<CompressionType>> compressors = new ArrayList<>();

    String templateName = ReadWriteIOUtils.readString(buffer);
    boolean isAlign = ReadWriteIOUtils.readBool(buffer);
    if (isAlign) {
      alignedPrefix.put("", new ArrayList<>());
      alignedDataTypes.put("", new ArrayList<>());
      alignedEncodings.put("", new ArrayList<>());
      alignedCompressions.put("", new ArrayList<>());
    }

    while(buffer.position() != buffer.limit()) {
      String prefix = ReadWriteIOUtils.readString(buffer);
      isAlign = ReadWriteIOUtils.readBool(buffer);
      String measurementName = ReadWriteIOUtils.readString(buffer);
      TSDataType dataType = TSDataType.values()[ReadWriteIOUtils.readByte(buffer)];
      TSEncoding encoding = TSEncoding.values()[ReadWriteIOUtils.readByte(buffer)];
      CompressionType compressionType = CompressionType.values()[ReadWriteIOUtils.readByte(buffer)];

      if (isAlign && !alignedPrefix.containsKey(prefix)) {
        alignedPrefix.put(prefix, new ArrayList<>());
        alignedDataTypes.put(prefix, new ArrayList<>());
        alignedEncodings.put(prefix, new ArrayList<>());
        alignedCompressions.put(prefix, new ArrayList<>());
      }

      if (alignedPrefix.containsKey(prefix)) {
        alignedPrefix.get(prefix).add(measurementName);
        alignedDataTypes.get(prefix).add(dataType);
        alignedEncodings.get(prefix).add(encoding);
        alignedCompressions.get(prefix).add(compressionType);
      } else {
        measurements.add(Collections.singletonList(prefix + TsFileConstant.PATH_SEPARATOR + measurementName));
        dataTypes.add(Collections.singletonList(dataType));
        encodings.add(Collections.singletonList(encoding));
        compressors.add(Collections.singletonList(compressionType));
      }
    }

    for (String prefix : alignedPrefix.keySet()) {
      List<String> thisMeasurements = new ArrayList<>();
      List<TSDataType> thisDataTypes = new ArrayList<>();
      List<TSEncoding> thisEncodings = new ArrayList<>();
      List<CompressionType> thisCompressors = new ArrayList<>();

      for (int i = 0; i < alignedPrefix.get(prefix).size(); i++) {
        if (prefix.equals("")) {
          thisMeasurements.add(alignedPrefix.get(prefix).get(i));
        } else {
          thisMeasurements.add(prefix + TsFileConstant.PATH_SEPARATOR + alignedPrefix.get(prefix).get(i));
        }
        thisDataTypes.add(alignedDataTypes.get(prefix).get(i));
        thisEncodings.add(alignedEncodings.get(prefix).get(i));
        thisCompressors.add(alignedCompressions.get(prefix).get(i));
      }

      measurements.add(thisMeasurements);
      dataTypes.add(thisDataTypes);
      encodings.add(thisEncodings);
      compressors.add(thisCompressors);
    }

    return new CreateTemplatePlan(
        templateName,
        measurements,
        dataTypes,
        encodings,
        compressors
    );
  }



  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, buffer);

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

    // compressors
    ReadWriteIOUtils.write(compressors.size(), buffer);
    for (List<CompressionType> compressorList : compressors) {
      ReadWriteIOUtils.write(compressorList.size(), buffer);
      for (CompressionType compressionType : compressorList) {
        ReadWriteIOUtils.write(compressionType.ordinal(), buffer);
      }
    }

    buffer.putLong(index);
  }

  @Override
  @SuppressWarnings("Duplicates")
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
      int listSize = ReadWriteIOUtils.readInt(buffer);
      List<CompressionType> compressionTypeList = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        compressionTypeList.add(CompressionType.values()[ReadWriteIOUtils.readInt(buffer)]);
      }
      compressors.add(compressionTypeList);
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
    for (List<CompressionType> compressionTypeList : compressors) {
      ReadWriteIOUtils.write(compressionTypeList.size(), stream);
      for (CompressionType compressionType : compressionTypeList) {
        ReadWriteIOUtils.write(compressionType.ordinal(), stream);
      }
    }

    stream.writeLong(index);
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
