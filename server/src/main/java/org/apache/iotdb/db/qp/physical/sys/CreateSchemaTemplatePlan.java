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

package org.apache.iotdb.db.qp.physical.sys;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateSchemaTemplatePlan extends PhysicalPlan {

  String name;
  Set<String> alignedPrefix;
  String[] schemaNames;
  String[][] measurements;
  TSDataType[][] dataTypes;
  TSEncoding[][] encodings;
  CompressionType[][] compressors;

  public CreateSchemaTemplatePlan() {
    super(false, OperatorType.CREATE_SCHEMA_TEMPLATE);
  }

  public CreateSchemaTemplatePlan(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors) {
    // New constructor for tree-structured template where aligned measurements get individual
    // compressors
    super(false, OperatorType.CREATE_SCHEMA_TEMPLATE);

    this.name = name;
    schemaNames = null;
    this.measurements = new String[measurements.size()][];
    for (int i = 0; i < measurements.size(); i++) {
      this.measurements[i] = new String[measurements.get(i).size()];
      for (int j = 0; j < measurements.get(i).size(); j++) {
        this.measurements[i][j] = measurements.get(i).get(j);
      }
    }

    this.dataTypes = new TSDataType[dataTypes.size()][];
    for (int i = 0; i < dataTypes.size(); i++) {
      this.dataTypes[i] = new TSDataType[dataTypes.get(i).size()];
      for (int j = 0; j < dataTypes.get(i).size(); j++) {
        this.dataTypes[i][j] = dataTypes.get(i).get(j);
      }
    }

    this.encodings = new TSEncoding[dataTypes.size()][];
    for (int i = 0; i < encodings.size(); i++) {
      this.encodings[i] = new TSEncoding[dataTypes.get(i).size()];
      for (int j = 0; j < encodings.get(i).size(); j++) {
        this.encodings[i][j] = encodings.get(i).get(j);
      }
    }

    this.compressors = new CompressionType[dataTypes.size()][];
    for (int i = 0; i < compressors.size(); i++) {
      this.compressors[i] = new CompressionType[compressors.get(i).size()];
      for (int j = 0; j < compressors.get(i).size(); j++) {
        this.compressors[i][j] = compressors.get(i).get(j);
      }
    }
    this.alignedPrefix = new HashSet<>();
  }

  public CreateSchemaTemplatePlan(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors) {
    // original constructor
    this(name, measurements, dataTypes, encodings, compressors);
    this.schemaNames = schemaNames.toArray(new String[0]);
  }

  private CreateSchemaTemplatePlan(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors,
      Set<String> alignedPrefix) {
    // Only accessed by deserialization, which may cause ambiguity with align designation
    this(name, measurements, dataTypes, encodings, compressors);
    this.alignedPrefix = alignedPrefix;
  }

  public CreateSchemaTemplatePlan(
      String name,
      String[][] measurements,
      TSDataType[][] dataTypes,
      TSEncoding[][] encodings,
      CompressionType[][] compressors) {
    super(false, OperatorType.CREATE_SCHEMA_TEMPLATE);
    this.name = name;
    this.schemaNames = null;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
    this.alignedPrefix = alignedPrefix;
  }

  public List<String> getSchemaNames() {
    if (schemaNames != null) {
      return Arrays.asList(schemaNames);
    } else {
      return null;
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<String> getAlignedPrefix() {
    return alignedPrefix;
  }

  public List<List<String>> getMeasurements() {
    List<List<String>> ret = new ArrayList<>();
    for (int i = 0; i < measurements.length; i++) {
      ret.add(Arrays.asList(measurements[i]));
    }
    return ret;
  }

  public List<List<TSDataType>> getDataTypes() {
    List<List<TSDataType>> ret = new ArrayList<>();
    for (TSDataType[] alignedDataTypes : dataTypes) {
      ret.add(Arrays.asList(alignedDataTypes));
    }
    return ret;
  }

  public List<List<TSEncoding>> getEncodings() {
    List<List<TSEncoding>> ret = new ArrayList<>();
    for (TSEncoding[] alignedEncodings : encodings) {
      ret.add(Arrays.asList(alignedEncodings));
    }
    return ret;
  }

  public List<List<CompressionType>> getCompressors() {
    List<List<CompressionType>> ret = new ArrayList<>();
    for (CompressionType[] alignedCompressor : compressors) {
      ret.add(Arrays.asList(alignedCompressor));
    }
    return ret;
  }

  public static CreateSchemaTemplatePlan deserializeFromReq(ByteBuffer buffer) throws MetadataException {
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

    while (buffer.position() != buffer.limit()) {
      String prefix = ReadWriteIOUtils.readString(buffer);
      isAlign = ReadWriteIOUtils.readBool(buffer);
      String measurementName = ReadWriteIOUtils.readString(buffer);
      TSDataType dataType = TSDataType.values()[ReadWriteIOUtils.readByte(buffer)];
      TSEncoding encoding = TSEncoding.values()[ReadWriteIOUtils.readByte(buffer)];
      CompressionType compressionType = CompressionType.values()[ReadWriteIOUtils.readByte(buffer)];

      if (alignedPrefix.containsKey(prefix) && !isAlign) {
        throw new MetadataException("Align designation incorrect at: " + prefix);
      }

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
        if (prefix.equals("")) {
          measurements.add(Collections.singletonList(measurementName));
        } else {
          measurements.add(
              Collections.singletonList(prefix + TsFileConstant.PATH_SEPARATOR + measurementName));
        }
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
          thisMeasurements.add(
              prefix + TsFileConstant.PATH_SEPARATOR + alignedPrefix.get(prefix).get(i));
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

    return new CreateSchemaTemplatePlan(
        templateName, measurements, dataTypes, encodings, compressors, alignedPrefix.keySet());
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, buffer);

    // measurements
    ReadWriteIOUtils.write(measurements.length, buffer);
    for (String[] measurementList : measurements) {
      ReadWriteIOUtils.write(measurementList.length, buffer);
      for (String measurement : measurementList) {
        ReadWriteIOUtils.write(measurement, buffer);
      }
    }

    // datatype
    ReadWriteIOUtils.write(dataTypes.length, buffer);
    for (TSDataType[] dataTypesList : dataTypes) {
      ReadWriteIOUtils.write(dataTypesList.length, buffer);
      for (TSDataType dataType : dataTypesList) {
        ReadWriteIOUtils.write(dataType.ordinal(), buffer);
      }
    }

    // encoding
    ReadWriteIOUtils.write(encodings.length, buffer);
    for (TSEncoding[] encodingList : encodings) {
      ReadWriteIOUtils.write(encodingList.length, buffer);
      for (TSEncoding encoding : encodingList) {
        ReadWriteIOUtils.write(encoding.ordinal(), buffer);
      }
    }

    // compressors
    ReadWriteIOUtils.write(compressors.length, buffer);
    for (CompressionType[] compressorList : compressors) {
      ReadWriteIOUtils.write(compressorList.length, buffer);
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

    // measurements
    int size = ReadWriteIOUtils.readInt(buffer);
    measurements = new String[size][];
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      measurements[i] = new String[listSize];
      for (int j = 0; j < listSize; j++) {
        measurements[i][j] = ReadWriteIOUtils.readString(buffer);
      }
    }

    // datatypes
    size = ReadWriteIOUtils.readInt(buffer);
    dataTypes = new TSDataType[size][];
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      dataTypes[i] = new TSDataType[listSize];
      for (int j = 0; j < listSize; j++) {
        dataTypes[i][j] = TSDataType.values()[ReadWriteIOUtils.readInt(buffer)];
      }
    }

    // encodings
    size = ReadWriteIOUtils.readInt(buffer);
    encodings = new TSEncoding[size][];
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      encodings[i] = new TSEncoding[listSize];
      for (int j = 0; j < listSize; j++) {
        encodings[i][j] = TSEncoding.values()[ReadWriteIOUtils.readInt(buffer)];
      }
    }

    // compressor
    size = ReadWriteIOUtils.readInt(buffer);
    compressors = new CompressionType[size][];
    for (int i = 0; i < size; i++) {
      int listSize = ReadWriteIOUtils.readInt(buffer);
      compressors[i] = new CompressionType[listSize];
      for (int j = 0; j < listSize; j++) {
        compressors[i][j] = CompressionType.values()[ReadWriteIOUtils.readInt(buffer)];
      }
    }

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, stream);

    // measurements
    ReadWriteIOUtils.write(measurements.length, stream);
    for (String[] measurementList : measurements) {
      ReadWriteIOUtils.write(measurementList.length, stream);
      for (String measurement : measurementList) {
        ReadWriteIOUtils.write(measurement, stream);
      }
    }

    // datatype
    ReadWriteIOUtils.write(dataTypes.length, stream);
    for (TSDataType[] dataTypesList : dataTypes) {
      ReadWriteIOUtils.write(dataTypesList.length, stream);
      for (TSDataType dataType : dataTypesList) {
        ReadWriteIOUtils.write(dataType.ordinal(), stream);
      }
    }

    // encoding
    ReadWriteIOUtils.write(encodings.length, stream);
    for (TSEncoding[] encodingList : encodings) {
      ReadWriteIOUtils.write(encodingList.length, stream);
      for (TSEncoding encoding : encodingList) {
        ReadWriteIOUtils.write(encoding.ordinal(), stream);
      }
    }

    // compressors
    ReadWriteIOUtils.write(compressors.length, stream);
    for (CompressionType[] compressorList : compressors) {
      ReadWriteIOUtils.write(compressorList.length, stream);
      for (CompressionType compressionType : compressorList) {
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
