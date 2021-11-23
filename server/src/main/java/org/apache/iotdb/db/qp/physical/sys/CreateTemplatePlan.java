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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.TestOnly;
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

public class CreateTemplatePlan extends PhysicalPlan {

  String name;
  Set<String> alignedDeviceId;
  String[] schemaNames;
  String[][] measurements;
  TSDataType[][] dataTypes;
  TSEncoding[][] encodings;
  CompressionType[][] compressors;
  // constant to help resolve serialized sequence
  private static final int NEW_PLAN = -1;

  public CreateTemplatePlan() {
    super(false, OperatorType.CREATE_TEMPLATE);
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
    this.alignedDeviceId = new HashSet<>();
  }

  public CreateTemplatePlan(
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

  private CreateTemplatePlan(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors,
      Set<String> alignedDeviceId) {
    // Only accessed by deserialization, which may cause ambiguity with align designation
    this(name, measurements, dataTypes, encodings, compressors);
    this.alignedDeviceId = alignedDeviceId;
  }

  public CreateTemplatePlan(
      String name,
      String[][] measurements,
      TSDataType[][] dataTypes,
      TSEncoding[][] encodings,
      CompressionType[][] compressors) {
    super(false, OperatorType.CREATE_TEMPLATE);
    this.name = name;
    this.schemaNames = null;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
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

  public Set<String> getAlignedDeviceId() {
    return alignedDeviceId;
  }

  public List<List<String>> getMeasurements() {
    List<List<String>> ret = new ArrayList<>();
    for (String[] measurement : measurements) {
      ret.add(Arrays.asList(measurement));
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
        if ("".equals(prefix)) {
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
        if ("".equals(prefix)) {
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

    return new CreateTemplatePlan(
        templateName, measurements, dataTypes, encodings, compressors, alignedPrefix.keySet());
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, buffer);

    // write NEW_PLAN as flag to note that there is no schemaNames and new nested list for
    // compressors
    ReadWriteIOUtils.write(NEW_PLAN, buffer);

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
    boolean isFormerSerialized;
    name = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);

    if (size == NEW_PLAN) {
      isFormerSerialized = false;
    } else {
      // deserialize schemaNames
      isFormerSerialized = true;
      schemaNames = new String[size];
      for (int i = 0; i < size; i++) {
        schemaNames[i] = ReadWriteIOUtils.readString(buffer);
      }
    }

    // measurements
    size = ReadWriteIOUtils.readInt(buffer);
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
    if (!isFormerSerialized) {
      // there is a nested list, where each measurement may has different compressor
      compressors = new CompressionType[size][];
      for (int i = 0; i < size; i++) {
        int listSize = ReadWriteIOUtils.readInt(buffer);
        compressors[i] = new CompressionType[listSize];
        for (int j = 0; j < listSize; j++) {
          compressors[i][j] = CompressionType.values()[ReadWriteIOUtils.readInt(buffer)];
        }
      }
    } else {
      // a flat list where aligned measurements have same compressor, serialize as a nested list
      compressors = new CompressionType[size][];
      for (int i = 0; i < size; i++) {
        int listSize = measurements[i].length;
        compressors[i] = new CompressionType[listSize];
        CompressionType alignedCompressionType =
            CompressionType.values()[ReadWriteIOUtils.readInt(buffer)];
        for (int j = 0; j < listSize; j++) {
          compressors[i][j] = alignedCompressionType;
        }
      }
    }

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, stream);

    // write NEW_PLAN as flag to note that there is no schemaNames and new nested list for
    // compressors
    ReadWriteIOUtils.write(NEW_PLAN, stream);

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

  // added and modified for test adaptation of CreateSchemaTemplate serialization.
  @TestOnly
  public void formerSerialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, stream);

    // schema names
    ReadWriteIOUtils.write(schemaNames.length, stream);
    for (String schemaName : schemaNames) {
      ReadWriteIOUtils.write(schemaName, stream);
    }

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

    // compressor
    ReadWriteIOUtils.write(compressors.length, stream);
    for (CompressionType[] compressionType : compressors) {
      ReadWriteIOUtils.write(compressionType[0].ordinal(), stream);
    }

    stream.writeLong(index);
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
