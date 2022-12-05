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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateAlignedTimeSeriesPlan extends PhysicalPlan
    implements ICreateAlignedTimeSeriesPlan {

  private static final Logger logger = LoggerFactory.getLogger(CreateAlignedTimeSeriesPlan.class);
  private static final int PLAN_SINCE_0_14 = -1;

  private PartialPath devicePath;
  private List<String> measurements;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private List<CompressionType> compressors;
  private List<String> aliasList;
  private List<Map<String, String>> tagsList;
  private List<Map<String, String>> attributesList;
  private List<Long> tagOffsets = null;

  public CreateAlignedTimeSeriesPlan() {
    super(Operator.OperatorType.CREATE_ALIGNED_TIMESERIES);
    canBeSplit = false;
  }

  public CreateAlignedTimeSeriesPlan(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList) {
    super(Operator.OperatorType.CREATE_ALIGNED_TIMESERIES);
    this.devicePath = devicePath;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
    this.aliasList = aliasList;
    this.tagsList = tagsList;
    this.attributesList = attributesList;
    this.canBeSplit = false;
  }

  public CreateAlignedTimeSeriesPlan(
      PartialPath devicePath, String measurement, MeasurementSchema schema) {
    super(Operator.OperatorType.CREATE_ALIGNED_TIMESERIES);
    this.devicePath = devicePath;
    this.measurements = Collections.singletonList(measurement);
    this.dataTypes = Collections.singletonList(schema.getType());
    this.encodings = Collections.singletonList(schema.getEncodingType());
    this.compressors = Collections.singletonList(schema.getCompressor());
    this.canBeSplit = false;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public void setDevicePath(PartialPath devicePath) {
    this.devicePath = devicePath;
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

  public List<String> getAliasList() {
    return aliasList;
  }

  public void setAliasList(List<String> aliasList) {
    this.aliasList = aliasList;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  public void setTagsList(List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  public List<Map<String, String>> getAttributesList() {
    return attributesList;
  }

  public void setAttributesList(List<Map<String, String>> attributesList) {
    this.attributesList = attributesList;
  }

  public List<Long> getTagOffsets() {
    if (tagOffsets == null) {
      tagOffsets = new ArrayList<>();
      for (int i = 0; i < measurements.size(); i++) {
        tagOffsets.add(Long.parseLong("-1"));
      }
    }
    return tagOffsets;
  }

  public void setTagOffsets(List<Long> tagOffsets) {
    this.tagOffsets = tagOffsets;
  }

  @Override
  public String toString() {
    return String.format(
        "devicePath: %s, measurements: %s, dataTypes: %s, encodings: %s, compressions: %s, tagOffsets: %s",
        devicePath, measurements, dataTypes, encodings, compressors, tagOffsets);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> paths = new ArrayList<>();
    for (String measurement : measurements) {
      try {
        paths.add(new PartialPath(devicePath.getFullPath(), measurement));
      } catch (IllegalPathException e) {
        logger.error("Failed to get paths of CreateAlignedTimeSeriesPlan. ", e);
      }
    }
    return paths;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_ALIGNED_TIMESERIES.ordinal());

    // distinguish the plan from that of old versions
    stream.writeInt(PLAN_SINCE_0_14);

    byte[] bytes = devicePath.getFullPath().getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);

    ReadWriteIOUtils.write(measurements.size(), stream);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, stream);
    }
    for (TSDataType dataType : dataTypes) {
      stream.write(dataType.ordinal());
    }
    for (TSEncoding encoding : encodings) {
      stream.write(encoding.ordinal());
    }
    for (CompressionType compressor : compressors) {
      stream.write(compressor.serialize());
    }
    if (tagOffsets == null) {
      tagOffsets = new ArrayList<>();
      for (int i = 0; i < measurements.size(); i++) {
        tagOffsets.add(Long.parseLong("-1"));
      }
    }
    for (Long tagOffset : tagOffsets) {
      stream.writeLong(tagOffset);
    }

    // alias
    if (aliasList != null && !aliasList.isEmpty()) {
      stream.write(1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, stream);
      }
    } else {
      stream.write(0);
    }

    // tags
    if (tagsList != null && !tagsList.isEmpty()) {
      stream.write(1);
      for (Map<String, String> tags : tagsList) {
        ReadWriteIOUtils.write(tags, stream);
      }
    } else {
      stream.write(0);
    }

    // attributes
    if (attributesList != null && !attributesList.isEmpty()) {
      stream.write(1);
      for (Map<String, String> attributes : attributesList) {
        ReadWriteIOUtils.write(attributes, stream);
      }
    } else {
      stream.write(0);
    }

    stream.writeLong(index);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_ALIGNED_TIMESERIES.ordinal());

    // distinguish the plan from that of old versions
    buffer.putInt(PLAN_SINCE_0_14);

    byte[] bytes = devicePath.getFullPath().getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);

    ReadWriteIOUtils.write(measurements.size(), buffer);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, buffer);
    }
    for (TSDataType dataType : dataTypes) {
      buffer.put((byte) dataType.ordinal());
    }
    for (TSEncoding encoding : encodings) {
      buffer.put((byte) encoding.ordinal());
    }
    for (CompressionType compressor : compressors) {
      buffer.put(compressor.serialize());
    }
    for (Long tagOffset : getTagOffsets()) {
      buffer.putLong(tagOffset);
    }

    // alias
    if (aliasList != null && !aliasList.isEmpty()) {
      buffer.put((byte) 1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    // tags
    if (tagsList != null && !tagsList.isEmpty()) {
      buffer.put((byte) 1);
      for (Map<String, String> tags : tagsList) {
        ReadWriteIOUtils.write(tags, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    // attributes
    if (attributesList != null && !attributesList.isEmpty()) {
      buffer.put((byte) 1);
      for (Map<String, String> attributes : attributesList) {
        ReadWriteIOUtils.write(attributes, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    buffer.putLong(index);
  }

  public void formerSerialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_ALIGNED_TIMESERIES.ordinal());

    byte[] bytes = devicePath.getFullPath().getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);

    ReadWriteIOUtils.write(measurements.size(), buffer);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, buffer);
    }
    for (TSDataType dataType : dataTypes) {
      buffer.put((byte) dataType.ordinal());
    }
    for (TSEncoding encoding : encodings) {
      buffer.put((byte) encoding.ordinal());
    }
    for (CompressionType compressor : compressors) {
      buffer.put(compressor.serialize());
    }

    // alias
    if (aliasList != null && !aliasList.isEmpty()) {
      buffer.put((byte) 1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    // adapt to old version based on version mark
    int length = buffer.getInt();
    boolean isOldVersion = true;
    if (length == PLAN_SINCE_0_14) {
      length = buffer.getInt();
      isOldVersion = false;
    }
    byte[] bytes = new byte[length];
    buffer.get(bytes);

    devicePath = new PartialPath(new String(bytes));
    int size = ReadWriteIOUtils.readInt(buffer);
    measurements = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      measurements.add(ReadWriteIOUtils.readString(buffer));
    }
    dataTypes = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      dataTypes.add(TSDataType.values()[buffer.get()]);
    }
    encodings = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      encodings.add(TSEncoding.values()[buffer.get()]);
    }
    compressors = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      compressors.add(CompressionType.deserialize(buffer.get()));
    }
    if (!isOldVersion) {
      tagOffsets = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        tagOffsets.add(buffer.getLong());
      }
    }

    // alias
    if (buffer.get() == 1) {
      aliasList = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        aliasList.add(ReadWriteIOUtils.readString(buffer));
      }
    }

    if (!isOldVersion) {
      // tags
      if (buffer.get() == 1) {
        tagsList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
          tagsList.add(ReadWriteIOUtils.readMap(buffer));
        }
      }

      // attributes
      if (buffer.get() == 1) {
        attributesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
          attributesList.add(ReadWriteIOUtils.readMap(buffer));
        }
      }
    }

    this.index = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateAlignedTimeSeriesPlan that = (CreateAlignedTimeSeriesPlan) o;

    return Objects.equals(devicePath, that.devicePath)
        && Objects.equals(measurements, that.measurements)
        && Objects.equals(dataTypes, that.dataTypes)
        && Objects.equals(encodings, that.encodings)
        && Objects.equals(compressors, that.compressors)
        && Objects.equals(tagOffsets, that.tagOffsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(devicePath, measurements, dataTypes, encodings, compressors, tagOffsets);
  }
}
