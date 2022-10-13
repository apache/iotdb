/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MeasurementPath extends PartialPath {

  private static final Logger logger = LoggerFactory.getLogger(MeasurementPath.class);

  private IMeasurementSchema measurementSchema;

  private Map<String, String> tagMap;

  private boolean isUnderAlignedEntity = false;

  // alias of measurement, null pointer cannot be serialized in thrift so empty string is instead
  private String measurementAlias = "";

  public MeasurementPath() {}

  public MeasurementPath(String measurementPath) throws IllegalPathException {
    super(measurementPath);
  }

  public MeasurementPath(String measurementPath, TSDataType type) throws IllegalPathException {
    super(measurementPath);
    this.measurementSchema = new MeasurementSchema(getMeasurement(), type);
  }

  public MeasurementPath(PartialPath path, TSDataType type) {
    this(path, new MeasurementSchema(path.getMeasurement(), type), false);
  }

  public MeasurementPath(PartialPath measurementPath, IMeasurementSchema measurementSchema) {
    this(measurementPath, measurementSchema, false);
  }

  public MeasurementPath(
      PartialPath measurementPath,
      IMeasurementSchema measurementSchema,
      boolean isUnderAlignedEntity) {
    super(measurementPath.getNodes());
    this.measurementSchema = measurementSchema;
    this.isUnderAlignedEntity = isUnderAlignedEntity;
  }

  public MeasurementPath(String device, String measurement, IMeasurementSchema measurementSchema)
      throws IllegalPathException {
    super(device, measurement);
    this.measurementSchema = measurementSchema;
  }

  public MeasurementPath(String[] nodes, MeasurementSchema schema) {
    super(nodes);
    this.measurementSchema = schema;
  }

  @Override
  public IMeasurementSchema getMeasurementSchema() {
    return measurementSchema;
  }

  public Map<String, String> getTagMap() {
    return tagMap;
  }

  @Override
  public TSDataType getSeriesType() {
    return getMeasurementSchema().getType();
  }

  public byte getSeriesTypeInByte() {
    return getMeasurementSchema().getTypeInByte();
  }

  public void setMeasurementSchema(IMeasurementSchema measurementSchema) {
    this.measurementSchema = measurementSchema;
  }

  public void setTagMap(Map<String, String> tagMap) {
    this.tagMap = tagMap;
  }

  @Override
  public String getMeasurementAlias() {
    return measurementAlias;
  }

  @Override
  public void setMeasurementAlias(String measurementAlias) {
    if (measurementAlias != null) {
      this.measurementAlias = measurementAlias;
    }
  }

  public void removeMeasurementAlias() {
    this.measurementAlias = null;
  }

  @Override
  public boolean isMeasurementAliasExists() {
    return measurementAlias != null && !measurementAlias.isEmpty();
  }

  @Override
  public String getFullPathWithAlias() {
    if (getDevice().isEmpty()) {
      return measurementAlias;
    }
    return getDevice() + IoTDBConstant.PATH_SEPARATOR + measurementAlias;
  }

  public boolean isUnderAlignedEntity() {
    return isUnderAlignedEntity;
  }

  public void setUnderAlignedEntity(boolean underAlignedEntity) {
    isUnderAlignedEntity = underAlignedEntity;
  }

  @Override
  public PartialPath copy() {
    MeasurementPath result = new MeasurementPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.measurementAlias = measurementAlias;
    result.measurementSchema = measurementSchema;
    if (tagMap != null) {
      result.tagMap = new HashMap<>(tagMap);
    }
    result.isUnderAlignedEntity = isUnderAlignedEntity;
    return result;
  }

  /**
   * if isUnderAlignedEntity is true, return an AlignedPath with only one sub sensor otherwise,
   * return itself
   */
  public PartialPath transformToExactPath() {
    return isUnderAlignedEntity ? new AlignedPath(this) : this;
  }

  @Override
  public MeasurementPath clone() {
    MeasurementPath newMeasurementPath = null;
    try {
      newMeasurementPath =
          new MeasurementPath(this.getDevice(), this.getMeasurement(), this.getMeasurementSchema());
      newMeasurementPath.setUnderAlignedEntity(this.isUnderAlignedEntity);
      newMeasurementPath.setMeasurementAlias(this.measurementAlias);
      if (tagMap != null) {
        newMeasurementPath.setTagMap(new HashMap<>(tagMap));
      }
    } catch (IllegalPathException e) {
      logger.warn("path is illegal: {}", this.getFullPath(), e);
    }
    return newMeasurementPath;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    PathType.Measurement.serialize(byteBuffer);
    super.serializeWithoutType(byteBuffer);
    if (measurementSchema == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      if (measurementSchema instanceof MeasurementSchema) {
        ReadWriteIOUtils.write((byte) 0, byteBuffer);
      } else if (measurementSchema instanceof VectorMeasurementSchema) {
        ReadWriteIOUtils.write((byte) 1, byteBuffer);
      }
      measurementSchema.serializeTo(byteBuffer);
    }
    if (tagMap == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(tagMap, byteBuffer);
    }
    ReadWriteIOUtils.write(isUnderAlignedEntity, byteBuffer);
    ReadWriteIOUtils.write(measurementAlias, byteBuffer);
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    PathType.Measurement.serialize(stream);
    super.serializeWithoutType(stream);
    if (measurementSchema == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      if (measurementSchema instanceof MeasurementSchema) {
        ReadWriteIOUtils.write((byte) 0, stream);
      } else if (measurementSchema instanceof VectorMeasurementSchema) {
        ReadWriteIOUtils.write((byte) 1, stream);
      }
      measurementSchema.serializeTo(stream);
    }
    if (tagMap == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(tagMap, stream);
    }
    ReadWriteIOUtils.write(isUnderAlignedEntity, stream);
    ReadWriteIOUtils.write(measurementAlias, stream);
  }

  public static MeasurementPath deserialize(ByteBuffer byteBuffer) {
    PartialPath partialPath = PartialPath.deserialize(byteBuffer);
    MeasurementPath measurementPath = new MeasurementPath();
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    if (isNull == 1) {
      byte type = ReadWriteIOUtils.readByte(byteBuffer);
      if (type == 0) {
        measurementPath.measurementSchema = MeasurementSchema.deserializeFrom(byteBuffer);
      } else if (type == 1) {
        measurementPath.measurementSchema = VectorMeasurementSchema.deserializeFrom(byteBuffer);
      }
    }
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    if (isNull == 1) {
      measurementPath.tagMap = ReadWriteIOUtils.readMap(byteBuffer);
    }
    measurementPath.isUnderAlignedEntity = ReadWriteIOUtils.readBool(byteBuffer);
    measurementPath.measurementAlias = ReadWriteIOUtils.readString(byteBuffer);
    measurementPath.nodes = partialPath.getNodes();
    measurementPath.device = partialPath.getDevice();
    measurementPath.fullPath = partialPath.getFullPath();
    return measurementPath;
  }

  @Override
  public PartialPath transformToPartialPath() {
    return getDevicePath().concatNode(getTailNode());
  }

  /**
   * In specific scenarios, like internal create timeseries, the message can only be passed as
   * String format.
   */
  public static String transformDataToString(MeasurementPath measurementPath) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      measurementPath.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // this exception won't happen.
    }
    byte[] bytes = byteArrayOutputStream.toByteArray();
    // must use single-byte char sets
    return new String(bytes, StandardCharsets.ISO_8859_1);
  }

  public static MeasurementPath parseDataFromString(String measurementPathData) {
    return (MeasurementPath)
        PathDeserializeUtil.deserialize(
            ByteBuffer.wrap(measurementPathData.getBytes(StandardCharsets.ISO_8859_1)));
  }
}
