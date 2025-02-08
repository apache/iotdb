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
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchemaType;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class MeasurementPath extends PartialPath {

  private static final String NODES_LENGTH_ERROR =
      "nodes.length for MeasurementPath should always be greater than 1, current is: %s";

  private static final Logger logger = LoggerFactory.getLogger(MeasurementPath.class);

  private IMeasurementSchema measurementSchema;

  private Map<String, String> tagMap;

  private Boolean isUnderAlignedEntity = false;

  // alias of measurement, null pointer cannot be serialized in thrift so empty string is instead
  private String measurementAlias = "";

  public MeasurementPath() {}

  public MeasurementPath(String measurementPath) throws IllegalPathException {
    super(measurementPath);
    if (nodes.length < 2) {
      throw new IllegalArgumentException(String.format(NODES_LENGTH_ERROR, Arrays.toString(nodes)));
    }
  }

  public MeasurementPath(String measurementPath, TSDataType type) throws IllegalPathException {
    super(measurementPath);
    if (nodes.length < 2) {
      throw new IllegalArgumentException(String.format(NODES_LENGTH_ERROR, Arrays.toString(nodes)));
    }
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
      Boolean isUnderAlignedEntity) {
    super(measurementPath.getNodes());
    if (nodes.length < 2) {
      throw new IllegalArgumentException(String.format(NODES_LENGTH_ERROR, Arrays.toString(nodes)));
    }
    this.measurementSchema = measurementSchema;
    this.isUnderAlignedEntity = isUnderAlignedEntity;
  }

  public MeasurementPath(IDeviceID device, String measurement) throws IllegalPathException {
    super(device, measurement);
  }

  public MeasurementPath(IDeviceID device, String measurement, IMeasurementSchema measurementSchema)
      throws IllegalPathException {
    super(device, measurement);
    this.measurementSchema = measurementSchema;
  }

  public MeasurementPath(String device, String measurement) throws IllegalPathException {
    super(device, measurement);
    if (nodes.length < 2) {
      throw new IllegalArgumentException(String.format(NODES_LENGTH_ERROR, Arrays.toString(nodes)));
    }
  }

  public MeasurementPath(String device, String measurement, IMeasurementSchema measurementSchema)
      throws IllegalPathException {
    super(device, measurement);
    if (nodes.length < 2) {
      throw new IllegalArgumentException(String.format(NODES_LENGTH_ERROR, Arrays.toString(nodes)));
    }
    this.measurementSchema = measurementSchema;
  }

  public MeasurementPath(String[] nodes, IMeasurementSchema schema) {
    super(nodes);
    if (nodes.length < 2) {
      throw new IllegalArgumentException(String.format(NODES_LENGTH_ERROR, Arrays.toString(nodes)));
    }
    this.measurementSchema = schema;
  }

  public MeasurementPath(String[] nodes) {
    super(nodes);
    if (nodes.length < 2) {
      throw new IllegalArgumentException(
          "nodes.length for MeasurementPath should always be greater than 2, current is: "
              + Arrays.toString(nodes));
    }
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
    if (measurementSchema == null) {
      return null;
    }
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
    if (getIDeviceID().isEmpty()) {
      return measurementAlias;
    }
    return getIDeviceID().toString() + IoTDBConstant.PATH_SEPARATOR + measurementAlias;
  }

  public boolean isUnderAlignedEntity() {
    if (isUnderAlignedEntity == null) {
      return false;
    }
    return isUnderAlignedEntity;
  }

  public void setUnderAlignedEntity(Boolean underAlignedEntity) {
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
          new MeasurementPath(
              this.getIDeviceID(), this.getMeasurement(), this.getMeasurementSchema());
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
      MeasurementSchemaType measurementSchemaType = measurementSchema.getSchemaType();
      ReadWriteIOUtils.write(
          measurementSchemaType.getMeasurementSchemaTypeInByteEnum(), byteBuffer);
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
      MeasurementSchemaType measurementSchemaType = measurementSchema.getSchemaType();
      ReadWriteIOUtils.write(measurementSchemaType.getMeasurementSchemaTypeInByteEnum(), stream);
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
      if (type == MeasurementSchemaType.MEASUREMENT_SCHEMA.getMeasurementSchemaTypeInByteEnum()) {
        measurementPath.measurementSchema = MeasurementSchema.deserializeFrom(byteBuffer);
      } else if (type
          == MeasurementSchemaType.VECTOR_MEASUREMENT_SCHEMA.getMeasurementSchemaTypeInByteEnum()) {
        measurementPath.measurementSchema = VectorMeasurementSchema.deserializeFrom(byteBuffer);
      } else if (type
          == MeasurementSchemaType.LOGICAL_VIEW_SCHEMA.getMeasurementSchemaTypeInByteEnum()) {
        measurementPath.measurementSchema = LogicalViewSchema.deserializeFrom(byteBuffer);
      } else {
        throw new RuntimeException(
            new UnexpectedException("Type (" + type + ") of measurementSchema is unknown."));
      }
    }
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    if (isNull == 1) {
      measurementPath.tagMap = ReadWriteIOUtils.readMap(byteBuffer);
    }
    measurementPath.isUnderAlignedEntity = ReadWriteIOUtils.readBoolObject(byteBuffer);
    measurementPath.measurementAlias = ReadWriteIOUtils.readString(byteBuffer);
    measurementPath.nodes = partialPath.getNodes();
    measurementPath.device = measurementPath.getIDeviceID();
    measurementPath.fullPath = measurementPath.getFullPath();
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

  @Override
  protected IDeviceID toDeviceID(String[] nodes) {
    // remove measurement
    nodes = Arrays.copyOfRange(nodes, 0, nodes.length - 1);
    return super.toDeviceID(nodes);
  }

  public static MeasurementPath parseDataFromString(String measurementPathData) {
    return (MeasurementPath)
        PathDeserializeUtil.deserialize(
            ByteBuffer.wrap(measurementPathData.getBytes(StandardCharsets.ISO_8859_1)));
  }

  @Override
  protected PartialPath createPartialPath(String[] newPathNodes) {
    return new MeasurementPath(newPathNodes);
  }

  @Override
  public PartialPath getDevicePath() {
    return new PartialPath(Arrays.copyOf(nodes, nodes.length - 1));
  }

  public List<PartialPath> getDevicePathPattern() {
    List<PartialPath> result = new ArrayList<>();
    result.add(getDevicePath());
    if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      result.add(new PartialPath(nodes));
    }
    return result;
  }
}
