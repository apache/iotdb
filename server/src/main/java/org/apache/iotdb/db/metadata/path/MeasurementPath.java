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
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

public class MeasurementPath extends PartialPath {

  private static final Logger logger = LoggerFactory.getLogger(MeasurementPath.class);

  private IMeasurementSchema measurementSchema;

  private boolean isUnderAlignedEntity = false;

  // alias of measurement, null pointer cannot be serialized in thrift so empty string is instead
  private String measurementAlias = "";

  private String version = null;

  public MeasurementPath() {}

  public MeasurementPath(String measurementPath) throws IllegalPathException {
    super(measurementPath);
  }

  public MeasurementPath(String measurementPath, TSDataType type) throws IllegalPathException {
    super(measurementPath);
    this.measurementSchema = new MeasurementSchema(getMeasurement(), type);
  }

  public MeasurementPath(PartialPath measurementPath, IMeasurementSchema measurementSchema) {
    super(measurementPath.getNodes());
    this.measurementSchema = measurementSchema;
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
    return getDevice() + IoTDBConstant.PATH_SEPARATOR + measurementAlias;
  }

  public boolean isUnderAlignedEntity() {
    return isUnderAlignedEntity;
  }

  public void setUnderAlignedEntity(boolean underAlignedEntity) {
    isUnderAlignedEntity = underAlignedEntity;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public PartialPath copy() {
    MeasurementPath result = new MeasurementPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.measurementAlias = measurementAlias;
    result.measurementSchema = measurementSchema;
    result.isUnderAlignedEntity = isUnderAlignedEntity;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MeasurementPath that = (MeasurementPath) o;
    return Objects.equals(version, that.version);
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
    ReadWriteIOUtils.write(isUnderAlignedEntity, byteBuffer);
    ReadWriteIOUtils.write(measurementAlias, byteBuffer);

    ReadWriteIOUtils.write(version, byteBuffer);
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
    measurementPath.isUnderAlignedEntity = ReadWriteIOUtils.readBool(byteBuffer);
    measurementPath.measurementAlias = ReadWriteIOUtils.readString(byteBuffer);

    measurementPath.version = ReadWriteIOUtils.readString(byteBuffer);

    measurementPath.nodes = partialPath.getNodes();
    measurementPath.device = partialPath.getDevice();
    measurementPath.fullPath = partialPath.getFullPath();
    return measurementPath;
  }

  @Override
  public PartialPath transformToPartialPath() {
    return getDevicePath().concatNode(getTailNode());
  }
}
