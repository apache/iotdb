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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * VectorPartialPath represents many fullPaths of aligned timeseries. In the AlignedPath, the nodes
 * in PartialPath is deviceId e.g. VectorPartialPath nodes=root.sg1.alignedD1 measurementList=[s1,
 * s2]
 */
public class AlignedPath extends PartialPath {

  private static final Logger logger = LoggerFactory.getLogger(AlignedPath.class);

  // todo improve vector implementation by remove this placeholder
  public static final String VECTOR_PLACEHOLDER = "";

  private List<String> measurementList;
  private List<IMeasurementSchema> schemaList;

  public AlignedPath() {}

  public AlignedPath(String vectorPath, List<String> subSensorsList) throws IllegalPathException {
    super(vectorPath);
    // check whether subSensor is legal
    for (String subSensor : subSensorsList) {
      PathUtils.isLegalPath(subSensor);
    }
    this.measurementList = subSensorsList;
  }

  public AlignedPath(
      String vectorPath, List<String> measurementList, List<IMeasurementSchema> schemaList)
      throws IllegalPathException {
    super(vectorPath);
    // check whether measurement is legal
    for (String measurement : measurementList) {
      PathUtils.isLegalPath(measurement);
    }
    this.measurementList = measurementList;
    this.schemaList = schemaList;
  }

  public AlignedPath(String vectorPath, String subSensor) throws IllegalPathException {
    super(vectorPath);
    measurementList = new ArrayList<>();
    PathUtils.isLegalPath(subSensor);
    measurementList.add(subSensor);
  }

  public AlignedPath(PartialPath vectorPath, String subSensor) throws IllegalPathException {
    super(vectorPath.getNodes());
    measurementList = new ArrayList<>();
    PathUtils.isLegalPath(subSensor);
    measurementList.add(subSensor);
  }

  public AlignedPath(PartialPath vectorPath) {
    super(vectorPath.getNodes());
    measurementList = new ArrayList<>();
    schemaList = new ArrayList<>();
  }

  public AlignedPath(MeasurementPath path) {
    super(path.getDevicePath().getNodes());
    measurementList = new ArrayList<>();
    measurementList.add(path.getMeasurement());
    schemaList = new ArrayList<>();
    schemaList.add(path.getMeasurementSchema());
  }

  public AlignedPath(String vectorPath) throws IllegalPathException {
    super(vectorPath);
    measurementList = new ArrayList<>();
    schemaList = new ArrayList<>();
  }

  @Override
  public PartialPath getDevicePath() {
    return new PartialPath(Arrays.copyOf(nodes, nodes.length));
  }

  @Override
  public String getDevice() {
    return getFullPath();
  }

  @Override
  public String getMeasurement() {
    throw new UnsupportedOperationException("AlignedPath doesn't have measurement name!");
  }

  public List<String> getMeasurementList() {
    return measurementList;
  }

  public String getMeasurement(int index) {
    return measurementList.get(index);
  }

  public PartialPath getPathWithMeasurement(int index) {
    return new PartialPath(nodes).concatNode(measurementList.get(index));
  }

  public void setMeasurementList(List<String> measurementList) {
    this.measurementList = measurementList;
  }

  public void addMeasurements(List<String> measurements) {
    this.measurementList.addAll(measurements);
  }

  public void addSchemas(List<IMeasurementSchema> schemas) {
    this.schemaList.addAll(schemas);
  }

  public void addMeasurement(MeasurementPath measurementPath) {
    if (measurementList == null) {
      measurementList = new ArrayList<>();
    }
    measurementList.add(measurementPath.getMeasurement());

    if (schemaList == null) {
      schemaList = new ArrayList<>();
    }
    schemaList.add(measurementPath.getMeasurementSchema());
  }

  public void addMeasurement(String measurement, IMeasurementSchema measurementSchema) {
    if (measurementList == null) {
      measurementList = new ArrayList<>();
    }
    measurementList.add(measurement);

    if (schemaList == null) {
      schemaList = new ArrayList<>();
    }
    schemaList.add(measurementSchema);
  }

  /**
   * merge another aligned path's sub sensors into this one
   *
   * @param alignedPath The caller need to ensure the alignedPath must have same device as this one
   *     and these two doesn't have same sub sensor
   */
  public void mergeAlignedPath(AlignedPath alignedPath) {
    if (measurementList == null) {
      measurementList = new ArrayList<>();
    }
    measurementList.addAll(alignedPath.measurementList);
    if (schemaList == null) {
      schemaList = new ArrayList<>();
    }
    schemaList.addAll(alignedPath.schemaList);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return this.schemaList == null ? Collections.emptyList() : this.schemaList;
  }

  @Override
  public VectorMeasurementSchema getMeasurementSchema() {
    TSDataType[] types = new TSDataType[measurementList.size()];
    TSEncoding[] encodings = new TSEncoding[measurementList.size()];

    for (int i = 0; i < measurementList.size(); i++) {
      types[i] = schemaList.get(i).getType();
      encodings[i] = schemaList.get(i).getEncodingType();
    }
    String[] array = new String[measurementList.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = measurementList.get(i);
    }
    return new VectorMeasurementSchema(
        VECTOR_PLACEHOLDER, array, types, encodings, schemaList.get(0).getCompressor());
  }

  @Override
  public TSDataType getSeriesType() {
    return TSDataType.VECTOR;
  }

  @Override
  public PartialPath copy() {
    AlignedPath result = new AlignedPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.measurementList = new ArrayList<>(measurementList);
    result.schemaList = new ArrayList<>(schemaList);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AlignedPath that = (AlignedPath) o;
    return Objects.equals(measurementList, that.measurementList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), measurementList);
  }

  @Override
  public int getColumnNum() {
    return measurementList.size();
  }

  @Override
  public AlignedPath clone() {
    AlignedPath alignedPath = null;
    try {
      alignedPath =
          new AlignedPath(
              this.getDevice(),
              new ArrayList<>(this.measurementList),
              new ArrayList<>(this.schemaList));
    } catch (IllegalPathException e) {
      logger.warn("path is illegal: {}", this.getFullPath(), e);
    }
    return alignedPath;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    PathType.Aligned.serialize(byteBuffer);
    super.serializeWithoutType(byteBuffer);
    ReadWriteIOUtils.write(measurementList.size(), byteBuffer);
    for (String measurement : measurementList) {
      ReadWriteIOUtils.write(measurement, byteBuffer);
    }
    if (schemaList == null) {
      ReadWriteIOUtils.write(-1, byteBuffer);
    } else {
      ReadWriteIOUtils.write(schemaList.size(), byteBuffer);
      for (IMeasurementSchema measurementSchema : schemaList) {
        if (measurementSchema instanceof MeasurementSchema) {
          ReadWriteIOUtils.write((byte) 0, byteBuffer);
        } else if (measurementSchema instanceof VectorMeasurementSchema) {
          ReadWriteIOUtils.write((byte) 1, byteBuffer);
        }
        measurementSchema.serializeTo(byteBuffer);
      }
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    PathType.Aligned.serialize(stream);
    super.serializeWithoutType(stream);
    ReadWriteIOUtils.write(measurementList.size(), stream);
    for (String measurement : measurementList) {
      ReadWriteIOUtils.write(measurement, stream);
    }
    if (schemaList == null) {
      ReadWriteIOUtils.write(-1, stream);
    } else {
      ReadWriteIOUtils.write(schemaList.size(), stream);
      for (IMeasurementSchema measurementSchema : schemaList) {
        if (measurementSchema instanceof MeasurementSchema) {
          ReadWriteIOUtils.write((byte) 0, stream);
        } else if (measurementSchema instanceof VectorMeasurementSchema) {
          ReadWriteIOUtils.write((byte) 1, stream);
        }
        measurementSchema.serializeTo(stream);
      }
    }
  }

  public static AlignedPath deserialize(ByteBuffer byteBuffer) {
    PartialPath partialPath = PartialPath.deserialize(byteBuffer);
    AlignedPath alignedPath = new AlignedPath();
    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < measurementSize; i++) {
      measurements.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    int measurementSchemaSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<IMeasurementSchema> measurementSchemas = null;
    if (measurementSchemaSize != -1) {
      measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementSchemaSize; i++) {
        byte type = ReadWriteIOUtils.readByte(byteBuffer);
        if (type == 0) {
          measurementSchemas.add(MeasurementSchema.deserializeFrom(byteBuffer));
        } else if (type == 1) {
          measurementSchemas.add(VectorMeasurementSchema.deserializeFrom(byteBuffer));
        }
      }
    }

    alignedPath.measurementList = measurements;
    alignedPath.schemaList = measurementSchemas;
    alignedPath.nodes = partialPath.getNodes();
    alignedPath.device = partialPath.getDevice();
    alignedPath.fullPath = partialPath.getFullPath();
    return alignedPath;
  }

  @Override
  public PartialPath transformToPartialPath() {
    if (measurementList.size() != 1) {
      throw new UnsupportedOperationException();
    }
    return getDevicePath().concatNode(measurementList.get(0));
  }

  public MeasurementPath getMeasurementPath() {
    if (schemaList.size() != 1) {
      throw new UnsupportedOperationException();
    }
    return new MeasurementPath(transformToPartialPath(), schemaList.get(0), true);
  }

  public String getFormattedString() {
    return getDevicePath().toString() + "[" + String.join(",", measurementList) + "]";
  }
}
