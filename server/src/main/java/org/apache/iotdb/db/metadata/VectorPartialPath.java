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

package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * VectorPartialPath represents a vector's fullPath. It not only contains the full path of vector's
 * own name, but also has subSensorsList which contain all the fullPath of vector's sub sensors.
 * e.g. VectorPartialPath1(root.sg1.d1.vector1, [root.sg1.d1.vector1.s1, root.sg1.d1.vector1.s2])
 * VectorPartialPath2(root.sg1.d1.vector2, [root.sg1.d1.vector2.s1, root.sg1.d1.vector2.s2])
 */
public class VectorPartialPath extends PartialPath {

  // todo improve vector implementation by remove this placeholder
  private static final String VECTOR_PLACEHOLDER = "";

  private List<String> subSensorsList;
  private List<IMeasurementSchema> schemaList;

  public VectorPartialPath() {}

  public VectorPartialPath(String vectorPath, List<String> subSensorsList)
      throws IllegalPathException {
    super(vectorPath);
    this.subSensorsList = subSensorsList;
  }

  public VectorPartialPath(String vectorPath, String subSensor) throws IllegalPathException {
    super(vectorPath);
    subSensorsList = new ArrayList<>();
    subSensorsList.add(subSensor);
  }

  public VectorPartialPath(PartialPath vectorPath, String subSensor) {
    super(vectorPath.getNodes());
    subSensorsList = new ArrayList<>();
    subSensorsList.add(subSensor);
  }

  public VectorPartialPath(MeasurementPath path) {
    super(path.getDevicePath().concatNode(VECTOR_PLACEHOLDER).getNodes());
    subSensorsList = new ArrayList<>();
    subSensorsList.add(path.getMeasurement());
    schemaList = new ArrayList<>();
    schemaList.add(path.getMeasurementSchema());
  }

  public List<String> getSubSensorsList() {
    return subSensorsList;
  }

  public String getSubSensor(int index) {
    return subSensorsList.get(index);
  }

  public PartialPath getPathWithSubSensor(int index) {
    return new PartialPath(nodes).concatNode(subSensorsList.get(index));
  }

  public void setSubSensorsList(List<String> subSensorsList) {
    this.subSensorsList = subSensorsList;
  }

  public void addSubSensor(String subSensor) {
    this.subSensorsList.add(subSensor);
  }

  public void addSubSensor(List<String> subSensors) {
    this.subSensorsList.addAll(subSensors);
  }

  public void addMeasurement(List<String> measurementList, List<IMeasurementSchema> schemaList) {
    this.subSensorsList.addAll(measurementList);
    this.schemaList.addAll(schemaList);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return this.schemaList;
  }

  public VectorMeasurementSchema getMeasurementSchema() {
    TSDataType[] types = new TSDataType[subSensorsList.size()];
    TSEncoding[] encodings = new TSEncoding[subSensorsList.size()];

    for (int i = 0; i < subSensorsList.size(); i++) {
      types[i] = schemaList.get(i).getType();
      encodings[i] = schemaList.get(i).getEncodingType();
    }
    String[] array = new String[subSensorsList.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = subSensorsList.get(i);
    }
    return new VectorMeasurementSchema(
        VECTOR_PLACEHOLDER, array, types, encodings, schemaList.get(0).getCompressor());
  }

  @Override
  public PartialPath copy() {
    VectorPartialPath result = new VectorPartialPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.subSensorsList = new ArrayList<>(subSensorsList);
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
    VectorPartialPath that = (VectorPartialPath) o;
    return Objects.equals(subSensorsList, that.subSensorsList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), subSensorsList);
  }

  @Override
  public String getExactFullPath() {
    fullPath = getFullPath();
    if (subSensorsList.size() == 1) {
      return fullPath + TsFileConstant.PATH_SEPARATOR + subSensorsList.get(0);
    }
    return fullPath;
  }
}
