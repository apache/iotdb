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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.AlignedSeriesReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * VectorPartialPath represents a vector's fullPath. It not only contains the full path of vector's
 * own name, but also has subSensorsList which contain all the fullPath of vector's sub sensors.
 * e.g. VectorPartialPath1(root.sg1.d1.vector1, [root.sg1.d1.vector1.s1, root.sg1.d1.vector1.s2])
 * VectorPartialPath2(root.sg1.d1.vector2, [root.sg1.d1.vector2.s1, root.sg1.d1.vector2.s2])
 */
public class AlignedPath extends PartialPath {

  // todo improve vector implementation by remove this placeholder
  private static final String VECTOR_PLACEHOLDER = "";

  private List<String> measurementList;
  private List<IMeasurementSchema> schemaList;

  public AlignedPath() {}

  public AlignedPath(String vectorPath, List<String> subSensorsList) throws IllegalPathException {
    super(vectorPath);
    this.measurementList = subSensorsList;
  }

  public AlignedPath(String vectorPath, String subSensor) throws IllegalPathException {
    super(vectorPath);
    measurementList = new ArrayList<>();
    measurementList.add(subSensor);
  }

  public AlignedPath(PartialPath vectorPath, String subSensor) {
    super(vectorPath.getNodes());
    measurementList = new ArrayList<>();
    measurementList.add(subSensor);
  }

  public AlignedPath(MeasurementPath path) {
    super(path.getDevicePath().getNodes());
    measurementList = new ArrayList<>();
    measurementList.add(path.getMeasurement());
    schemaList = new ArrayList<>();
    schemaList.add(path.getMeasurementSchema());
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

  public void addMeasurement(String measurement) {
    this.measurementList.add(measurement);
  }

  public void addMeasurement(List<String> measurements) {
    this.measurementList.addAll(measurements);
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

  public void addMeasurement(List<String> measurementList, List<IMeasurementSchema> schemaList) {
    this.measurementList.addAll(measurementList);
    if (this.schemaList == null) {
      this.schemaList = new ArrayList<>();
    }
    this.schemaList.addAll(schemaList);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return this.schemaList == null ? Collections.emptyList() : this.schemaList;
  }

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
  public PartialPath copy() {
    AlignedPath result = new AlignedPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    result.measurementList = new ArrayList<>(measurementList);
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
  public AlignedSeriesReader createSeriesReader(
          Set<String> allSensors,
          TSDataType dataType,
          QueryContext context,
          QueryDataSource dataSource,
          Filter timeFilter,
          Filter valueFilter,
          TsFileFilter fileFilter,
          boolean ascending) {
    return new AlignedSeriesReader(
            this,
            allSensors,
            dataType,
            context,
            dataSource,
            timeFilter,
            valueFilter,
            fileFilter,
            ascending);
  }

  @Override
  @TestOnly
  public AlignedSeriesReader createSeriesReader(
          Set<String> allSensors,
          TSDataType dataType,
          QueryContext context,
          List<TsFileResource> seqFileResource,
          List<TsFileResource> unseqFileResource,
          Filter timeFilter,
          Filter valueFilter,
          boolean ascending) {
    return new AlignedSeriesReader(
            this,
            allSensors,
            dataType,
            context,
            seqFileResource,
            unseqFileResource,
            timeFilter,
            valueFilter,
            ascending);
  }
}
