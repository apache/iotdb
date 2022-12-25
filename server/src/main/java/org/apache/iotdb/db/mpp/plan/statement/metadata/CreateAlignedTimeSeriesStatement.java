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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * CREATE ALIGNED TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>CREATE ALIGNED TIMESERIES devicePath (measurementId attributeClauses [, measurementId
 * attributeClauses]...)
 */
public class CreateAlignedTimeSeriesStatement extends Statement {

  private PartialPath devicePath;
  private List<String> measurements = new ArrayList<>();
  private List<TSDataType> dataTypes = new ArrayList<>();
  private List<TSEncoding> encodings = new ArrayList<>();
  private List<CompressionType> compressors = new ArrayList<>();
  private List<String> aliasList = new ArrayList<>();
  private List<Map<String, String>> tagsList = new ArrayList<>();
  private List<Map<String, String>> attributesList = new ArrayList<>();

  public CreateAlignedTimeSeriesStatement() {
    super();
    statementType = StatementType.CREATE_ALIGNED_TIMESERIES;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> paths = new ArrayList<>();
    for (String measurement : measurements) {
      paths.add(devicePath.concatNode(measurement));
    }
    return paths;
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

  public void addMeasurement(String measurement) {
    this.measurements.add(measurement);
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public void addDataType(TSDataType dataType) {
    this.dataTypes.add(dataType);
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  public void addEncoding(TSEncoding encoding) {
    this.encodings.add(encoding);
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public void addCompressor(CompressionType compression) {
    this.compressors.add(compression);
  }

  public List<String> getAliasList() {
    return aliasList;
  }

  public void setAliasList(List<String> aliasList) {
    this.aliasList = aliasList;
  }

  public void addAliasList(String alias) {
    this.aliasList.add(alias);
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  public void setTagsList(List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  public void addTagsList(Map<String, String> tags) {
    this.tagsList.add(tags);
  }

  public List<Map<String, String>> getAttributesList() {
    return attributesList;
  }

  public void setAttributesList(List<Map<String, String>> attributesList) {
    this.attributesList = attributesList;
  }

  public void addAttributesList(Map<String, String> attributes) {
    this.attributesList.add(attributes);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateAlignedTimeseries(this, context);
  }
}
