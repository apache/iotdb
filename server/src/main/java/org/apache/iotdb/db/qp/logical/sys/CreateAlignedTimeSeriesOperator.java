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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateAlignedTimeSeriesOperator extends Operator {

  private PartialPath prefixPath;
  private List<String> measurements = new ArrayList<>();
  private List<TSDataType> dataTypes = new ArrayList<>();
  private List<TSEncoding> encodings = new ArrayList<>();
  private List<CompressionType> compressors = new ArrayList<>();
  private List<String> aliasList = new ArrayList<>();
  private List<Map<String, String>> tagsList = new ArrayList<>();
  private List<Map<String, String>> attributesList = new ArrayList<>();
  private List<Long> tagOffsets = null;

  public CreateAlignedTimeSeriesOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_ALIGNED_TIMESERIES;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(PartialPath prefixPath) {
    this.prefixPath = prefixPath;
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

  public void addTagOffsets(Long tagsOffset) {
    this.tagOffsets.add(tagsOffset);
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    Set<String> measurementSet = new HashSet<>(measurements);
    if (measurementSet.size() < measurements.size()) {
      throw new SQLParserException(
          "the measurement under an aligned device is not allowed to have the same measurement name");
    }

    return new CreateAlignedTimeSeriesPlan(
        prefixPath,
        measurements,
        dataTypes,
        encodings,
        compressors,
        aliasList,
        tagsList,
        attributesList);
  }
}
