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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CreateAlignedTimeSeriesPlanImpl implements ICreateAlignedTimeSeriesPlan {

  private PartialPath devicePath;
  private List<String> measurements;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private List<CompressionType> compressors;
  private List<String> aliasList;
  private List<Map<String, String>> tagsList;
  private List<Map<String, String>> attributesList;
  private List<Long> tagOffsets = null;

  CreateAlignedTimeSeriesPlanImpl() {}

  CreateAlignedTimeSeriesPlanImpl(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList) {
    this.devicePath = devicePath;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
    this.aliasList = aliasList;
    this.tagsList = tagsList;
    this.attributesList = attributesList;
  }

  public CreateAlignedTimeSeriesPlanImpl(
      PartialPath devicePath, String measurement, MeasurementSchema schema) {
    this.devicePath = devicePath;
    this.measurements = Collections.singletonList(measurement);
    this.dataTypes = Collections.singletonList(schema.getType());
    this.encodings = Collections.singletonList(schema.getEncodingType());
    this.compressors = Collections.singletonList(schema.getCompressor());
  }

  @Override
  public PartialPath getDevicePath() {
    return devicePath;
  }

  @Override
  public void setDevicePath(PartialPath devicePath) {
    this.devicePath = devicePath;
  }

  @Override
  public List<String> getMeasurements() {
    return measurements;
  }

  @Override
  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  @Override
  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  @Override
  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  @Override
  public void setEncodings(List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  @Override
  public List<CompressionType> getCompressors() {
    return compressors;
  }

  @Override
  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  @Override
  public List<String> getAliasList() {
    return aliasList;
  }

  @Override
  public void setAliasList(List<String> aliasList) {
    this.aliasList = aliasList;
  }

  @Override
  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  @Override
  public void setTagsList(List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  @Override
  public List<Map<String, String>> getAttributesList() {
    return attributesList;
  }

  @Override
  public void setAttributesList(List<Map<String, String>> attributesList) {
    this.attributesList = attributesList;
  }

  @Override
  public List<Long> getTagOffsets() {
    if (tagOffsets == null) {
      tagOffsets = new ArrayList<>();
      for (int i = 0; i < measurements.size(); i++) {
        tagOffsets.add(Long.parseLong("-1"));
      }
    }
    return tagOffsets;
  }

  @Override
  public void setTagOffsets(List<Long> tagOffsets) {
    this.tagOffsets = tagOffsets;
  }
}
