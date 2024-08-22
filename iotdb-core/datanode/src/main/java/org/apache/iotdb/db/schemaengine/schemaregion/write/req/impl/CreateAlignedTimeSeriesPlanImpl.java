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

package org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
  private transient boolean withMerge;

  public CreateAlignedTimeSeriesPlanImpl() {}

  public CreateAlignedTimeSeriesPlanImpl(
      final PartialPath devicePath,
      final List<String> measurements,
      final List<TSDataType> dataTypes,
      final List<TSEncoding> encodings,
      final List<CompressionType> compressors,
      final List<String> aliasList,
      final List<Map<String, String>> tagsList,
      final List<Map<String, String>> attributesList) {
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
      final PartialPath devicePath, final String measurement, final MeasurementSchema schema) {
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
  public void setDevicePath(final PartialPath devicePath) {
    this.devicePath = devicePath;
  }

  @Override
  public List<String> getMeasurements() {
    return measurements;
  }

  @Override
  public void setMeasurements(final List<String> measurements) {
    this.measurements = measurements;
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  @Override
  public void setDataTypes(final List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  @Override
  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  @Override
  public void setEncodings(final List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  @Override
  public List<CompressionType> getCompressors() {
    return compressors;
  }

  @Override
  public void setCompressors(final List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  @Override
  public List<String> getAliasList() {
    return aliasList;
  }

  @Override
  public void setAliasList(final List<String> aliasList) {
    this.aliasList = aliasList;
  }

  @Override
  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  @Override
  public void setTagsList(final List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  @Override
  public List<Map<String, String>> getAttributesList() {
    return attributesList;
  }

  @Override
  public void setAttributesList(final List<Map<String, String>> attributesList) {
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
  public void setTagOffsets(final List<Long> tagOffsets) {
    this.tagOffsets = tagOffsets;
  }

  public boolean isWithMerge() {
    return withMerge;
  }

  public void setWithMerge(final boolean withMerge) {
    this.withMerge = withMerge;
    // Deep copy if with merge because when upsert option is set
    // The original set may be altered to help distinguish between creation plan
    // and update plan
    // Here we deeply copy to avoid damaging the original plan node for pipe schema region listening
    // queue
    if (withMerge) {
      measurements = new ArrayList<>(measurements);
      dataTypes = new ArrayList<>(dataTypes);
      encodings = new ArrayList<>(encodings);
      compressors = new ArrayList<>(compressors);
      aliasList = Objects.nonNull(aliasList) ? new ArrayList<>(aliasList) : null;
      tagsList = Objects.nonNull(tagsList) ? new ArrayList<>(tagsList) : null;
      attributesList = Objects.nonNull(attributesList) ? new ArrayList<>(attributesList) : null;
      tagOffsets = Objects.nonNull(tagOffsets) ? new ArrayList<>(tagOffsets) : null;
    }
  }
}
