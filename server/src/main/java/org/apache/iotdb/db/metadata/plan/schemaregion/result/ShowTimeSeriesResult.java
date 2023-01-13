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
package org.apache.iotdb.db.metadata.plan.schemaregion.result;

import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Map;
import java.util.Objects;

public class ShowTimeSeriesResult extends ShowSchemaResult implements ITimeSeriesSchemaInfo {

  private String alias;

  private MeasurementSchema measurementSchema;

  private Map<String, String> tags;
  private Map<String, String> attributes;

  private boolean isUnderAlignedDevice;

  public ShowTimeSeriesResult(
      String name,
      String alias,
      MeasurementSchema measurementSchema,
      Map<String, String> tags,
      Map<String, String> attributes,
      boolean isUnderAlignedDevice) {
    super(name);
    this.alias = alias;
    this.measurementSchema = measurementSchema;
    this.tags = tags;
    this.attributes = attributes;
    this.isUnderAlignedDevice = isUnderAlignedDevice;
  }

  public ShowTimeSeriesResult() {
    super();
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public MeasurementSchema getSchema() {
    return measurementSchema;
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public boolean isUnderAlignedDevice() {
    return isUnderAlignedDevice;
  }

  public TSDataType getDataType() {
    return measurementSchema.getType();
  }

  public TSEncoding getEncoding() {
    return measurementSchema.getEncodingType();
  }

  public CompressionType getCompressor() {
    return measurementSchema.getCompressor();
  }

  public Map<String, String> getTag() {
    return tags;
  }

  public Map<String, String> getAttribute() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowTimeSeriesResult result = (ShowTimeSeriesResult) o;
    return Objects.equals(path, result.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}
