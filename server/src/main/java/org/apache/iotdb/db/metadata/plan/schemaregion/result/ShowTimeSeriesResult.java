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
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Map;
import java.util.Objects;

public class ShowTimeSeriesResult extends ShowSchemaResult implements ITimeSeriesSchemaInfo {

  private String alias;

  private MeasurementSchema measurementSchema;

  private Map<String, String> tags;
  private Map<String, String> attributes;

  public ShowTimeSeriesResult(
      String name,
      String alias,
      MeasurementSchema measurementSchema,
      Map<String, String> tags,
      Map<String, String> attributes) {
    super(name);
    this.alias = alias;
    this.measurementSchema = measurementSchema;
    this.tags = tags;
    this.attributes = attributes;
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
  public Pair<Map<String, String>, Map<String, String>> getTagAndAttribute() {
    return new Pair<>(tags, attributes);
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

  public String getDeadband() {
    return MetaUtils.parseDeadbandInfo(measurementSchema.getProps()).left;
  }

  public String getDeadbandParameters() {
    return MetaUtils.parseDeadbandInfo(measurementSchema.getProps()).right;
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
