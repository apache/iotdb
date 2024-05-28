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

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.execution.operator.schema.source.TimeSeriesSchemaSource.mapToString;

public class TimeseriesSchemaInfo {
  private final String dataType;
  private final String encoding;
  private final String compression;
  private final String tags;

  // TODO: Currently we can't get attributes from fetchSchema in query
  // private final String attributes;

  private final String deadband;
  private final String deadbandParameters;

  public TimeseriesSchemaInfo(IMeasurementSchemaInfo schemaInfo) {
    this.dataType = schemaInfo.getSchema().getType().toString();
    this.encoding = schemaInfo.getSchema().getEncodingType().toString();
    this.compression = schemaInfo.getSchema().getCompressor().toString();
    this.tags = mapToString(schemaInfo.getTagMap());
    Pair<String, String> deadbandInfo =
        MetaUtils.parseDeadbandInfo(schemaInfo.getSchema().getProps());
    this.deadband = deadbandInfo.left == null ? "" : deadbandInfo.left;
    this.deadbandParameters = deadbandInfo.right == null ? "" : deadbandInfo.right;
  }

  public String getDataType() {
    return dataType;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getCompression() {
    return compression;
  }

  public String getTags() {
    return tags;
  }

  public String getDeadbandParameters() {
    return deadbandParameters;
  }

  public String getDeadband() {
    return deadband;
  }

  public TimeseriesSchemaInfo(
      String dataType,
      String encoding,
      String compression,
      String tags,
      String deadband,
      String deadbandParameters) {
    this.dataType = dataType;
    this.encoding = encoding;
    this.compression = compression;
    this.tags = tags;
    this.deadband = deadband;
    this.deadbandParameters = deadbandParameters;
  }

  public void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(dataType, byteBuffer);
    ReadWriteIOUtils.write(encoding, byteBuffer);
    ReadWriteIOUtils.write(compression, byteBuffer);
    ReadWriteIOUtils.write(tags, byteBuffer);
    ReadWriteIOUtils.write(deadband, byteBuffer);
    ReadWriteIOUtils.write(deadbandParameters, byteBuffer);
  }

  public void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(dataType, stream);
    ReadWriteIOUtils.write(encoding, stream);
    ReadWriteIOUtils.write(compression, stream);
    ReadWriteIOUtils.write(tags, stream);
    ReadWriteIOUtils.write(deadband, stream);
    ReadWriteIOUtils.write(deadbandParameters, stream);
  }

  public static TimeseriesSchemaInfo deserialize(ByteBuffer buffer) {
    String dataType = ReadWriteIOUtils.readString(buffer);
    String encoding = ReadWriteIOUtils.readString(buffer);
    String compression = ReadWriteIOUtils.readString(buffer);
    String tags = ReadWriteIOUtils.readString(buffer);
    String deadband = ReadWriteIOUtils.readString(buffer);
    String deadbandParameters = ReadWriteIOUtils.readString(buffer);
    return new TimeseriesSchemaInfo(
        dataType, encoding, compression, tags, deadband, deadbandParameters);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TimeseriesSchemaInfo that = (TimeseriesSchemaInfo) obj;
    return dataType.equals(that.dataType)
        && encoding.equals(that.encoding)
        && compression.equals(that.compression)
        && tags.equals(that.tags)
        && deadband.equals(that.deadband)
        && deadbandParameters.equals(that.deadbandParameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataType, encoding, compression, tags, deadband, deadbandParameters);
  }

  // Some info which can be calculated when executing query, we don't need to store them.
  private String dataBase;

  public void setDataBase(String dataBase) {
    this.dataBase = dataBase;
  }

  public String getDataBase() {
    return dataBase;
  }
}
