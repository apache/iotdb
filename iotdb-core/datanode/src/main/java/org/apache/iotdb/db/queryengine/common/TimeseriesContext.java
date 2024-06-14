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

public class TimeseriesContext {
  private final String dataType;
  private final String encoding;
  private final String compression;
  private final String tags;
  private final String alias;
  private final String attributes;

  private final String deadband;
  private final String deadbandParameters;

  public TimeseriesContext(IMeasurementSchemaInfo schemaInfo) {
    this.dataType = schemaInfo.getSchema().getType().toString();
    this.encoding = schemaInfo.getSchema().getEncodingType().toString();
    this.compression = schemaInfo.getSchema().getCompressor().toString();
    this.alias = schemaInfo.getAlias();
    this.tags = mapToString(schemaInfo.getTagMap());
    this.attributes = mapToString(schemaInfo.getAttributeMap());
    Pair<String, String> deadbandInfo =
        MetaUtils.parseDeadbandInfo(schemaInfo.getSchema().getProps());
    this.deadband = deadbandInfo.left;
    this.deadbandParameters = deadbandInfo.right;
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

  public String getAlias() {
    return alias;
  }

  public String getTags() {
    return tags;
  }

  public String getAttributes() {
    return attributes;
  }

  public String getDeadbandParameters() {
    return deadbandParameters;
  }

  public String getDeadband() {
    return deadband;
  }

  public TimeseriesContext(
      String dataType,
      String alias,
      String encoding,
      String compression,
      String tags,
      String attributes,
      String deadband,
      String deadbandParameters) {
    this.dataType = dataType;
    this.alias = alias;
    this.encoding = encoding;
    this.compression = compression;
    this.tags = tags;
    this.attributes = attributes;
    this.deadband = deadband;
    this.deadbandParameters = deadbandParameters;
  }

  public void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(dataType, byteBuffer);
    ReadWriteIOUtils.write(alias, byteBuffer);
    ReadWriteIOUtils.write(encoding, byteBuffer);
    ReadWriteIOUtils.write(compression, byteBuffer);
    ReadWriteIOUtils.write(tags, byteBuffer);
    ReadWriteIOUtils.write(attributes, byteBuffer);
    ReadWriteIOUtils.write(deadband, byteBuffer);
    ReadWriteIOUtils.write(deadbandParameters, byteBuffer);
  }

  public void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(dataType, stream);
    ReadWriteIOUtils.write(alias, stream);
    ReadWriteIOUtils.write(encoding, stream);
    ReadWriteIOUtils.write(compression, stream);
    ReadWriteIOUtils.write(tags, stream);
    ReadWriteIOUtils.write(attributes, stream);
    ReadWriteIOUtils.write(deadband, stream);
    ReadWriteIOUtils.write(deadbandParameters, stream);
  }

  public static TimeseriesContext deserialize(ByteBuffer buffer) {
    String dataType = ReadWriteIOUtils.readString(buffer);
    String alias = ReadWriteIOUtils.readString(buffer);
    String encoding = ReadWriteIOUtils.readString(buffer);
    String compression = ReadWriteIOUtils.readString(buffer);
    String tags = ReadWriteIOUtils.readString(buffer);
    String attributes = ReadWriteIOUtils.readString(buffer);
    String deadband = ReadWriteIOUtils.readString(buffer);
    String deadbandParameters = ReadWriteIOUtils.readString(buffer);
    return new TimeseriesContext(
        dataType, alias, encoding, compression, tags, attributes, deadband, deadbandParameters);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TimeseriesContext that = (TimeseriesContext) obj;
    boolean res =
        Objects.equals(dataType, that.dataType)
            && Objects.equals(alias, that.alias)
            && encoding.equals(that.encoding)
            && Objects.equals(compression, that.compression)
            && Objects.equals(tags, that.tags)
            && Objects.equals(attributes, that.attributes)
            && Objects.equals(deadband, that.deadband)
            && Objects.equals(deadbandParameters, that.deadbandParameters);
    return res;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        dataType, alias, encoding, compression, tags, attributes, deadband, deadbandParameters);
  }
}
