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

import static org.apache.iotdb.db.queryengine.execution.operator.schema.source.TimeSeriesSchemaSource.mapToString;

public class TimeseriesSchemaInfo {
  boolean isAligned;
  String dataType;
  String encoding;
  String compression;
  String tags;

  // TODO: Currently we can't get attributes from fetchSchema in query
  // String attributes;
  String deadband;
  String deadbandParameters;

  public TimeseriesSchemaInfo(boolean isAligned, IMeasurementSchemaInfo schemaInfo) {
    this.isAligned = isAligned;
    this.dataType = schemaInfo.getSchema().getType().toString();
    this.encoding = schemaInfo.getSchema().getEncodingType().toString();
    this.compression = schemaInfo.getSchema().getCompressor().toString();
    this.tags = mapToString(schemaInfo.getTagMap());
    Pair<String, String> deadbandInfo =
        MetaUtils.parseDeadbandInfo(schemaInfo.getSchema().getProps());
    this.deadband = deadbandInfo.left;
    this.deadbandParameters = deadbandInfo.right;
  }

  public TimeseriesSchemaInfo(
      boolean isAligned,
      String dataType,
      String encoding,
      String compression,
      String tags,
      String deadband,
      String deadbandParameters) {
    this.isAligned = isAligned;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compression = compression;
    this.tags = tags;
    this.deadband = deadband;
    this.deadbandParameters = deadbandParameters;
  }

  public void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(isAligned, byteBuffer);
    ReadWriteIOUtils.write(dataType, byteBuffer);
    ReadWriteIOUtils.write(encoding, byteBuffer);
    ReadWriteIOUtils.write(compression, byteBuffer);
    ReadWriteIOUtils.write(tags, byteBuffer);
    ReadWriteIOUtils.write(deadband, byteBuffer);
    ReadWriteIOUtils.write(deadbandParameters, byteBuffer);
  }

  public void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isAligned, stream);
    ReadWriteIOUtils.write(dataType, stream);
    ReadWriteIOUtils.write(encoding, stream);
    ReadWriteIOUtils.write(compression, stream);
    ReadWriteIOUtils.write(tags, stream);
    ReadWriteIOUtils.write(deadband, stream);
    ReadWriteIOUtils.write(deadbandParameters, stream);
  }

  public static TimeseriesSchemaInfo deserialize(ByteBuffer buffer) {
    boolean isAligned = ReadWriteIOUtils.readBool(buffer);
    String dataType = ReadWriteIOUtils.readString(buffer);
    String encoding = ReadWriteIOUtils.readString(buffer);
    String compression = ReadWriteIOUtils.readString(buffer);
    String tags = ReadWriteIOUtils.readString(buffer);
    String deadband = ReadWriteIOUtils.readString(buffer);
    String deadbandParameters = ReadWriteIOUtils.readString(buffer);
    return new TimeseriesSchemaInfo(
        isAligned, dataType, encoding, compression, tags, deadband, deadbandParameters);
  }
}
