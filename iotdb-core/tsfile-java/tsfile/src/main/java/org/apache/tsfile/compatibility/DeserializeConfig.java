package org.apache.tsfile.compatibility;

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

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DeserializeConfig {
  public byte versionNumber = org.apache.tsfile.common.conf.TSFileConfig.VERSION_NUMBER;
  public BufferDeserializer<TsFileMetadata> tsFileMetadataBufferDeserializer =
      TsFileMetadata::deserializeFrom;

  public BufferDeserializer<MetadataIndexNode> deviceMetadataIndexNodeBufferDeserializer =
      (buffer, context) -> MetadataIndexNode.deserializeFrom(buffer, true, context);
  public BufferDeserializer<MetadataIndexNode> measurementMetadataIndexNodeBufferDeserializer =
      (buffer, context) -> MetadataIndexNode.deserializeFrom(buffer, false, context);
  public BufferDeserializer<IMetadataIndexEntry> deviceMetadataIndexEntryBufferDeserializer =
      DeviceMetadataIndexEntry::deserializeFrom;
  public BufferDeserializer<IMetadataIndexEntry> measurementMetadataIndexEntryBufferDeserializer =
      ((buffer, context) -> MeasurementMetadataIndexEntry.deserializeFrom(buffer));

  public BufferDeserializer<TableSchema> tableSchemaBufferDeserializer = TableSchema::deserialize;
  public BufferDeserializer<MeasurementSchema> measurementSchemaBufferDeserializer =
      ((buffer, context) -> MeasurementSchema.deserializeFrom(buffer));

  public BufferDeserializer<IDeviceID> deviceIDBufferDeserializer =
      ((buffer, context) -> StringArrayDeviceID.deserialize(buffer));

  // stream deserializers
  public StreamDeserializer<MetadataIndexNode> deviceMetadataIndexNodeStreamDeserializer =
      (stream, context) -> MetadataIndexNode.deserializeFrom(stream, true, context);
  public StreamDeserializer<MetadataIndexNode> measurementMetadataIndexNodeStreamDeserializer =
      (stream, context) -> MetadataIndexNode.deserializeFrom(stream, false, context);
  public StreamDeserializer<IMetadataIndexEntry> deviceMetadataIndexEntryStreamDeserializer =
      DeviceMetadataIndexEntry::deserializeFrom;
  public StreamDeserializer<IMetadataIndexEntry> measurementMetadataIndexEntryStreamDeserializer =
      ((stream, context) -> MeasurementMetadataIndexEntry.deserializeFrom(stream));

  public StreamDeserializer<IDeviceID> deviceIDStreamDeserializer =
      ((stream, context) -> StringArrayDeviceID.deserialize(stream));

  public MetadataIndexNode deserializeMetadataIndexNode(ByteBuffer buffer, boolean isDeviceLevel) {
    if (isDeviceLevel) {
      return deviceMetadataIndexNodeBufferDeserializer.deserialize(buffer, this);
    } else {
      return measurementMetadataIndexNodeBufferDeserializer.deserialize(buffer, this);
    }
  }

  public IMetadataIndexEntry deserializeMetadataIndexEntry(
      ByteBuffer buffer, boolean isDeviceLevel) {
    if (isDeviceLevel) {
      return deviceMetadataIndexEntryBufferDeserializer.deserialize(buffer, this);
    } else {
      return measurementMetadataIndexEntryBufferDeserializer.deserialize(buffer, this);
    }
  }

  public MetadataIndexNode deserializeMetadataIndexNode(InputStream stream, boolean isDeviceLevel)
      throws IOException {
    if (isDeviceLevel) {
      return deviceMetadataIndexNodeStreamDeserializer.deserialize(stream, this);
    } else {
      return measurementMetadataIndexNodeStreamDeserializer.deserialize(stream, this);
    }
  }

  public IMetadataIndexEntry deserializeMetadataIndexEntry(
      InputStream stream, boolean isDeviceLevel) throws IOException {
    if (isDeviceLevel) {
      return deviceMetadataIndexEntryStreamDeserializer.deserialize(stream, this);
    } else {
      return measurementMetadataIndexEntryStreamDeserializer.deserialize(stream, this);
    }
  }
}
