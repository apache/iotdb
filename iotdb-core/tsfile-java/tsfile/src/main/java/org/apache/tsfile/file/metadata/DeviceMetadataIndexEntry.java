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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DeviceMetadataIndexEntry implements IMetadataIndexEntry {

  private IDeviceID deviceID;
  private long offset;

  public DeviceMetadataIndexEntry(IDeviceID deviceID, long offset) {
    this.deviceID = deviceID;
    this.offset = offset;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += deviceID.serialize(outputStream);
    byteLen += ReadWriteIOUtils.write(offset, outputStream);
    return byteLen;
  }

  @Override
  public Comparable getCompareKey() {
    return deviceID;
  }

  @Override
  public boolean isDeviceLevel() {
    return true;
  }

  public static DeviceMetadataIndexEntry deserializeFrom(
      ByteBuffer buffer, DeserializeConfig context) {
    IDeviceID device = context.deviceIDBufferDeserializer.deserialize(buffer, context);
    long offset = ReadWriteIOUtils.readLong(buffer);
    return new DeviceMetadataIndexEntry(device, offset);
  }

  public static DeviceMetadataIndexEntry deserializeFrom(
      InputStream inputStream, DeserializeConfig config) throws IOException {
    IDeviceID device = config.deviceIDStreamDeserializer.deserialize(inputStream, config);
    long offset = ReadWriteIOUtils.readLong(inputStream);
    return new DeviceMetadataIndexEntry(device, offset);
  }

  @Override
  public int serializedSize() {
    return deviceID.serializedSize() + Long.BYTES; // offset
  }

  @Override
  public String toString() {
    return "<" + deviceID + "," + offset + ">";
  }
}
