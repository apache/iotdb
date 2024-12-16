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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.tsfile.utils.ReadWriteIOUtils.readBytes;
import static org.apache.tsfile.utils.ReadWriteIOUtils.readInt;
import static org.apache.tsfile.utils.ReadWriteIOUtils.write;

public class TableDeviceEntry extends DeviceEntry {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDeviceEntry.class);

  private final List<Binary> attributeColumnValues;

  public TableDeviceEntry(final IDeviceID deviceID, final List<Binary> attributeColumnValues) {
    super(deviceID);
    this.attributeColumnValues = attributeColumnValues;
  }

  public List<Binary> getAttributeColumnValues() {
    return attributeColumnValues;
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    write(attributeColumnValues.size(), byteBuffer);
    for (final Binary value : attributeColumnValues) {
      serializeBinary(byteBuffer, value);
    }
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
    write(attributeColumnValues.size(), stream);
    for (final Binary value : attributeColumnValues) {
      serializeBinary(stream, value);
    }
  }

  public static TableDeviceEntry deserialize(final ByteBuffer byteBuffer) {
    final IDeviceID iDeviceID = StringArrayDeviceID.deserialize(byteBuffer);
    int size = readInt(byteBuffer);
    final List<Binary> attributeColumnValues = new ArrayList<>(size);
    while (size-- > 0) {
      attributeColumnValues.add(deserializeBinary(byteBuffer));
    }
    return new TableDeviceEntry(iDeviceID, attributeColumnValues);
  }

  public static void serializeBinary(final ByteBuffer byteBuffer, final Binary binary) {
    if (binary == null) {
      write(-1, byteBuffer);
      return;
    }
    write(binary, byteBuffer);
  }

  public static void serializeBinary(final DataOutputStream outputStream, final Binary binary)
      throws IOException {
    if (binary == null) {
      write(-1, outputStream);
      return;
    }
    write(binary, outputStream);
  }

  public static Binary deserializeBinary(final ByteBuffer byteBuffer) {
    int length = readInt(byteBuffer);
    if (length <= 0) {
      return null;
    }
    byte[] bytes = readBytes(byteBuffer, length);
    return new Binary(bytes);
  }

  @Override
  public String toString() {
    return "TableDeviceEntry{"
        + "deviceID="
        + deviceID
        + ", attributeColumnValues="
        + attributeColumnValues
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + super.ramBytesUsed()
        + RamUsageEstimator.sizeOfCollection(attributeColumnValues);
  }

  @Override
  public boolean equals(final Object obj) {
    return super.equals(obj)
        && Objects.equals(attributeColumnValues, ((TableDeviceEntry) obj).attributeColumnValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), attributeColumnValues);
  }
}
