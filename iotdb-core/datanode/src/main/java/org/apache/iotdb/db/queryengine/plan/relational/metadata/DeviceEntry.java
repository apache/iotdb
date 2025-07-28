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

import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.tsfile.utils.ReadWriteIOUtils.readBytes;
import static org.apache.tsfile.utils.ReadWriteIOUtils.readInt;
import static org.apache.tsfile.utils.ReadWriteIOUtils.write;

/**
 * The {@link DeviceEntry} shall have {@code null} suffix in its {@link IDeviceID}, e.g. "a.b.null".
 * However, in other places related to {@link TableDeviceSchemaCache} or {@link ISchemaRegion}, the
 * {@code null}s are trimmed thus will not appear in the {@link IDeviceID}, and it will be like
 * "a.b".
 */
public abstract class DeviceEntry implements Accountable {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceEntry.class);

  protected final IDeviceID deviceID;
  protected final Binary[] attributeColumnValues;

  public DeviceEntry(final IDeviceID deviceID, final Binary[] attributeColumnValues) {
    this.deviceID = deviceID;
    this.attributeColumnValues = attributeColumnValues;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  // if segmentIndex >= deviceID.segmentNum(), just return null, because we already trimmed tailing
  // null in DeviceID
  public Object getNthSegment(final int segmentIndex) {
    return segmentIndex < deviceID.segmentNum() ? deviceID.segment(segmentIndex) : null;
  }

  public Binary[] getAttributeColumnValues() {
    return attributeColumnValues;
  }

  public void serialize(final ByteBuffer byteBuffer) {
    deviceID.serialize(byteBuffer);
    write(attributeColumnValues.length, byteBuffer);
    for (final Binary value : attributeColumnValues) {
      serializeBinary(byteBuffer, value);
    }
    write(
        this instanceof AlignedDeviceEntry
            ? DeviceEntryType.ALIGNED.ordinal()
            : DeviceEntryType.NON_ALIGNED.ordinal(),
        byteBuffer);
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    deviceID.serialize(stream);
    write(attributeColumnValues.length, stream);
    for (final Binary value : attributeColumnValues) {
      serializeBinary(stream, value);
    }
    write(
        this instanceof AlignedDeviceEntry
            ? DeviceEntryType.ALIGNED.ordinal()
            : DeviceEntryType.NON_ALIGNED.ordinal(),
        stream);
  }

  public static DeviceEntry deserialize(final ByteBuffer byteBuffer) {
    final IDeviceID iDeviceID = StringArrayDeviceID.deserialize(byteBuffer);
    int size = readInt(byteBuffer);
    final Binary[] attributeColumnValues = new Binary[size];
    while (size-- > 0) {
      attributeColumnValues[attributeColumnValues.length - size - 1] =
          deserializeBinary(byteBuffer);
    }
    return constructDeviceEntry(iDeviceID, attributeColumnValues, readInt(byteBuffer));
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

  public enum DeviceEntryType {
    ALIGNED,
    NON_ALIGNED
  }

  private static DeviceEntry constructDeviceEntry(
      IDeviceID deviceID, Binary[] attributeColumnValues, int ordinal) {
    switch (DeviceEntryType.values()[ordinal]) {
      case ALIGNED:
        return new AlignedDeviceEntry(deviceID, attributeColumnValues);
      case NON_ALIGNED:
        return new NonAlignedDeviceEntry(deviceID, attributeColumnValues);
      default:
        throw new UnsupportedOperationException(
            "Unknown AlignedDeviceEntry Type: " + DeviceEntryType.values()[ordinal]);
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + deviceID.ramBytesUsed()
        + RamUsageEstimator.sizeOf(attributeColumnValues);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final DeviceEntry that = (DeviceEntry) obj;
    return Objects.equals(deviceID, that.deviceID)
        && Arrays.equals(attributeColumnValues, that.attributeColumnValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID, Arrays.hashCode(attributeColumnValues));
  }
}
