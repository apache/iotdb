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
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TreeDeviceEntry extends DeviceEntry {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TreeDeviceEntry.class);

  private final boolean isAligned;

  public TreeDeviceEntry(final IDeviceID deviceID, final boolean isAligned) {
    super(deviceID);
    this.isAligned = isAligned;
  }

  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isAligned, byteBuffer);
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isAligned, stream);
  }

  public static TreeDeviceEntry deserialize(final ByteBuffer byteBuffer) {
    return new TreeDeviceEntry(
        StringArrayDeviceID.deserialize(byteBuffer), ReadWriteIOUtils.readBool(byteBuffer));
  }

  @Override
  public String toString() {
    return "TableDeviceEntry{" + "deviceID=" + deviceID + ", isAligned=" + isAligned + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + super.ramBytesUsed();
  }

  @Override
  public boolean equals(final Object obj) {
    return super.equals(obj) && Objects.equals(isAligned, ((TreeDeviceEntry) obj).isAligned);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), isAligned);
  }
}
