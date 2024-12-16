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
import org.apache.tsfile.utils.Accountable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class DeviceEntry implements Accountable {

  protected final IDeviceID deviceID;

  protected DeviceEntry(final IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  // if segmentIndex >= deviceID.segmentNum(), just return null, because we already trimmed tailing
  // null in DeviceID
  public Object getNthSegment(final int segmentIndex) {
    return segmentIndex < deviceID.segmentNum() ? deviceID.segment(segmentIndex) : null;
  }

  public void serialize(final ByteBuffer byteBuffer) {
    deviceID.serialize(byteBuffer);
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    deviceID.serialize(stream);
  }

  @Override
  public long ramBytesUsed() {
    return deviceID.ramBytesUsed();
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
    return Objects.equals(deviceID, that.deviceID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID);
  }
}
