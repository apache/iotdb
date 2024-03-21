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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.sizeOfCharArray;

/** Using device id path as id. */
public class PlainDeviceID implements IDeviceID {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PlainDeviceID.class)
          + RamUsageEstimator.shallowSizeOfInstance(String.class);
  String deviceID;

  public PlainDeviceID(String deviceID) {
    this.deviceID = deviceID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PlainDeviceID)) {
      return false;
    }
    PlainDeviceID that = (PlainDeviceID) o;
    return Objects.equals(deviceID, that.deviceID);
  }

  @Override
  public int hashCode() {
    return deviceID.hashCode();
  }

  public String toString() {
    return "PlainDeviceID{" + "deviceID='" + deviceID + '\'' + '}';
  }

  public String toStringID() {
    return deviceID;
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    return ReadWriteIOUtils.write(deviceID, byteBuffer);
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    return ReadWriteIOUtils.writeVar(deviceID, outputStream);
  }

  @Override
  public byte[] getBytes() {
    return deviceID.getBytes();
  }

  @Override
  public boolean isEmpty() {
    return deviceID.isEmpty();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + sizeOfCharArray(deviceID.length());
  }

  public static PlainDeviceID deserialize(ByteBuffer byteBuffer) {
    return new PlainDeviceID(ReadWriteIOUtils.readString(byteBuffer));
  }

  @Override
  public int compareTo(IDeviceID other) {
    if (!(other instanceof PlainDeviceID)) {
      throw new IllegalArgumentException();
    }
    return deviceID.compareTo(((PlainDeviceID) other).deviceID);
  }
}
