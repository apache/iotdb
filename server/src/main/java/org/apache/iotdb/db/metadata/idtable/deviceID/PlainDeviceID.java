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

package org.apache.iotdb.db.metadata.idtable.deviceID;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Using device id path as id */
public class PlainDeviceID implements IDeviceID {
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

  @Override
  public String toString() {
    return "PlainDeviceID{" + "deviceID='" + deviceID + '\'' + '}';
  }

  @Override
  public String toStringID() {
    return deviceID;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(deviceID, byteBuffer);
  }

  public static PlainDeviceID deserialize(ByteBuffer byteBuffer) {
    return new PlainDeviceID(ReadWriteIOUtils.readString(byteBuffer));
  }
}
