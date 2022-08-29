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

public class AutoIncDeviceID implements IDeviceID {

  // starting with 0,the maximum value is Integer.MAX_VALUE
  int schemaRegionId;

  // starting with 0,the maximum value is Integer.MAX_VALUE
  int autoIncrementID;

  public AutoIncDeviceID() {}

  public AutoIncDeviceID(String deviceID) {
    long id = parseFromDeviceID(deviceID);
    this.schemaRegionId = (int) (id >>> 32);
    this.autoIncrementID = (int) id;
  }

  public AutoIncDeviceID(int schemaRegionId, int autoIncrementID) {
    this.schemaRegionId = schemaRegionId;
    this.autoIncrementID = autoIncrementID;
  }

  private long parseFromDeviceID(String deviceID) {
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    return Long.parseLong(deviceID);
  }

  @Override
  public int hashCode() {
    long id = (long) schemaRegionId << 32;
    id |= autoIncrementID;
    return Long.hashCode(id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AutoIncDeviceID)) {
      return false;
    }
    AutoIncDeviceID that = (AutoIncDeviceID) o;
    return this.autoIncrementID == that.autoIncrementID
        && this.schemaRegionId == that.schemaRegionId;
  }

  @Override
  public String toString() {
    return "AutoIncDeviceID{"
        + "schemaRegionId="
        + schemaRegionId
        + ", autoIncrementID="
        + autoIncrementID
        + '}';
  }

  @Override
  public String toStringID() {
    long stringID = (long) schemaRegionId << 32;
    stringID |= autoIncrementID;
    return "`" + stringID + '`';
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(schemaRegionId, byteBuffer);
    ReadWriteIOUtils.write(autoIncrementID, byteBuffer);
  }

  public static AutoIncDeviceID deserialize(ByteBuffer byteBuffer) {
    AutoIncDeviceID deviceID = new AutoIncDeviceID();
    deviceID.schemaRegionId = ReadWriteIOUtils.readInt(byteBuffer);
    deviceID.autoIncrementID = ReadWriteIOUtils.readInt(byteBuffer);
    return deviceID;
  }

  public int getSchemaRegionId() {
    return schemaRegionId;
  }

  public int getAutoIncrementID() {
    return autoIncrementID;
  }
}
