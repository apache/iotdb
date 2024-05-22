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
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeviceEntry {

  private final IDeviceID deviceID;
  private final List<String> attributeColumnValues;

  public DeviceEntry(IDeviceID deviceID, List<String> attributeColumnValues) {
    this.deviceID = deviceID;
    this.attributeColumnValues = attributeColumnValues;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  public List<String> getAttributeColumnValues() {
    return attributeColumnValues;
  }

  public void serialize(ByteBuffer byteBuffer) {
    deviceID.serialize(byteBuffer);
    ReadWriteIOUtils.write(attributeColumnValues.size(), byteBuffer);
    for (String value : attributeColumnValues) {
      ReadWriteIOUtils.write(value, byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    deviceID.serialize(stream);
    ReadWriteIOUtils.write(attributeColumnValues.size(), stream);
    for (String value : attributeColumnValues) {
      ReadWriteIOUtils.write(value, stream);
    }
  }

  public static DeviceEntry deserialize(ByteBuffer byteBuffer) {
    IDeviceID iDeviceID = StringArrayDeviceID.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> attributeColumnValues = new ArrayList<>(size);
    while (size-- > 0) {
      attributeColumnValues.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    return new DeviceEntry(iDeviceID, attributeColumnValues);
  }

  @Override
  public String toString() {
    return "DeviceEntry{"
        + "deviceID="
        + deviceID
        + ", attributeColumnValues="
        + attributeColumnValues
        + '}';
  }
}
