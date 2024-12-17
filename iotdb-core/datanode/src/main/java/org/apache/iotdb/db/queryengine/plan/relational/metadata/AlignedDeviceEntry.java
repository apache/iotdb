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
import org.apache.tsfile.utils.Binary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.tsfile.utils.ReadWriteIOUtils.write;

public class AlignedDeviceEntry extends DeviceEntry {

  public AlignedDeviceEntry(IDeviceID deviceID, List<Binary> attributeColumnValues) {
    super(deviceID, attributeColumnValues);
  }

  public void serialize(final ByteBuffer byteBuffer) {
    deviceID.serialize(byteBuffer);
    write(attributeColumnValues.size(), byteBuffer);
    for (final Binary value : attributeColumnValues) {
      serializeBinary(byteBuffer, value);
    }
    write(DeviceEntryType.ALIGNED.ordinal(), byteBuffer);
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    deviceID.serialize(stream);
    write(attributeColumnValues.size(), stream);
    for (final Binary value : attributeColumnValues) {
      serializeBinary(stream, value);
    }
    write(DeviceEntryType.ALIGNED.ordinal(), stream);
  }

  @Override
  public String toString() {
    return "AlignedDeviceEntry{"
        + "deviceID="
        + deviceID
        + ", attributeColumnValues="
        + attributeColumnValues
        + '}';
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final AlignedDeviceEntry that = (AlignedDeviceEntry) obj;
    return Objects.equals(deviceID, that.deviceID)
        && Objects.equals(attributeColumnValues, that.attributeColumnValues);
  }
}
