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

import java.util.Arrays;

public class NonAlignedDeviceEntry extends DeviceEntry {

  public NonAlignedDeviceEntry(final IDeviceID deviceID, final Binary[] attributeColumnValues) {
    super(deviceID, attributeColumnValues);
  }

  @Override
  public String toString() {
    return "NonAlignedDeviceEntry{"
        + "deviceID="
        + deviceID
        + ", attributeColumnValues="
        + Arrays.toString(attributeColumnValues)
        + '}';
  }
}
