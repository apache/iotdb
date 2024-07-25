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

package org.apache.iotdb.db.metadata.idtable.entry;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DeviceIDTest {
  @Test
  public void deviceIDBuildTest() throws IllegalPathException {
    PartialPath partialPath1 = new MeasurementPath("root.sg1.d1.s1");
    PartialPath partialPath2 = new MeasurementPath("root.sg1.d1.s2");
    PartialPath partialPath3 = new MeasurementPath("root.sg1.d2.s1");

    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceID(partialPath1.getDevicePath());
    IDeviceID deviceID2 = DeviceIDFactory.getInstance().getDeviceID(partialPath2.getDevicePath());
    IDeviceID deviceID3 = DeviceIDFactory.getInstance().getDeviceID(partialPath3.getDevicePath());

    assertEquals(deviceID1, deviceID2);
    assertNotEquals(deviceID1, deviceID3);
    assertNotEquals(deviceID2, deviceID3);
  }
}
