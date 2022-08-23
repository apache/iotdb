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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DeviceIDTest {

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  @Before
  public void setUp() throws Exception {
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement_INT");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
  }

  @Test
  public void deviceIDBuildTest() throws IllegalPathException {
    PartialPath partialPath1 = new PartialPath("root.sg1.d1.s1");
    PartialPath partialPath2 = new PartialPath("root.sg1.d1.s2");
    PartialPath partialPath3 = new PartialPath("root.sg1.d2.s1");

    IDeviceID deviceID1 =
        DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate(partialPath1.getDevicePath());
    IDeviceID deviceID2 =
        DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate(partialPath2.getDevicePath());
    IDeviceID deviceID3 =
        DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate(partialPath3.getDevicePath());

    assertEquals(deviceID1, deviceID2);
    assertNotEquals(deviceID1, deviceID3);
    assertNotEquals(deviceID2, deviceID3);
  }

  @Test
  public void testHashCode() {
    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg1.x.d1");
    IDeviceID deviceID2 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg1.x.d1");
    IDeviceID deviceID3 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg1.x.d2");
    assertEquals(deviceID1.hashCode(), deviceID2.hashCode());
    assertNotEquals(deviceID1.hashCode(), deviceID3.hashCode());
    IDeviceID deviceID4 = DeviceIDFactory.getInstance().getDeviceID(deviceID1.toStringID());
    IDeviceID deviceID5 = DeviceIDFactory.getInstance().getDeviceID(deviceID2.toStringID());
    IDeviceID deviceID6 = DeviceIDFactory.getInstance().getDeviceID(deviceID3.toStringID());
    assertEquals(deviceID1.hashCode(), deviceID4.hashCode());
    assertEquals(deviceID1.hashCode(), deviceID5.hashCode());
    assertEquals(deviceID3.hashCode(), deviceID6.hashCode());

    deviceID1 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg2.x.d1");
    deviceID2 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg2.x.d1");
    deviceID3 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg2.x.d2");
    assertEquals(deviceID1.hashCode(), deviceID2.hashCode());
    assertNotEquals(deviceID1.hashCode(), deviceID3.hashCode());
    deviceID4 = DeviceIDFactory.getInstance().getDeviceID(deviceID1.toStringID());
    deviceID5 = DeviceIDFactory.getInstance().getDeviceID(deviceID2.toStringID());
    deviceID6 = DeviceIDFactory.getInstance().getDeviceID(deviceID3.toStringID());
    assertEquals(deviceID1.hashCode(), deviceID4.hashCode());
    assertEquals(deviceID1.hashCode(), deviceID5.hashCode());
    assertEquals(deviceID3.hashCode(), deviceID6.hashCode());
  }

  @Test
  public void testEquals() throws MetadataException {
    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg1.x.d1");
    IDeviceID deviceID2 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg1.x.d1");
    IDeviceID deviceID3 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg1.x.d2");
    assertEquals(deviceID1, deviceID2);
    assertNotEquals(deviceID1, deviceID3);
    IDeviceID deviceID4 = DeviceIDFactory.getInstance().getDeviceID(deviceID1.toStringID());
    IDeviceID deviceID5 = DeviceIDFactory.getInstance().getDeviceID(deviceID2.toStringID());
    IDeviceID deviceID6 = DeviceIDFactory.getInstance().getDeviceID(deviceID3.toStringID());
    assertEquals(deviceID1, deviceID4);
    assertEquals(deviceID1, deviceID5);
    assertEquals(deviceID3, deviceID6);

    deviceID1 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg2.x.d1");
    deviceID2 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg2.x.d1");
    deviceID3 = DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg2.x.d2");
    assertEquals(deviceID1, deviceID2);
    assertNotEquals(deviceID1, deviceID3);
    deviceID4 = DeviceIDFactory.getInstance().getDeviceID(deviceID1.toStringID());
    deviceID5 = DeviceIDFactory.getInstance().getDeviceID(deviceID2.toStringID());
    deviceID6 = DeviceIDFactory.getInstance().getDeviceID(deviceID3.toStringID());
    assertEquals(deviceID1, deviceID4);
    assertEquals(deviceID1, deviceID5);
    assertEquals(deviceID3, deviceID6);
  }

  @Test
  public void testSerializeAndDeserialize() throws MetadataException {
    for (int i = 1; i < 10; i++) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(100);
      IDeviceID deviceID =
          DeviceIDFactory.getInstance().getDeviceIDWithAutoCreate("root.sg.x.d" + i);
      deviceID.serialize(byteBuffer);
      byteBuffer.flip();
      IDeviceID deviceID1 = StandAloneAutoIncDeviceID.deserialize(byteBuffer);
      assertEquals(deviceID, deviceID1);
    }
  }
}
