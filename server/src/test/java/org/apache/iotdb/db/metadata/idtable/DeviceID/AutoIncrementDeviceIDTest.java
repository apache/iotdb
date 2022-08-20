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
package org.apache.iotdb.db.metadata.idtable.DeviceID;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AutoIncrementDeviceIDTest {

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  @Before
  public void before() {
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement_INT");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testHashCode() {
    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    IDeviceID deviceID2 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    IDeviceID deviceID3 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d2");
    assertEquals(deviceID1.hashCode(), deviceID2.hashCode());
    assertNotEquals(deviceID1.hashCode(), deviceID3.hashCode());

    AutoIncrementDeviceID autoIncrementDeviceID1 = new AutoIncrementDeviceID();
    autoIncrementDeviceID1.setAutoIncrementID(1);
    AutoIncrementDeviceID autoIncrementDeviceID2 = new AutoIncrementDeviceID();
    autoIncrementDeviceID2.setAutoIncrementID(1);
    AutoIncrementDeviceID autoIncrementDeviceID3 = new AutoIncrementDeviceID();
    autoIncrementDeviceID3.setAutoIncrementID(3);
    assertEquals(autoIncrementDeviceID1.hashCode(), autoIncrementDeviceID2.hashCode());
    assertNotEquals(autoIncrementDeviceID1.hashCode(), autoIncrementDeviceID3.hashCode());
  }

  @Test
  public void testEquals() {
    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    IDeviceID deviceID2 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    IDeviceID deviceID3 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d2");
    SHA256DeviceID sha256DeviceID = new SHA256DeviceID("root.sg.x.d1");
    assertEquals(deviceID1, deviceID2);
    assertNotEquals(deviceID1, deviceID3);
    assertNotEquals(deviceID1, sha256DeviceID);

    AutoIncrementDeviceID autoIncrementDeviceID1 = new AutoIncrementDeviceID();
    autoIncrementDeviceID1.setAutoIncrementID(1);
    AutoIncrementDeviceID autoIncrementDeviceID2 = new AutoIncrementDeviceID();
    autoIncrementDeviceID2.setAutoIncrementID(1);
    AutoIncrementDeviceID autoIncrementDeviceID3 = new AutoIncrementDeviceID();
    autoIncrementDeviceID3.setAutoIncrementID(3);
    assertEquals(autoIncrementDeviceID1, autoIncrementDeviceID2);
    assertNotEquals(autoIncrementDeviceID1, autoIncrementDeviceID3);
  }

  @Test
  public void testToStringID() {
    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    assertEquals(deviceID1.toStringID(), "`0`");
    IDeviceID deviceID2 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    assertEquals(deviceID2.toStringID(), "`0`");
    IDeviceID deviceID3 = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d2");
    assertEquals(deviceID3.toStringID(), "`1`");
  }

  @Test
  public void testSerializeAndDeserialize() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(100);
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    deviceID.serialize(byteBuffer);
    IDeviceID deviceID1 = AutoIncrementDeviceID.deserialize(byteBuffer);
    assertEquals(deviceID, deviceID1);
  }

  @Test
  public void testAutoIncrementDeviceID() {
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d1");
    IDeviceID deviceID1 = DeviceIDFactory.getInstance().getDeviceID("`0`");
    assertEquals(deviceID, deviceID1);
    deviceID = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d2");
    deviceID1 = DeviceIDFactory.getInstance().getDeviceID("`1`");
    assertEquals(deviceID, deviceID1);
    for (int i = 3; i < 10; i++) {
      deviceID = DeviceIDFactory.getInstance().getDeviceID("root.sg.x.d" + i);
      assertEquals(deviceID.toStringID(), "`" + (i - 1) + "`");
    }
  }
}
