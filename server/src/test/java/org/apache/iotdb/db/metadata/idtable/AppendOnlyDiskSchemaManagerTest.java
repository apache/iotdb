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
package org.apache.iotdb.db.metadata.idtable;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.deviceID.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class AppendOnlyDiskSchemaManagerTest {
  /** system dir */
  private String systemDir =
      FilePathUtils.regularizePath(IoTDBDescriptor.getInstance().getConfig().getSystemDir())
          + "storage_groups";

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  private AppendOnlyDiskSchemaManager appendOnlyDiskSchemaManager = null;

  private String storageGroupPath = "root.AppendOnlyDiskSchemaManagerTest";

  @Before
  public void setUp() throws Exception {
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement_INT");
    EnvironmentUtils.envSetUp();
    appendOnlyDiskSchemaManager =
        new AppendOnlyDiskSchemaManager(
            SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + storageGroupPath));
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    appendOnlyDiskSchemaManager.close();
    appendOnlyDiskSchemaManager = null;
  }

  @Test
  public void serialize() {
    for (int i = 0; i < 10; i++) {
      String devicePath = storageGroupPath + "." + "d" + i;
      String measurement = "s";
      String deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath).toStringID();
      DiskSchemaEntry schemaEntry =
          new DiskSchemaEntry(
              deviceID,
              devicePath + "." + measurement,
              measurement,
              Byte.parseByte("0"),
              Byte.parseByte("0"),
              Byte.parseByte("0"),
              false);
      appendOnlyDiskSchemaManager.serialize(schemaEntry);
    }
  }

  @Test
  public void recover() {
    serialize();
    IDTable idTable =
        new IDTableHashmapImpl(
            SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + storageGroupPath));
    appendOnlyDiskSchemaManager.recover(idTable);
    for (int i = 0; i < 10; i++) {
      String devicePath = storageGroupPath + "." + "d" + i;
      String measurement = "s";
      IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
      DeviceEntry deviceEntry = idTable.getDeviceEntry(deviceID.toStringID());
      DeviceEntry deviceEntry1 = idTable.getDeviceEntry(deviceID);
      assertNotNull(deviceEntry);
      assertNotNull(deviceEntry1);
      assertEquals(deviceEntry, deviceEntry1);
      SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(measurement);
      assertNotNull(schemaEntry);
    }
  }

  @Test
  public void getAllSchemaEntry() {
    serialize();
    try {
      Collection<DiskSchemaEntry> diskSchemaEntries =
          appendOnlyDiskSchemaManager.getAllSchemaEntry();
      int i = 0;
      for (DiskSchemaEntry diskSchemaEntry : diskSchemaEntries) {
        String devicePath = storageGroupPath + "." + "d" + i;
        String measurement = "s";
        IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
        assertEquals(diskSchemaEntry.deviceID, deviceID.toStringID());
        assertEquals(diskSchemaEntry.measurementName, measurement);
        assertEquals(diskSchemaEntry.seriesKey, devicePath + "." + measurement);
        i++;
      }
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
