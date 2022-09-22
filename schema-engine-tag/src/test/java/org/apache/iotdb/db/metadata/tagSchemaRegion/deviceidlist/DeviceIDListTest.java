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

package org.apache.iotdb.db.metadata.tagSchemaRegion.deviceidlist;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DeviceIDListTest {

  private IDeviceIDList deviceIDList;

  private String storageGroupDirPath;

  private String schemaRegionDirPath;

  private String storageGroupFullPath = "root/testDeviceIDList";

  private boolean isEnableIDTable;

  private String originalDeviceIDTransformationMethod;

  private String schemaDir;

  private String[] devicePaths =
      new String[] {
        storageGroupFullPath + ".a.b.c.d1",
        storageGroupFullPath + ".a.b.c.d2",
        storageGroupFullPath + ".a.b.c.d3",
        storageGroupFullPath + ".a.b.c.d4",
        storageGroupFullPath + ".a.b.d.d1",
        storageGroupFullPath + ".a.b.d.d2",
        storageGroupFullPath + ".a.b.e.d1",
        storageGroupFullPath + ".a.b.f.d1",
        storageGroupFullPath + ".a.b.g.d1",
        storageGroupFullPath + ".a.b.h.d1",
      };

  @Before
  public void setUp() throws Exception {
    schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("SHA256");
    storageGroupDirPath = schemaDir + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + 0;
    deviceIDList = new DeviceIDList(schemaRegionDirPath);
  }

  @After
  public void tearDown() throws Exception {
    deviceIDList.clear();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    FileUtils.deleteDirectoryAndEmptyParent(new File(schemaDir));
  }

  @Test
  public void testAddandGetDeviceID() {
    List<IDeviceID> deviceIDS = generateTestDeviceIDS();
    for (IDeviceID deviceID : deviceIDS) {
      deviceIDList.add(deviceID);
      System.out.println(deviceID.toStringID());
    }
    for (int i = 0; i < deviceIDS.size(); i++) {
      assertEquals(deviceIDS.get(i), deviceIDList.get(i));
    }
  }

  @Test
  public void testRecover() {
    List<IDeviceID> deviceIDS = generateTestDeviceIDS();
    for (IDeviceID deviceID : deviceIDS) {
      deviceIDList.add(deviceID);
      System.out.println(deviceID.toStringID());
    }
    deviceIDList = new DeviceIDList(schemaRegionDirPath);
    for (int i = 0; i < deviceIDS.size(); i++) {
      assertEquals(deviceIDS.get(i), deviceIDList.get(i));
    }
  }

  private List<IDeviceID> generateTestDeviceIDS() {
    List<IDeviceID> deviceIDS = new ArrayList<>();
    for (String devicePath : devicePaths) {
      deviceIDS.add(DeviceIDFactory.getInstance().getDeviceID(devicePath));
    }
    return deviceIDS;
  }
}
