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

public class IDTableLogFileTest {

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  private boolean isEnableIDTableLogFile = false;

  //  @Before
  //  public void before() {
  //    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
  //    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
  //    originalDeviceIDTransformationMethod =
  //        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
  //    isEnableIDTableLogFile = IoTDBDescriptor.getInstance().getConfig().isEnableIDTableLogFile();
  //
  //    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
  //    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("SHA256");
  //    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(true);
  //    EnvironmentUtils.envSetUp();
  //  }
  //
  //  @After
  //  public void clean() throws IOException, StorageEngineException {
  //    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
  //    IoTDBDescriptor.getInstance()
  //        .getConfig()
  //        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
  //    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);
  //    EnvironmentUtils.cleanEnv();
  //  }

  //  @Test
  //  public void testInsertAndAutoCreate() {
  //    try {
  //      // construct an insertRowPlan with mismatched data type
  //      long time = 1L;
  //      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};
  //
  //      String[] columns = new String[2];
  //      columns[0] = "1";
  //      columns[1] = "2";
  //
  //      InsertRowPlan insertRowPlan =
  //          new InsertRowPlan(
  //              new PartialPath("root.laptop.d1.non_aligned_device1"),
  //              time,
  //              new String[] {"s1", "s2"},
  //              dataTypes,
  //              columns,
  //              false);
  //      insertRowPlan.setMeasurementMNodes(
  //          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
  //
  //      // call getSeriesSchemasAndReadLockDevice
  //      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
  //
  //      idTable.getSeriesSchemas(insertRowPlan);
  //
  //      insertRowPlan =
  //          new InsertRowPlan(
  //              new PartialPath("root.laptop.d2.non_aligned_device2"),
  //              time,
  //              new String[] {"s3", "s4"},
  //              dataTypes,
  //              columns,
  //              false);
  //      insertRowPlan.setMeasurementMNodes(
  //          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
  //
  //      idTable.getSeriesSchemas(insertRowPlan);
  //
  //      // test part
  //      HashMap<String, HashSet<String>> deviceIdToNameMap = new HashMap<>();
  //      HashSet<String> d1Set = new HashSet<>();
  //      d1Set.add("root.laptop.d1.non_aligned_device1.s1");
  //      d1Set.add("root.laptop.d1.non_aligned_device1.s2");
  //      HashSet<String> d2Set = new HashSet<>();
  //      d2Set.add("root.laptop.d2.non_aligned_device2.s3");
  //      d2Set.add("root.laptop.d2.non_aligned_device2.s4");
  //
  //      HashSet<String> resSet = new HashSet<>();
  //
  //      deviceIdToNameMap.put(
  //          DeviceIDFactory.getInstance()
  //              .getDeviceID(new PartialPath("root.laptop.d1.non_aligned_device1"))
  //              .toStringID(),
  //          d1Set);
  //      deviceIdToNameMap.put(
  //          DeviceIDFactory.getInstance()
  //              .getDeviceID(new PartialPath("root.laptop.d2.non_aligned_device2"))
  //              .toStringID(),
  //          d2Set);
  //
  //      int count = 0;
  //      for (DiskSchemaEntry entry : idTable.getIDiskSchemaManager().getAllSchemaEntry()) {
  //        String deviceID = entry.deviceID;
  //        HashSet<String> set = deviceIdToNameMap.get(deviceID);
  //
  //        if (set == null) {
  //          fail("device path is not correct " + deviceID);
  //        }
  //
  //        if (!set.contains(entry.seriesKey)) {
  //          fail("series path is not correct " + entry.seriesKey);
  //        }
  //        count++;
  //        resSet.add(entry.seriesKey);
  //      }
  //
  //      assertEquals(4, count);
  //      assertEquals(4, resSet.size());
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail("throw exception");
  //    }
  //  }
}
