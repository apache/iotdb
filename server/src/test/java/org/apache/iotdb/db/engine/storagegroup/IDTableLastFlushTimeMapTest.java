/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.storagegroup;

public class IDTableLastFlushTimeMapTest extends LastFlushTimeMapTest {

  //  private boolean isEnableIDTable = false;
  //
  //  private String originalDeviceIDTransformationMethod = null;
  //
  //  private boolean isEnableIDTableLogFile = false;
  //
  //  public IDTableLastFlushTimeMapTest() throws QueryProcessException {}
  //
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
  //    super.before();
  //  }
  //
  //  @After
  //  public void clean() throws IOException, StorageEngineException {
  //    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
  //    IoTDBDescriptor.getInstance()
  //        .getConfig()
  //        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
  //    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);
  //    super.clean();
  //  }
  //
  //  @Override
  //  @Test
  //  public void testMemoryCalculation()
  //      throws QueryProcessException, IllegalPathException, StorageEngineException,
  //          StorageGroupNotSetException {
  //    insertRecord("root.sg.d1", 100L);
  //    CreateMultiTimeSeriesPlan plan = new CreateMultiTimeSeriesPlan();
  //    plan.setPaths(
  //        Arrays.asList(
  //            new PartialPath("root.sg.d100.s1"),
  //            new PartialPath("root.sg.d101.s1"),
  //            new PartialPath("root.sg.d102.s1"),
  //            new PartialPath("root.sg.d103.s1"),
  //            new PartialPath("root.sg.d104.s1")));
  //    plan.setDataTypes(
  //        Arrays.asList(
  //            TSDataType.INT64,
  //            TSDataType.INT64,
  //            TSDataType.INT64,
  //            TSDataType.INT64,
  //            TSDataType.INT64));
  //    plan.setEncodings(
  //        Arrays.asList(
  //            TSEncoding.GORILLA,
  //            TSEncoding.GORILLA,
  //            TSEncoding.GORILLA,
  //            TSEncoding.GORILLA,
  //            TSEncoding.GORILLA));
  //    plan.setCompressors(
  //        Arrays.asList(
  //            CompressionType.SNAPPY,
  //            CompressionType.SNAPPY,
  //            CompressionType.SNAPPY,
  //            CompressionType.SNAPPY,
  //            CompressionType.SNAPPY));
  //    executor.processNonQuery(plan);
  //
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.sg"));
  //    assertEquals(62l, storageGroupProcessor.getLastFlushTimeMap().getMemSize(0L));
  //
  //    storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0L, "root.sg.d100");
  //    storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0L, "root.sg.d101");
  //    assertEquals(186l, storageGroupProcessor.getLastFlushTimeMap().getMemSize(0L));
  //
  //    storageGroupProcessor.getLastFlushTimeMap().setOneDeviceFlushedTime(0L, "root.sg.d102", 0L);
  //    HashMap<String, Long> updateMap = new HashMap<>();
  //    updateMap.put("root.sg.d103", 1L);
  //    updateMap.put("root.sg.d100", 1L);
  //    storageGroupProcessor.getLastFlushTimeMap().setMultiDeviceFlushedTime(0L, updateMap);
  //    storageGroupProcessor.getLastFlushTimeMap().updateFlushedTime(0L, "root.sg.d103", 2L);
  //    storageGroupProcessor.getLastFlushTimeMap().updateFlushedTime(0L, "root.sg.d104", 2L);
  //    assertEquals(372L, storageGroupProcessor.getLastFlushTimeMap().getMemSize(0L));
  //  }
  //
  //  @Test
  //  public void testRecoverFlushTime()
  //      throws QueryProcessException, IllegalPathException, StorageEngineException,
  //          StorageGroupNotSetException {
  //    insertData(100);
  //    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
  //    executor.processNonQuery(flushPlan);
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp"));
  //    String deviceId = DeviceIDFactory.getInstance().getDeviceID("root.isp.d1").toStringID();
  //    assertEquals(103L, storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0l,
  // deviceId));
  //
  //    storageGroupProcessor.getLastFlushTimeMap().removePartition(0l);
  //    storageGroupProcessor.getLastFlushTimeMap().checkAndCreateFlushedTimePartition(0l);
  //    assertEquals(103L, storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0l,
  // deviceId));
  //  }
}
