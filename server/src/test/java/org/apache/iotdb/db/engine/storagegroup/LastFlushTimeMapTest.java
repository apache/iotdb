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

public class LastFlushTimeMapTest {
  //  protected PlanExecutor executor = new PlanExecutor();
  //
  //  protected final Planner processor = new Planner();
  //
  //  public LastFlushTimeMapTest() throws QueryProcessException {}
  //
  //  @Before
  //  public void before() {
  //    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
  //    EnvironmentUtils.envSetUp();
  //  }
  //
  //  @Test
  //  public void testSequenceInsert()
  //      throws MetadataException, QueryProcessException, StorageEngineException {
  //    insertData(0);
  //    insertData(10);
  //    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
  //    executor.processNonQuery(flushPlan);
  //
  //    insertData(20);
  //
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
  //    assertEquals(2, storageGroupProcessor.getSequenceFileList().size());
  //    assertEquals(0, storageGroupProcessor.getUnSequenceFileList().size());
  //  }
  //
  //  @Test
  //  public void testUnSequenceInsert()
  //      throws MetadataException, QueryProcessException, StorageEngineException {
  //    insertData(100);
  //    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
  //    executor.processNonQuery(flushPlan);
  //
  //    insertData(20);
  //
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
  //    assertEquals(1, storageGroupProcessor.getSequenceFileList().size());
  //    assertEquals(1, storageGroupProcessor.getUnSequenceFileList().size());
  //  }
  //
  //  @Test
  //  public void testSequenceAndUnSequenceInsert()
  //      throws MetadataException, QueryProcessException, StorageEngineException {
  //    // sequence
  //    insertData(100);
  //    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
  //    executor.processNonQuery(flushPlan);
  //
  //    // sequence
  //    insertData(120);
  //    executor.processNonQuery(flushPlan);
  //
  //    // unsequence
  //    insertData(20);
  //    // sequence
  //    insertData(130);
  //    executor.processNonQuery(flushPlan);
  //
  //    // sequence
  //    insertData(150);
  //    // unsequence
  //    insertData(90);
  //
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
  //    assertEquals(4, storageGroupProcessor.getSequenceFileList().size());
  //    assertEquals(2, storageGroupProcessor.getUnSequenceFileList().size());
  //    assertEquals(1, storageGroupProcessor.getWorkSequenceTsFileProcessors().size());
  //    assertEquals(1, storageGroupProcessor.getWorkUnsequenceTsFileProcessors().size());
  //  }
  //
  //  @Test
  //  public void testDeletePartition()
  //      throws MetadataException, QueryProcessException, StorageEngineException {
  //    insertData(100);
  //    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
  //    executor.processNonQuery(flushPlan);
  //    insertData(20);
  //    insertData(120);
  //
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
  //
  //    assertEquals(
  //        103L, storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0L, "root.isp.d1"));
  //    assertEquals(
  //        103L, storageGroupProcessor.getLastFlushTimeMap().getGlobalFlushedTime("root.isp.d1"));
  //
  //    // delete time partition
  //    Set<Long> deletedPartition = new HashSet<>();
  //    deletedPartition.add(0L);
  //    DeletePartitionPlan deletePartitionPlan =
  //        new DeletePartitionPlan(new PartialPath("root.isp"), deletedPartition);
  //    executor.processNonQuery(deletePartitionPlan);
  //
  //    assertEquals(
  //        123L, storageGroupProcessor.getLastFlushTimeMap().getGlobalFlushedTime("root.isp.d1"));
  //  }
  //
  //  @Test
  //  public void testMemoryCalculation()
  //      throws QueryProcessException, IllegalPathException, StorageEngineException,
  //          StorageGroupNotSetException {
  //    insertRecord("root.sg.d1", 100L);
  //    DataRegion storageGroupProcessor =
  //        StorageEngine.getInstance().getProcessor(new PartialPath("root.sg"));
  //    assertEquals(98l, storageGroupProcessor.getLastFlushTimeMap().getMemSize(0L));
  //
  //    storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0L, "root.sg.d100");
  //    storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0L, "root.sg.d101");
  //    assertEquals(302l, storageGroupProcessor.getLastFlushTimeMap().getMemSize(0L));
  //
  //    storageGroupProcessor.getLastFlushTimeMap().setOneDeviceFlushedTime(0L, "root.sg.d102", 0L);
  //    HashMap<String, Long> updateMap = new HashMap<>();
  //    updateMap.put("root.sg.d103", 1L);
  //    updateMap.put("root.sg.d100", 1L);
  //    storageGroupProcessor.getLastFlushTimeMap().setMultiDeviceFlushedTime(0L, updateMap);
  //    storageGroupProcessor.getLastFlushTimeMap().updateFlushedTime(0L, "root.sg.d103", 2L);
  //    storageGroupProcessor.getLastFlushTimeMap().updateFlushedTime(0L, "root.sg.d104", 2L);
  //    assertEquals(608L, storageGroupProcessor.getLastFlushTimeMap().getMemSize(0L));
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
  //    assertEquals(
  //        103L, storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0l, "root.isp.d1"));
  //
  //    storageGroupProcessor.getLastFlushTimeMap().removePartition(0l);
  //    storageGroupProcessor.getLastFlushTimeMap().checkAndCreateFlushedTimePartition(0l);
  //    assertEquals(
  //        103L, storageGroupProcessor.getLastFlushTimeMap().getFlushedTime(0l, "root.isp.d1"));
  //  }
  //
  //  @After
  //  public void clean() throws IOException, StorageEngineException {
  //    EnvironmentUtils.cleanEnv();
  //  }
  //
  //  protected void insertData(long initTime) throws IllegalPathException, QueryProcessException {
  //
  //    long[] times = new long[] {initTime, initTime + 1, initTime + 2, initTime + 3};
  //    List<Integer> dataTypes = new ArrayList<>();
  //    dataTypes.add(TSDataType.DOUBLE.ordinal());
  //    dataTypes.add(TSDataType.FLOAT.ordinal());
  //    dataTypes.add(TSDataType.INT64.ordinal());
  //    dataTypes.add(TSDataType.INT32.ordinal());
  //    dataTypes.add(TSDataType.BOOLEAN.ordinal());
  //    dataTypes.add(TSDataType.TEXT.ordinal());
  //
  //    Object[] columns = new Object[6];
  //    columns[0] = new double[4];
  //    columns[1] = new float[4];
  //    columns[2] = new long[4];
  //    columns[3] = new int[4];
  //    columns[4] = new boolean[4];
  //    columns[5] = new Binary[4];
  //
  //    for (int r = 0; r < 4; r++) {
  //      ((double[]) columns[0])[r] = 10.0 + r;
  //      ((float[]) columns[1])[r] = 20 + r;
  //      ((long[]) columns[2])[r] = 100000 + r;
  //      ((int[]) columns[3])[r] = 1000 + r;
  //      ((boolean[]) columns[4])[r] = false;
  //      ((Binary[]) columns[5])[r] = new Binary("mm" + r);
  //    }
  //
  //    InsertTabletPlan tabletPlan =
  //        new InsertTabletPlan(
  //            new PartialPath("root.isp.d1"),
  //            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
  //            dataTypes);
  //    tabletPlan.setTimes(times);
  //    tabletPlan.setColumns(columns);
  //    tabletPlan.setRowCount(times.length);
  //
  //    executor.insertTablet(tabletPlan);
  //  }
  //
  //  protected void insertRecord(String devicePath, long time)
  //      throws IllegalPathException, QueryProcessException {
  //    InsertRowPlan insertRowPlan =
  //        new InsertRowPlan(
  //            new PartialPath(devicePath),
  //            time,
  //            new String[] {"s1", "s2", "s3"},
  //            new TSDataType[] {TSDataType.INT32, TSDataType.INT32, TSDataType.INT32},
  //            new String[] {"1", "1", "1"});
  //
  //    executor.insert(insertRowPlan);
  //  }
}
