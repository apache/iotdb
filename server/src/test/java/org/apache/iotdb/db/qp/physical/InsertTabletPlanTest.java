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
package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan.PhysicalPlanType;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InsertTabletPlanTest {

  private final Planner processor = new Planner();

  @Before
  public void before() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsertTabletPlan()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
      ((int[]) columns[3])[r] = 100;
      ((boolean[]) columns[4])[r] = false;
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath("root.isp.d1"),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertTabletPlanWithAlignedTimeseries()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertTabletPlan tabletPlan = getInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    Assert.assertEquals("[$#$0, $#$1, s6]", Arrays.toString(tabletPlan.getMeasurementMNodes()));

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      System.out.println(record);
      Assert.assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertNullableTabletPlanWithAlignedTimeseries()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertTabletPlan tabletPlan = getInsertTabletPlan();
    tabletPlan.setBitMaps(new BitMap[6]);
    BitMap[] bitMaps = tabletPlan.getBitMaps();
    for (int i = 0; i < 4; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(4);
      }
      bitMaps[i].mark(i);
    }
    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    Assert.assertEquals("[$#$0, $#$1, s6]", Arrays.toString(tabletPlan.getMeasurementMNodes()));
    System.out.println(Arrays.toString(tabletPlan.getMeasurementMNodes()));

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      System.out.println(record);
      Assert.assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertTabletPlanWithDeviceTemplate()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    IoTDB.metaManager.createDeviceTemplate(plan);
    IoTDB.metaManager.setDeviceTemplate(new SetDeviceTemplatePlan("template1", "root.isp"));

    InsertTabletPlan tabletPlan = getInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();

    // nothing can be found when we not insert data
    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(0, dataSet.getPaths().size());

    executor.insertTablet(tabletPlan);

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(6, record.getFields().size());
    }
  }

  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    List<String> v1 = new ArrayList<>();
    v1.add("s1");
    v1.add("s2");
    v1.add("s3");
    measurementList.add(v1);
    List<String> v2 = new ArrayList<>();
    v2.add("s4");
    v2.add("s5");
    measurementList.add(v2);
    measurementList.add(Collections.singletonList("s6"));

    List<List<TSDataType>> dataTypesList = new ArrayList<>();
    List<TSDataType> d1 = new ArrayList<>();
    d1.add(TSDataType.DOUBLE);
    d1.add(TSDataType.FLOAT);
    d1.add(TSDataType.INT64);
    dataTypesList.add(d1);
    List<TSDataType> d2 = new ArrayList<>();
    d2.add(TSDataType.INT32);
    d2.add(TSDataType.BOOLEAN);
    dataTypesList.add(d2);
    dataTypesList.add(Collections.singletonList(TSDataType.TEXT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    List<TSEncoding> e1 = new ArrayList<>();
    e1.add(TSEncoding.PLAIN);
    e1.add(TSEncoding.PLAIN);
    e1.add(TSEncoding.PLAIN);
    encodingList.add(e1);
    List<TSEncoding> e2 = new ArrayList<>();
    e2.add(TSEncoding.PLAIN);
    e2.add(TSEncoding.PLAIN);
    encodingList.add(e2);
    encodingList.add(Collections.singletonList(TSEncoding.PLAIN));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    return new CreateTemplatePlan(
        "template1", measurementList, dataTypesList, encodingList, compressionTypes);
  }

  @Test
  public void testInsertTabletPlanWithDeviceTemplateAndAutoCreateSchema()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    IoTDB.metaManager.createDeviceTemplate(plan);
    IoTDB.metaManager.setDeviceTemplate(new SetDeviceTemplatePlan("template1", "root.isp"));
    InsertTabletPlan tabletPlan = getInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(6, record.getFields().size());
    }

    // test recover
    EnvironmentUtils.stopDaemon();
    IoTDB.metaManager.clear();
    // wait for close
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
    EnvironmentUtils.activeDaemon();

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertTabletSerialization() throws IllegalPathException, QueryProcessException {
    InsertTabletPlan plan1 = getInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(plan1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    plan1.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PhysicalPlanType.BATCHINSERT.ordinal(), byteBuffer.get());

    InsertTabletPlan plan2 = new InsertTabletPlan();
    plan2.deserialize(byteBuffer);

    Assert.assertEquals(plan1, plan2);
  }

  @Test
  public void testInsertTabletWithBitMapsSerialization()
      throws IllegalPathException, QueryProcessException {
    InsertTabletPlan plan1 = getInsertTabletPlan();
    plan1.setBitMaps(new BitMap[6]);
    BitMap[] bitMaps = plan1.getBitMaps();
    for (int i = 0; i < 4; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(4);
      }
      bitMaps[i].mark(i);
    }
    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(plan1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    plan1.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PhysicalPlanType.BATCHINSERT.ordinal(), byteBuffer.get());

    InsertTabletPlan plan2 = new InsertTabletPlan();
    plan2.deserialize(byteBuffer);

    Assert.assertEquals(plan1, plan2);
  }

  private InsertTabletPlan getInsertTabletPlan() throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
      ((int[]) columns[3])[r] = 100;
      ((boolean[]) columns[4])[r] = false;
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath("root.isp.d1"),
            new String[] {"(s1,s2,s3)", "(s4,s5)", "s6"},
            dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    return tabletPlan;
  }
}
