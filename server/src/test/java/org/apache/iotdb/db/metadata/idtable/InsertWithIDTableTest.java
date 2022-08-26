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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan.PhysicalPlanType;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
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

import static org.junit.Assert.assertEquals;

public class InsertWithIDTableTest {
  private final Planner processor = new Planner();

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  @Before
  public void before() {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();

    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
  }

  @Test
  public void testInsertRowPlan()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertRowPlan rowPlan = getInsertRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(rowPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    int count = 0;
    assertEquals(6, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
      count++;
    }

    assertEquals(1, count);
  }

  @Test
  public void testInsertRowPlanWithAlignedTimeseries()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertRowPlan vectorRowPlan = getInsertAlignedRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(vectorRowPlan);

    assertEquals("[s1, s2, s3]", Arrays.toString(vectorRowPlan.getMeasurementMNodes()));

    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1.GPS");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    int count = 0;
    assertEquals(1, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(3, record.getFields().size());
      count++;
    }

    assertEquals(1, count);
  }

  @Test
  public void testInsertRowPlanWithSchemaTemplate()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    List<List<String>> measurementList = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      measurementList.add(Collections.singletonList("s" + i));
    }

    List<List<TSDataType>> dataTypesList = new ArrayList<>();
    dataTypesList.add(Collections.singletonList(TSDataType.DOUBLE));
    dataTypesList.add(Collections.singletonList(TSDataType.FLOAT));
    dataTypesList.add(Collections.singletonList(TSDataType.INT64));
    dataTypesList.add(Collections.singletonList(TSDataType.INT32));
    dataTypesList.add(Collections.singletonList(TSDataType.BOOLEAN));
    dataTypesList.add(Collections.singletonList(TSDataType.TEXT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      encodingList.add(Collections.singletonList(TSEncoding.PLAIN));
    }

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    }

    List<String> schemaNames = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      schemaNames.add("s" + i);
    }

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypesList,
            encodingList,
            compressionTypes);

    IoTDB.schemaProcessor.createSchemaTemplate(plan);
    IoTDB.schemaProcessor.setSchemaTemplate(new SetTemplatePlan("template1", "root.isp.d1"));

    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);

    InsertRowPlan rowPlan = getInsertRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(rowPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(6, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertRowSerialization() throws IllegalPathException, QueryProcessException {
    InsertRowPlan plan1 = getInsertAlignedRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(plan1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    plan1.serialize(byteBuffer);
    byteBuffer.flip();

    assertEquals(PhysicalPlanType.INSERT.ordinal(), byteBuffer.get());

    InsertRowPlan plan2 = new InsertRowPlan();
    plan2.deserialize(byteBuffer);
    plan2.setDevicePath(new PartialPath("root.isp.d1.GPS"));

    executor.insert(plan2);
    assertEquals(plan1, plan2);
  }

  @Test
  public void testInsertRowPlanWithSchemaTemplateFormer()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      List<CompressionType> compressorList = new ArrayList<>();
      for (int j = 0; j < 3; j++) {
        compressorList.add(CompressionType.SNAPPY);
      }
      compressionTypes.add(compressorList);
    }

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1", measurementList, dataTypesList, encodingList, compressionTypes);

    IoTDB.schemaProcessor.createSchemaTemplate(plan);
    IoTDB.schemaProcessor.setSchemaTemplate(new SetTemplatePlan("template1", "root.isp.d1.GPS"));

    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);

    InsertRowPlan rowPlan = getInsertAlignedRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(rowPlan);

    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select s1 from root.isp.d1.GPS");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(1, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      count++;
      RowRecord record = dataSet.next();
      assertEquals(1, record.getFields().size());
    }
    assertEquals(1, count);
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
    assertEquals(6, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertNullableTabletPlan()
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
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath("root.isp.d1"),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    tabletPlan.setBitMaps(bitMaps);

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(6, dataSet.getPaths().size());
    int rowNum = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
      List<Field> fields = record.getFields();
      for (int i = 0; i < 6; ++i) {
        if (i % times.length == rowNum) {
          Assert.assertNull(fields.get(i));
        } else {
          Assert.assertNotNull(fields.get(i));
        }
      }
      rowNum++;
    }

    assertEquals(4, rowNum);
  }

  @Test
  public void testInsertNullableTabletPlanWithAlignedTimeseries()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertTabletPlan tabletPlan = getAlignedInsertTabletPlan();
    tabletPlan.setBitMaps(new BitMap[3]);
    BitMap[] bitMaps = tabletPlan.getBitMaps();
    for (int i = 0; i < 3; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(4);
      }
      bitMaps[i].mark(i);
    }
    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    assertEquals("[s1, s2, s3]", Arrays.toString(tabletPlan.getMeasurementMNodes()));

    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select ** from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(1, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(3, record.getFields().size());
    }
  }

  @Test
  public void testInsertTabletSerialization() throws IllegalPathException, QueryProcessException {
    InsertTabletPlan plan1 = getAlignedInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(plan1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    plan1.serialize(byteBuffer);
    byteBuffer.flip();

    assertEquals(PhysicalPlanType.BATCHINSERT.ordinal(), byteBuffer.get());

    InsertTabletPlan plan2 = new InsertTabletPlan();
    plan2.deserialize(byteBuffer);
    plan2.setDevicePath(new PartialPath("root.isp.d1.vector"));
    executor.insertTablet(plan2);

    assertEquals(plan1, plan2);
  }

  @Test
  public void testInsertTabletWithBitMapsSerialization()
      throws IllegalPathException, QueryProcessException {
    InsertTabletPlan plan1 = getAlignedInsertTabletPlan();
    plan1.setBitMaps(new BitMap[3]);
    BitMap[] bitMaps = plan1.getBitMaps();
    for (int i = 0; i < 3; i++) {
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

    assertEquals(PhysicalPlanType.BATCHINSERT.ordinal(), byteBuffer.get());

    InsertTabletPlan plan2 = new InsertTabletPlan();
    plan2.deserialize(byteBuffer);
    plan2.setDevicePath(new PartialPath("root.isp.d1.vector"));
    executor.insertTablet(plan2);

    assertEquals(plan1, plan2);
  }

  @Test
  public void testInsertTabletPlanWithSchemaTemplateAndAutoCreateSchema()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    IoTDB.schemaProcessor.createSchemaTemplate(plan);
    IoTDB.schemaProcessor.setSchemaTemplate(new SetTemplatePlan("template1", "root.isp.d1"));
    InsertTabletPlan tabletPlan = getAlignedInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);

    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select ** from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
    }

    // test recover
    EnvironmentUtils.stopDaemon();
    IoTDB.configManager.clear();
    // wait for close
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
    EnvironmentUtils.activeDaemon();

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select ** from root.isp.d1");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertTabletPlanWithSchemaTemplate()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    IoTDB.schemaProcessor.createSchemaTemplate(plan);
    IoTDB.schemaProcessor.setSchemaTemplate(new SetTemplatePlan("template1", "root.isp.d1"));

    InsertTabletPlan tabletPlan = getAlignedInsertTabletPlan();

    PlanExecutor executor = new PlanExecutor();

    // nothing can be found when we not insert data
    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select ** from root.isp");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(0, dataSet.getPaths().size());

    executor.insertTablet(tabletPlan);

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select ** from root.isp");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(3, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(6, record.getFields().size());
    }
  }

  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    List<String> v1 = new ArrayList<>();
    v1.add("vector.s1");
    v1.add("vector.s2");
    v1.add("vector.s3");
    measurementList.add(v1);
    List<String> v2 = new ArrayList<>();
    v2.add("vector2.s4");
    v2.add("vector2.s5");
    measurementList.add(v2);
    measurementList.add(Collections.singletonList("vector3.s6"));

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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> c1 = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      c1.add(CompressionType.SNAPPY);
    }
    List<CompressionType> c2 = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      c2.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(c1);
    compressionTypes.add(c2);
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("vector");
    schemaNames.add("vector2");
    schemaNames.add("s6");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypesList, encodingList, compressionTypes);
  }

  private InsertRowPlan getInsertRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath("root.isp.d1"),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
  }

  private InsertRowPlan getInsertAlignedRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64};

    String[] columns = new String[3];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";

    return new InsertRowPlan(
        new PartialPath("root.isp.d1.GPS"),
        time,
        new String[] {"s1", "s2", "s3"},
        dataTypes,
        columns,
        true);
  }

  private InsertTabletPlan getAlignedInsertTabletPlan() throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[3];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath("root.isp.d1.vector"), new String[] {"s1", "s2", "s3"}, dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    tabletPlan.setAligned(true);
    return tabletPlan;
  }

  @Test
  public void testInsertMultiTabletPlan()
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

    List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      InsertTabletPlan tabletPlan =
          new InsertTabletPlan(
              new PartialPath("root.multi.d" + i),
              new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
              dataTypes);
      tabletPlan.setTimes(times);
      tabletPlan.setColumns(columns);
      tabletPlan.setRowCount(times.length);
      insertTabletPlanList.add(tabletPlan);
    }
    PlanExecutor executor = new PlanExecutor();

    InsertMultiTabletsPlan insertMultiTabletsPlan =
        new InsertMultiTabletsPlan(insertTabletPlanList);

    executor.insertTablet(insertMultiTabletsPlan);
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.multi.**");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(60, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(60, record.getFields().size());
    }

    // query for records that do not exist
    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.multi.d11");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(0, dataSet.getPaths().size());
  }
}
