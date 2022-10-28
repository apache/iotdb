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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.filter.executor.PlanExecutor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryWithIDTableTest {
  private final Planner processor = new Planner();

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  Set<String> retSet =
      new HashSet<>(
          Arrays.asList(
              "113\troot.isp.d1.s3\t100003\tINT64",
              "113\troot.isp.d1.s4\t1003\tINT32",
              "113\troot.isp.d1.s5\tfalse\tBOOLEAN",
              "113\troot.isp.d1.s6\tmm3\tTEXT",
              "113\troot.isp.d1.s1\t13.0\tDOUBLE",
              "113\troot.isp.d1.s2\t23.0\tFLOAT"));

  private static String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "SET STORAGE GROUP TO root.other",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.other.d1.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
        "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
        "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
        "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
        "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
        "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
        "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
        "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
        "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
        "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
        "insert into root.other.d1(timestamp,s0) values(2, 3.14)",
      };

  @Before
  public void before() {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();

    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("SHA256");
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
  public void testRawDataQueryAfterFlush()
      throws MetadataException, QueryProcessException, StorageEngineException, InterruptedException,
          QueryFilterOptimizationException, IOException {
    insertDataInDisk();
    insertDataInMemory();

    PlanExecutor executor = new PlanExecutor();
    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      count++;
    }

    assertEquals(8, count);
  }

  @Test
  public void testAggregateQueryAfterFlush()
      throws MetadataException, QueryProcessException, StorageEngineException, InterruptedException,
          QueryFilterOptimizationException, IOException {
    insertDataInDisk();
    insertDataInMemory();

    PlanExecutor executor = new PlanExecutor();
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select count(*) from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      for (Field f : record.getFields()) {
        assertEquals(8L, f.getLongV());
      }
      count++;
    }

    assertEquals(1, count);
  }

  @Test
  public void testGroupByQueryAfterFlush()
      throws MetadataException, QueryProcessException, StorageEngineException, InterruptedException,
          QueryFilterOptimizationException, IOException {
    insertDataInDisk();
    insertDataInMemory();

    PlanExecutor executor = new PlanExecutor();
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select count(*) from root.isp.d1 group by ([10, 114), 10ms)");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    System.out.println(dataSet.getPaths());
    Assert.assertEquals(6, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      for (Field f : record.getFields()) {
        if (count == 0 || count == 10) {
          assertEquals(4L, f.getLongV());
        } else {
          assertEquals(0L, f.getLongV());
        }
      }
      count++;
    }

    assertEquals(11, count);
  }

  @Test
  public void testLastCacheQuery()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    insertDataInMemory();

    PlanExecutor executor = new PlanExecutor();
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select last * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertTrue(retSet.contains(record.toString()));
      count++;
    }

    assertEquals(retSet.size(), count);

    // test it from id table
    assertEquals(
        new TimeValuePair(113L, new TsDouble(13.0d)),
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp.d1.s1"))));
    assertEquals(
        new TimeValuePair(113L, new TsFloat(23.0f)),
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp.d1.s2"))));
    assertEquals(
        new TimeValuePair(113L, new TsLong(100003L)),
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp.d1.s3"))));
    assertEquals(
        new TimeValuePair(113L, new TsInt(1003)),
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp.d1.s4"))));
    assertEquals(
        new TimeValuePair(113L, new TsBoolean(false)),
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp.d1.s5"))));
    assertEquals(
        new TimeValuePair(113L, new TsBinary(new Binary("mm3"))),
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp.d1.s6"))));
  }

  private void insertDataInMemory() throws IllegalPathException, QueryProcessException {
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
      ((double[]) columns[0])[r] = 10.0 + r;
      ((float[]) columns[1])[r] = 20 + r;
      ((long[]) columns[2])[r] = 100000 + r;
      ((int[]) columns[3])[r] = 1000 + r;
      ((boolean[]) columns[4])[r] = false;
      ((Binary[]) columns[5])[r] = new Binary("mm" + r);
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
  }

  private void insertDataInDisk()
      throws IllegalPathException, QueryProcessException, StorageGroupNotSetException,
          StorageEngineException {
    long[] times = new long[] {10L, 11L, 12L, 13L};
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

    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);
  }
}
