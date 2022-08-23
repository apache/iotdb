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
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LastQueryWithIDTable {
  private final Planner processor = new Planner();

  private boolean isEnableIDTable = false;

  private boolean originalEnableCache = false;

  private String originalDeviceIDTransformationMethod = null;

  Set<String> retSet =
      new HashSet<>(
          Arrays.asList(
              "113\troot.isp1.d1.s3\t100003\tINT64",
              "113\troot.isp1.d1.s4\t1003\tINT32",
              "113\troot.isp1.d1.s5\tfalse\tBOOLEAN",
              "113\troot.isp1.d1.s6\tmm3\tTEXT",
              "113\troot.isp1.d1.s1\t13.0\tDOUBLE",
              "113\troot.isp1.d1.s2\t23.0\tFLOAT",
              "113\troot.isp2.d1.s3\t100003\tINT64",
              "113\troot.isp2.d1.s4\t1003\tINT32",
              "113\troot.isp2.d1.s5\tfalse\tBOOLEAN",
              "113\troot.isp2.d1.s6\tmm3\tTEXT",
              "113\troot.isp2.d1.s1\t13.0\tDOUBLE",
              "113\troot.isp2.d1.s2\t23.0\tFLOAT"));

  @Before
  public void before() {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    originalEnableCache = IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();
    IoTDBDescriptor.getInstance().getConfig().setEnableLastCache(false);

    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement_INT");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    IoTDBDescriptor.getInstance().getConfig().setEnableLastCache(originalEnableCache);
  }

  @Test
  public void testLastCacheQueryWithoutCache()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {

    insertDataInMemory("root.isp1.d1");
    insertDataInMemory("root.isp2.d1");

    PlanExecutor executor = new PlanExecutor();
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select last * from root.isp1.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertTrue(retSet.contains(record.toString()));
      count++;
    }

    assertEquals(6, count);

    executor = new PlanExecutor();
    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select last * from root.isp2.d1");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertTrue(retSet.contains(record.toString()));
      count++;
    }

    assertEquals(6, count);

    // flush and test again
    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select last * from root.isp1.d1");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertTrue(retSet.contains(record.toString()));
      count++;
    }
    assertEquals(6, count);

    // assert id table is not refresh
    assertNull(
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp1.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp1.d1.s1"))));

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select last * from root.isp2.d1");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(3, dataSet.getPaths().size());
    count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertTrue(retSet.contains(record.toString()));
      count++;
    }
    assertEquals(6, count);

    // assert id table is not refresh
    assertNull(
        IDTableManager.getInstance()
            .getIDTable(new PartialPath("root.isp2.d1"))
            .getLastCache(new TimeseriesID(new PartialPath("root.isp2.d1.s1"))));

    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select last * from root.isp2.d2");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(0, dataSet.getPaths().size());
  }

  private void insertDataInMemory(String path) throws IllegalPathException, QueryProcessException {
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
            new PartialPath(path), new String[] {"s1", "s2", "s3", "s4", "s5", "s6"}, dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);
  }
}
