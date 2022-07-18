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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
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
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IDTableRestartTest {

  private final Planner processor = new Planner();

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  private boolean isEnableIDTableLogFile = false;

  @Before
  public void before() {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    isEnableIDTableLogFile = IoTDBDescriptor.getInstance().getConfig().isEnableIDTableLogFile();

    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("SHA256");
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);

    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRawDataQueryAfterRestart() throws Exception {
    insertDataInMemoryWithTablet();
    insertDataInMemoryWithRecord();

    // restart
    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    PlanExecutor executor = new PlanExecutor();
    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      System.out.println(record);
      count++;
    }

    assertEquals(5, count);

    // flush and test again
    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    executor = new PlanExecutor();
    queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      System.out.println(record);
      count++;
    }

    assertEquals(5, count);
  }

  private void insertDataInMemoryWithRecord() throws IllegalPathException, QueryProcessException {
    long time = 100L;
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

    InsertRowPlan insertRowPlan =
        new InsertRowPlan(
            new PartialPath("root.isp.d1"),
            time,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            columns);

    PlanExecutor executor = new PlanExecutor();
    executor.insert(insertRowPlan);
  }

  private void insertDataInMemoryWithTablet() throws IllegalPathException, QueryProcessException {
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
}
