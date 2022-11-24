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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

@Ignore
@Deprecated
public class ListDataSetTest {

  private final IPlanExecutor queryExecutor = new PlanExecutor();
  private final Planner processor = new Planner();

  private final String[] sqls = {
    "CREATE DATABASE root.vehicle",
    "CREATE DATABASE root.test",
    "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.test.d1.`\"s3+xy\"` WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE ALIGNED TIMESERIES root.test.d2(s1 DOUBLE, s2 BOOLEAN)"
  };

  public ListDataSetTest() throws QueryProcessException {}

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    for (String sql : sqls) {
      queryExecutor.processNonQuery(processor.parseSQLToPhysicalPlan(sql));
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void showStorageGroups()
      throws QueryProcessException, TException, StorageEngineException,
          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
          SQLException {
    String[] results = new String[] {"0\troot.test", "0\troot.vehicle"};
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("SHOW DATABASES");
    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof ListDataSet);
    Assert.assertEquals("[database]", dataSet.getPaths().toString());
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(results[i], record.toString());
      i++;
    }
  }

  @Test
  public void showChildPaths()
      throws QueryProcessException, TException, StorageEngineException,
          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
          SQLException {
    String[] results =
        new String[] {
          "0\troot.test.d0\tDEVICE", "0\troot.test.d1\tDEVICE", "0\troot.test.d2\tDEVICE"
        };
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("show child paths root.test");
    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof ListDataSet);
    Assert.assertEquals("[child paths, node types]", dataSet.getPaths().toString());
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(results[i], record.toString());
      i++;
    }
  }

  @Test
  public void showDevices()
      throws QueryProcessException, TException, StorageEngineException,
          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
          SQLException {
    String[] results =
        new String[] {
          "0\troot.test.d0\tfalse",
          "0\troot.test.d1\tfalse",
          "0\troot.test.d2\ttrue",
          "0\troot.vehicle.d0\tfalse"
        };
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("show devices");
    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof ShowDevicesDataSet);
    Assert.assertEquals("[devices, isAligned]", dataSet.getPaths().toString());
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(results[i], record.toString());
      i++;
    }
  }

  @Test
  public void showDevicesWithSg()
      throws QueryProcessException, TException, StorageEngineException,
          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
          SQLException {
    String[] results =
        new String[] {
          "0\troot.test.d0\troot.test\tfalse",
          "0\troot.test.d1\troot.test\tfalse",
          "0\troot.test.d2\troot.test\ttrue",
          "0\troot.vehicle.d0\troot.vehicle\tfalse"
        };
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("show devices with database");
    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof ShowDevicesDataSet);
    Assert.assertEquals("devices", dataSet.getPaths().get(0).toString());
    Assert.assertEquals("database", dataSet.getPaths().get(1).toString());
    Assert.assertEquals("isAligned", dataSet.getPaths().get(2).toString());
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(results[i], record.toString());
      i++;
    }
  }
}
