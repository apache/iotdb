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
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class InsertRowPlanTest {

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
  public void testInsertRowPlan()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
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

    InsertRowPlan rowPlan =
        new InsertRowPlan(
            new PartialPath("root.isp.d1"),
            time,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            columns);

    PlanExecutor executor = new PlanExecutor();
    executor.insert(rowPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(6, record.getFields().size());
    }
  }

  @Test
  public void testInsertRowPlanWithAlignedTimeseries()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertRowPlan vectorRowPlan = getInsertVectorRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(vectorRowPlan);

    Assert.assertEquals(
        "[vector, vector, vector]", Arrays.toString(vectorRowPlan.getMeasurementMNodes()));

    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1.vector");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(1, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(3, record.getFields().size());
    }
  }

  @Test
  public void testInsertRowSerialization() throws IllegalPathException, QueryProcessException {
    InsertRowPlan plan1 = getInsertVectorRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(plan1);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    plan1.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(PhysicalPlanType.INSERT.ordinal(), byteBuffer.get());

    InsertRowPlan plan2 = new InsertRowPlan();
    plan2.deserialize(byteBuffer);

    executor.insert(plan2);
    Assert.assertEquals(plan1, plan2);
  }

  private InsertRowPlan getInsertVectorRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64};

    String[] columns = new String[3];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";

    return new InsertRowPlan(
        new PartialPath("root.isp.d1.vector"),
        time,
        new String[] {"s1", "s2", "s3"},
        dataTypes,
        columns,
        true);
  }
}
