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
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.SetSchemaTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

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
  public void testInsertRowPlanWithSchemaTemplate()
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

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("vector");
    schemaNames.add("vector2");
    schemaNames.add("s6");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypesList,
            encodingList,
            compressionTypes);

    IoTDB.metaManager.createSchemaTemplate(plan);
    IoTDB.metaManager.setSchemaTemplate(new SetSchemaTemplatePlan("template1", "root.isp.d1"));

    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);

    InsertRowPlan rowPlan = getInsertVectorRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(rowPlan);

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
