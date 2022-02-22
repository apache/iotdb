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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ClusterDataQueryExecutorTest extends BaseQueryTest {

  private ClusterDataQueryExecutor queryExecutor;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testNoFilter() throws IOException, StorageEngineException {
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      checkSequentialDataset(dataSet, 0, 20);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilter()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    IExpression expression =
        new SingleSeriesExpression(
            new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.gtEq(5.0));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setExpression(expression);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithValueFilter(context);
      checkSequentialDataset(dataSet, 5, 15);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testNoFilterWithRedirect() throws StorageEngineException {
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setEnableRedirect(true);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      assertNull(dataSet.getEndPoint());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilterWithValueFilterRedirect()
      throws StorageEngineException, QueryProcessException, IllegalPathException {
    IExpression expression =
        new SingleSeriesExpression(
            new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.gtEq(5.0));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setExpression(expression);
    plan.setEnableRedirect(true);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithValueFilter(context);
      assertNull(dataSet.getEndPoint());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilterWithTimeFilterRedirect()
      throws StorageEngineException, QueryProcessException {
    IExpression expression =
        new GlobalTimeExpression(new AndFilter(TimeFilter.gtEq(5), TimeFilter.ltEq(10)));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList.subList(0, 1));
    plan.setExpression(expression);
    plan.setEnableRedirect(true);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      assertEquals("ip:port=0.0.0.0:6667", dataSet.getEndPoint().toString());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test // IOTDB-2219
  public void testQueryInMemory()
      throws IOException, StorageEngineException, MetadataException, QueryProcessException {
    PlanExecutor planExecutor = new PlanExecutor();
    MeasurementPath[] paths =
        new MeasurementPath[] {
          new MeasurementPath(
              TestUtils.getTestSg(100),
              TestUtils.getTestMeasurement(0),
              new MeasurementSchema(TestUtils.getTestMeasurement(0), TSDataType.DOUBLE)),
          new MeasurementPath(
              TestUtils.getTestSg(100),
              TestUtils.getTestMeasurement(1),
              new MeasurementSchema(TestUtils.getTestMeasurement(1), TSDataType.DOUBLE)),
          new MeasurementPath(
              TestUtils.getTestSg(100),
              TestUtils.getTestMeasurement(2),
              new MeasurementSchema(TestUtils.getTestMeasurement(2), TSDataType.DOUBLE)),
        };
    String[] measurements =
        new String[] {
          TestUtils.getTestMeasurement(0),
          TestUtils.getTestMeasurement(1),
          TestUtils.getTestMeasurement(2)
        };
    IMeasurementMNode[] schemas =
        new IMeasurementMNode[] {
          TestUtils.getTestMeasurementMNode(0),
          TestUtils.getTestMeasurementMNode(1),
          TestUtils.getTestMeasurementMNode(2)
        };
    TSDataType[] dataTypes =
        new TSDataType[] {TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE};
    Object[] values = new Object[] {1.0, 2.0, 3.0};

    // set storage group
    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan();
    setStorageGroupPlan.setPath(new PartialPath(TestUtils.getTestSg(100)));
    planExecutor.setStorageGroup(setStorageGroupPlan);

    // insert data
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDevicePath(new PartialPath(TestUtils.getTestSg(100)));
    insertPlan.setMeasurements(measurements);
    insertPlan.setMeasurementMNodes(schemas);
    insertPlan.setDataTypes(dataTypes);
    insertPlan.setNeedInferType(true);
    insertPlan.setTime(0);
    insertPlan.setValues(values);

    planExecutor.processNonQuery(insertPlan);

    // query data
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedPaths(Arrays.asList(paths));
    queryExecutor = new ClusterDataQueryExecutor(queryPlan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      Assert.assertEquals(values.length, fields.size());
      for (int i = 0; i < values.length; i++) {
        Assert.assertEquals(String.valueOf(values[i]), fields.get(i).getStringValue());
      }
      assertFalse(dataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
