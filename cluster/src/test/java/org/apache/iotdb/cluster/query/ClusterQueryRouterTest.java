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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Before;
import org.junit.Test;

public class ClusterQueryRouterTest extends BaseQueryTest {

  private ClusterQueryRouter clusterQueryRouter;

  @Override
  @Before
  public void setUp() throws MetadataException, QueryProcessException {
    super.setUp();
    clusterQueryRouter = new ClusterQueryRouter(localMetaGroupMember);
  }

  @Test
  public void test() throws StorageEngineException, IOException {
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedPaths(pathList);
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    QueryDataSet dataSet = clusterQueryRouter.rawDataQuery(queryPlan, context);
    checkDataset(dataSet, 0, 100);
  }

  @Test
  public void testAggregation()
      throws StorageEngineException, IOException, QueryProcessException, QueryFilterOptimizationException {
    AggregationPlan plan = new AggregationPlan();
    List<Path> paths = Arrays.asList(
        new Path(TestUtils.getTestSeries(0, 0)),
        new Path(TestUtils.getTestSeries(0, 1)),
        new Path(TestUtils.getTestSeries(0, 2)),
        new Path(TestUtils.getTestSeries(0, 3)),
        new Path(TestUtils.getTestSeries(0, 4)));
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE,
        TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE);
    List<String> aggregations = Arrays.asList(SQLConstant.MIN_TIME, SQLConstant.MAX_VALUE,
        SQLConstant.AVG, SQLConstant.COUNT, SQLConstant.SUM);
    plan.setPaths(paths);
    plan.setDeduplicatedPaths(paths);
    plan.setDataTypes(dataTypes);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setAggregations(aggregations);
    plan.setDeduplicatedAggregations(aggregations);

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    QueryDataSet queryDataSet = clusterQueryRouter.aggregate(plan, context);
    assertTrue(queryDataSet.hasNext());
    RowRecord record = queryDataSet.next();
    List<Field> fields = record.getFields();
    assertEquals(5, fields.size());
    Object[] answers = new Object[] {0,0,0,0,0};
    for (int i = 0; i < 5; i++) {
      assertEquals(answers[i], fields.get(i));
    }
    assertFalse(queryDataSet.hasNext());
  }
}