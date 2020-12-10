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

import java.io.IOException;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Test;

public class ClusterDataQueryExecutorTest extends BaseQueryTest {

  private ClusterDataQueryExecutor queryExecutor;

  @Test
  public void testNoFilter() throws IOException, StorageEngineException, QueryProcessException {
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPaths(pathList);
    plan.setDeduplicatedDataTypes(dataTypes);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1)));
    checkSequentialDataset(dataSet, 0, 20);
  }

  @Test
  public void testFilter()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    IExpression expression = new SingleSeriesExpression(new PartialPath(TestUtils.getTestSeries(0, 0)),
        ValueFilter.gtEq(5.0));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPaths(pathList);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setExpression(expression);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    QueryDataSet dataSet = queryExecutor.executeWithValueFilter(
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1)));
    checkSequentialDataset(dataSet, 5, 15);
  }

}