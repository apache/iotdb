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
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Test;

public class ClusterDataQueryExecutorTest extends BaseQueryTest {

  private ClusterDataQueryExecutor queryExecutor;

  @Test
  public void testNoFilter() throws IOException, StorageEngineException {
    queryExecutor = new ClusterDataQueryExecutor(pathList, dataTypes, null, localMetaGroupMember);
    QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(
        new QueryContext(QueryResourceManager.getInstance().assignQueryId(true)));
    checkDataset(dataSet, 0, 100);
  }

  @Test
  public void testFilter() throws IOException, StorageEngineException {
    IExpression expression = new SingleSeriesExpression(new Path(TestUtils.getTestSeries(0, 0)),
        ValueFilter.gtEq(50.0));
    queryExecutor = new ClusterDataQueryExecutor(pathList, dataTypes, expression,
        localMetaGroupMember);
    QueryDataSet dataSet = queryExecutor.executeWithValueFilter(
        new QueryContext(QueryResourceManager.getInstance().assignQueryId(true)));
    checkDataset(dataSet, 50, 50);
  }

}