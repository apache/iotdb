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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterTimeGeneratorTest extends BaseQueryTest {

  @Test
  public void test()
      throws StorageEngineException, IOException, IllegalPathException, QueryProcessException {
    RawDataQueryPlan dataQueryPlan = new RawDataQueryPlan();
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      IExpression expression =
          BinaryExpression.and(
              new SingleSeriesExpression(
                  new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.gtEq(3.0)),
              new SingleSeriesExpression(
                  new PartialPath(TestUtils.getTestSeries(1, 1)), ValueFilter.ltEq(8.0)));
      dataQueryPlan.setExpression(expression);
      dataQueryPlan.addDeduplicatedPaths(new PartialPath(TestUtils.getTestSeries(0, 0)));
      dataQueryPlan.addDeduplicatedPaths(new PartialPath(TestUtils.getTestSeries(1, 1)));

      ClusterTimeGenerator timeGenerator =
          new ClusterTimeGenerator(expression, context, testMetaMember, dataQueryPlan);
      for (int i = 3; i <= 8; i++) {
        assertTrue(timeGenerator.hasNext());
        assertEquals(i, timeGenerator.next());
      }
      assertFalse(timeGenerator.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
