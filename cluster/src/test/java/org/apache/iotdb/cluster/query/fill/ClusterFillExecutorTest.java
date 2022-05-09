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

package org.apache.iotdb.cluster.query.fill;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.executor.fill.ValueFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public class ClusterFillExecutorTest extends BaseQueryTest {

  @Test
  public void testPreviousFill()
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(
        Collections.singletonList(
            new MeasurementPath(TestUtils.getTestSeries(0, 10), TSDataType.DOUBLE)));
    plan.setPaths(plan.getDeduplicatedPaths());
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    Map<TSDataType, IFill> tsDataTypeIFillMap =
        Collections.singletonMap(
            TSDataType.DOUBLE, new PreviousFill(TSDataType.DOUBLE, 0, defaultFillInterval));
    plan.setFillType(tsDataTypeIFillMap);
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    try {
      ClusterFillExecutor fillExecutor;
      QueryDataSet queryDataSet;
      long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
      Object[][] answers =
          new Object[][] {
            new Object[] {null},
            new Object[] {0.0},
            new Object[] {0.0},
            new Object[] {10.0},
            new Object[] {10.0},
          };
      for (int i = 0; i < queryTimes.length; i++) {
        plan.setQueryTime(queryTimes[i]);
        fillExecutor = new ClusterFillExecutor(plan, testMetaMember);
        queryDataSet = fillExecutor.execute(context);
        checkDoubleDataset(queryDataSet, answers[i]);
        assertFalse(queryDataSet.hasNext());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testLinearFill()
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(
        Collections.singletonList(
            new MeasurementPath(TestUtils.getTestSeries(0, 10), TSDataType.DOUBLE)));
    plan.setPaths(plan.getDeduplicatedPaths());
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    Map<TSDataType, IFill> tsDataTypeIFillMap =
        Collections.singletonMap(
            TSDataType.DOUBLE,
            new LinearFill(TSDataType.DOUBLE, 0, defaultFillInterval, defaultFillInterval));
    plan.setFillType(tsDataTypeIFillMap);
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    try {
      ClusterFillExecutor fillExecutor;
      QueryDataSet queryDataSet;
      long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
      Object[][] answers =
          new Object[][] {
            new Object[] {null},
            new Object[] {0.0},
            new Object[] {5.0},
            new Object[] {10.0},
            new Object[] {null},
          };
      for (int i = 0; i < queryTimes.length; i++) {
        plan.setQueryTime(queryTimes[i]);
        fillExecutor = new ClusterFillExecutor(plan, testMetaMember);
        queryDataSet = fillExecutor.execute(context);
        checkDoubleDataset(queryDataSet, answers[i]);
        assertFalse(queryDataSet.hasNext());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testValueFill()
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(
        Collections.singletonList(
            new MeasurementPath(TestUtils.getTestSeries(0, 10), TSDataType.DOUBLE)));
    plan.setPaths(plan.getDeduplicatedPaths());
    double fillValue = 1.0D;
    Map<TSDataType, IFill> tsDataTypeIFillMap =
        Collections.singletonMap(
            TSDataType.DOUBLE, new ValueFill(Double.toString(fillValue), TSDataType.DOUBLE));
    plan.setFillType(tsDataTypeIFillMap);
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    try {
      ClusterFillExecutor fillExecutor;
      QueryDataSet queryDataSet;
      long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
      Object[][] answers =
          new Object[][] {
            new Object[] {1.0D},
            new Object[] {0.0D},
            new Object[] {1.0D},
            new Object[] {10.0D},
            new Object[] {1.0D},
          };
      for (int i = 0; i < queryTimes.length; i++) {
        plan.setQueryTime(queryTimes[i]);
        fillExecutor = new ClusterFillExecutor(plan, testMetaMember);
        queryDataSet = fillExecutor.execute(context);
        checkDoubleDataset(queryDataSet, answers[i]);
        assertFalse(queryDataSet.hasNext());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
