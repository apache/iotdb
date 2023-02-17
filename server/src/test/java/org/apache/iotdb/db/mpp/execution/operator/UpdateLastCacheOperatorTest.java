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
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.mpp.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UpdateLastCacheOperatorTest {

  private static final String SERIES_SCAN_OPERATOR_TEST_SG = "root.UpdateLastCacheOperator";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, SERIES_SCAN_OPERATOR_TEST_SG);
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void testUpdateLastCacheOperatorTestWithoutTimeFilter() {
    try {
      List<Aggregator> aggregators = LastQueryUtil.createAggregators(TSDataType.INT32);
      UpdateLastCacheOperator updateLastCacheOperator =
          initUpdateLastCacheOperator(aggregators, null, false, null);

      assertTrue(updateLastCacheOperator.isBlocked().isDone());
      assertTrue(updateLastCacheOperator.hasNext());
      TsBlock result = updateLastCacheOperator.next();
      assertEquals(1, result.getPositionCount());
      assertEquals(3, result.getValueColumnCount());

      assertEquals(499, result.getTimeByIndex(0));
      assertEquals(
          SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0",
          result.getColumn(0).getBinary(0).toString());
      assertEquals("10499", result.getColumn(1).getBinary(0).toString());
      assertEquals(TSDataType.INT32.name(), result.getColumn(2).getBinary(0).toString());

      assertFalse(updateLastCacheOperator.hasNext());
      assertTrue(updateLastCacheOperator.isFinished());

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testUpdateLastCacheOperatorTestWithTimeFilter1() {
    try {
      List<Aggregator> aggregators = LastQueryUtil.createAggregators(TSDataType.INT32);
      Filter timeFilter = TimeFilter.gtEq(200);
      UpdateLastCacheOperator updateLastCacheOperator =
          initUpdateLastCacheOperator(aggregators, timeFilter, false, null);

      assertTrue(updateLastCacheOperator.isBlocked().isDone());
      assertTrue(updateLastCacheOperator.hasNext());
      TsBlock result = updateLastCacheOperator.next();
      assertEquals(1, result.getPositionCount());
      assertEquals(3, result.getValueColumnCount());

      assertEquals(499, result.getTimeByIndex(0));
      assertEquals(
          SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0",
          result.getColumn(0).getBinary(0).toString());
      assertEquals("10499", result.getColumn(1).getBinary(0).toString());
      assertEquals(TSDataType.INT32.name(), result.getColumn(2).getBinary(0).toString());

      assertFalse(updateLastCacheOperator.hasNext());
      assertTrue(updateLastCacheOperator.isFinished());

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testUpdateLastCacheOperatorTestWithTimeFilter2() {
    try {
      List<Aggregator> aggregators = LastQueryUtil.createAggregators(TSDataType.INT32);
      Filter timeFilter = TimeFilter.ltEq(120);
      UpdateLastCacheOperator updateLastCacheOperator =
          initUpdateLastCacheOperator(aggregators, timeFilter, false, null);

      assertTrue(updateLastCacheOperator.isBlocked().isDone());
      assertTrue(updateLastCacheOperator.hasNext());
      TsBlock result = updateLastCacheOperator.next();
      assertEquals(1, result.getPositionCount());
      assertEquals(3, result.getValueColumnCount());

      assertEquals(120, result.getTimeByIndex(0));
      assertEquals(
          SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0",
          result.getColumn(0).getBinary(0).toString());
      assertEquals("20120", result.getColumn(1).getBinary(0).toString());
      assertEquals(TSDataType.INT32.name(), result.getColumn(2).getBinary(0).toString());

      assertFalse(updateLastCacheOperator.hasNext());
      assertTrue(updateLastCacheOperator.isFinished());

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  public UpdateLastCacheOperator initUpdateLastCacheOperator(
      List<Aggregator> aggregators,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter)
      throws IllegalPathException {
    MeasurementPath measurementPath =
        new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
    Set<String> allSensors = Sets.newHashSet("sensor0");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    fragmentInstanceContext.addOperatorContext(
        1, planNodeId1, SeriesAggregationScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    fragmentInstanceContext.addOperatorContext(
        2, planNodeId2, UpdateLastCacheOperator.class.getSimpleName());

    fragmentInstanceContext
        .getOperatorContexts()
        .forEach(
            operatorContext -> {
              operatorContext.setMaxRunTime(TEST_TIME_SLICE);
            });

    SeriesAggregationScanOperator seriesAggregationScanOperator =
        new SeriesAggregationScanOperator(
            planNodeId1,
            measurementPath,
            allSensors,
            fragmentInstanceContext.getOperatorContexts().get(0),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, ascending, true),
            timeFilter,
            ascending,
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
    seriesAggregationScanOperator.initQueryDataSource(
        new QueryDataSource(seqResources, unSeqResources));

    return new UpdateLastCacheOperator(
        fragmentInstanceContext.getOperatorContexts().get(1),
        seriesAggregationScanOperator,
        measurementPath,
        measurementPath.getSeriesType(),
        null,
        false);
  }
}
