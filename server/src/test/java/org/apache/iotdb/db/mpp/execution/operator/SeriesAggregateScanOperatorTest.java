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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregateScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;

public class SeriesAggregateScanOperatorTest {

  private static final String SERIES_SCAN_OPERATOR_TEST_SG = "root.SeriesScanOperatorTest";
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
  public void testAggregationWithoutTimeFilter() throws IllegalPathException {
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), null, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(500, resultTsBlock.getColumn(0).getLong(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationFuncWithoutTimeFilter1() throws IllegalPathException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.COUNT);
    aggregationTypes.add(AggregationType.SUM);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(aggregationTypes, null, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(500, resultTsBlock.getColumn(0).getLong(0));
      assertEquals(6524750.0, resultTsBlock.getColumn(1).getDouble(0), 0.0001);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationFuncWithoutTimeFilter2() throws IllegalPathException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.FIRST_VALUE);
    aggregationTypes.add(AggregationType.LAST_VALUE);
    aggregationTypes.add(AggregationType.MIN_TIME);
    aggregationTypes.add(AggregationType.MAX_TIME);
    aggregationTypes.add(AggregationType.MAX_VALUE);
    aggregationTypes.add(AggregationType.MIN_VALUE);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(aggregationTypes, null, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(20000, resultTsBlock.getColumn(0).getInt(0));
      assertEquals(10499, resultTsBlock.getColumn(1).getInt(0));
      assertEquals(0, resultTsBlock.getColumn(2).getLong(0));
      assertEquals(499, resultTsBlock.getColumn(3).getLong(0));
      assertEquals(20199, resultTsBlock.getColumn(4).getInt(0));
      assertEquals(260, resultTsBlock.getColumn(5).getInt(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithTimeFilter1() throws IllegalPathException {
    Filter timeFilter = TimeFilter.gtEq(120);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), timeFilter, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(resultTsBlock.getColumn(0).getLong(0), 380);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithTimeFilter2() throws IllegalPathException {
    Filter timeFilter = TimeFilter.ltEq(379);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), timeFilter, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(resultTsBlock.getColumn(0).getLong(0), 380);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithTimeFilter3() throws IllegalPathException {
    Filter timeFilter = new AndFilter(TimeFilter.gtEq(100), TimeFilter.ltEq(399));
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), timeFilter, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(resultTsBlock.getColumn(0).getLong(0), 300);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationWithTimeFilter() throws IllegalPathException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.FIRST_VALUE);
    aggregationTypes.add(AggregationType.LAST_VALUE);
    aggregationTypes.add(AggregationType.MIN_TIME);
    aggregationTypes.add(AggregationType.MAX_TIME);
    aggregationTypes.add(AggregationType.MAX_VALUE);
    aggregationTypes.add(AggregationType.MIN_VALUE);
    Filter timeFilter = new AndFilter(TimeFilter.gtEq(100), TimeFilter.ltEq(399));
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(aggregationTypes, timeFilter, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(20100, resultTsBlock.getColumn(0).getInt(0));
      assertEquals(399, resultTsBlock.getColumn(1).getInt(0));
      assertEquals(100, resultTsBlock.getColumn(2).getLong(0));
      assertEquals(399, resultTsBlock.getColumn(3).getLong(0));
      assertEquals(20199, resultTsBlock.getColumn(4).getInt(0));
      assertEquals(260, resultTsBlock.getColumn(5).getInt(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testGroupByWithoutGlobalTimeFilter() throws IllegalPathException {
    int[] result = new int[] {100, 100, 100, 100};
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(0));
      assertEquals(result[count], resultTsBlock.getColumn(0).getLong(0));
      count++;
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupByWithGlobalTimeFilter() throws IllegalPathException {
    int[] result = new int[] {0, 80, 100, 80};
    Filter timeFilter = new AndFilter(TimeFilter.gtEq(120), TimeFilter.ltEq(379));
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT),
            timeFilter,
            true,
            groupByTimeParameter);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(0));
      assertEquals(result[count], resultTsBlock.getColumn(0).getLong(0));
      count++;
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupByWithMultiFunction() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 399},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.FIRST_VALUE);
    aggregationTypes.add(AggregationType.LAST_VALUE);
    aggregationTypes.add(AggregationType.MAX_VALUE);
    aggregationTypes.add(AggregationType.MIN_VALUE);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(aggregationTypes, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(0));
      assertEquals(result[0][count], resultTsBlock.getColumn(0).getInt(0));
      assertEquals(result[1][count], resultTsBlock.getColumn(1).getInt(0));
      assertEquals(result[2][count], resultTsBlock.getColumn(2).getInt(0));
      assertEquals(result[3][count], resultTsBlock.getColumn(3).getInt(0));
      count++;
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupBySlidingTimeWindow() throws IllegalPathException {
    int[] result = new int[] {50, 50, 50, 50, 50, 50, 50, 50};
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 50, true);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(50 * count, resultTsBlock.getTimeColumn().getLong(0));
      assertEquals(result[count], resultTsBlock.getColumn(0).getLong(0));
      count++;
    }
    assertEquals(result.length, count);
  }

  @Test
  public void testGroupBySlidingTimeWindow2() throws IllegalPathException {
    int[] timeColumn = new int[] {0, 20, 30, 50, 60, 80, 90, 110, 120, 140};
    int[] result = new int[] {20, 10, 20, 10, 20, 10, 20, 10, 20, 9};
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 149, 50, 30, true);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(timeColumn[count], resultTsBlock.getTimeColumn().getLong(0));
      assertEquals(result[count], resultTsBlock.getColumn(0).getLong(0));
      count++;
    }
    assertEquals(timeColumn.length, count);
  }

  @Test
  public void testGroupBySlidingWindowWithMultiFunction() throws IllegalPathException {
    int[] timeColumn = new int[] {0, 20, 30, 50, 60, 80, 90, 110, 120, 140};
    int[][] result =
        new int[][] {
          {20000, 20020, 20030, 20050, 20060, 20080, 20090, 20110, 20120, 20140},
          {20019, 20029, 20049, 20059, 20079, 20089, 20109, 20119, 20139, 20148},
          {20019, 20029, 20049, 20059, 20079, 20089, 20109, 20119, 20139, 20148},
          {20000, 20020, 20030, 20050, 20060, 20080, 20090, 20110, 20120, 20140}
        };
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.FIRST_VALUE);
    aggregationTypes.add(AggregationType.LAST_VALUE);
    aggregationTypes.add(AggregationType.MAX_VALUE);
    aggregationTypes.add(AggregationType.MIN_VALUE);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 149, 50, 30, true);
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(aggregationTypes, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(timeColumn[count], resultTsBlock.getTimeColumn().getLong(0));
      assertEquals(result[0][count], resultTsBlock.getColumn(0).getInt(0));
      assertEquals(result[1][count], resultTsBlock.getColumn(1).getInt(0));
      assertEquals(result[2][count], resultTsBlock.getColumn(2).getInt(0));
      assertEquals(result[3][count], resultTsBlock.getColumn(3).getInt(0));
      count++;
    }
    assertEquals(timeColumn.length, count);
  }

  public SeriesAggregateScanOperator initSeriesAggregateScanOperator(
      List<AggregationType> aggregateFuncList,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter)
      throws IllegalPathException {
    MeasurementPath measurementPath =
        new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
    Set<String> allSensors = Sets.newHashSet("sensor0");
    QueryId queryId = new QueryId("stub_query");
    AtomicReference<FragmentInstanceState> state =
        new AtomicReference<>(FragmentInstanceState.RUNNING);
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    PlanNodeId planNodeId = new PlanNodeId("1");
    fragmentInstanceContext.addOperatorContext(
        1, planNodeId, SeriesScanOperator.class.getSimpleName());

    SeriesAggregateScanOperator seriesAggregateScanOperator =
        new SeriesAggregateScanOperator(
            planNodeId,
            measurementPath,
            allSensors,
            fragmentInstanceContext.getOperatorContexts().get(0),
            aggregateFuncList,
            timeFilter,
            ascending,
            groupByTimeParameter);
    seriesAggregateScanOperator.initQueryDataSource(
        new QueryDataSource(seqResources, unSeqResources));
    return seriesAggregateScanOperator;
  }
}
