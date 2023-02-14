/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignedSeriesAggregationScanOperatorTest {

  private static final String SERIES_AGGREGATION_SCAN_OPERATOR_TEST_SG =
      "root.AlignedSeriesAggregationScanOperatorTest";
  private static final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private static final List<TsFileResource> seqResources = new ArrayList<>();
  private static final List<TsFileResource> unSeqResources = new ArrayList<>();

  private ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");;
  private static final double DELTA = 0.000001;

  @BeforeClass
  public static void setUp() throws MetadataException, IOException, WriteProcessException {
    AlignedSeriesTestUtil.setUp(
        measurementSchemas, seqResources, unSeqResources, SERIES_AGGREGATION_SCAN_OPERATOR_TEST_SG);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    AlignedSeriesTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testAggregationWithoutTimeFilter() throws IllegalPathException {
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      for (int i = 0; i < measurementSchemas.size(); i++) {
        assertEquals(500, resultTsBlock.getColumn(i).getLong(0));
      }
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithoutTimeFilterOrderByTimeDesc() throws IllegalPathException {
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  false),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, false, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      for (int i = 0; i < measurementSchemas.size(); i++) {
        assertEquals(500, resultTsBlock.getColumn(i).getLong(0));
      }
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationFuncWithoutTimeFilter1() throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.COUNT);
    aggregationTypes.add(TAggregationType.SUM);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  aggregationTypes.get(i),
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      assertEquals(500, resultTsBlock.getColumn(0).getLong(0));
      assertEquals(6524750.0, resultTsBlock.getColumn(1).getDouble(0), 0.0001);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationFuncWithoutTimeFilter2() throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);
    aggregationTypes.add(TAggregationType.MIN_TIME);
    aggregationTypes.add(TAggregationType.MAX_TIME);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  aggregationTypes.get(i),
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      assertTrue(resultTsBlock.getColumn(0).getBoolean(0));
      assertEquals(10499, resultTsBlock.getColumn(1).getInt(0));
      assertEquals(20199, resultTsBlock.getColumn(2).getLong(0));
      assertEquals(260.0, resultTsBlock.getColumn(3).getFloat(0), DELTA);
      assertEquals(0, resultTsBlock.getColumn(4).getLong(0));
      assertEquals(499, resultTsBlock.getColumn(5).getLong(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationFuncWithoutTimeFilterOrderByTimeDesc()
      throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);
    aggregationTypes.add(TAggregationType.MIN_TIME);
    aggregationTypes.add(TAggregationType.MAX_TIME);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  aggregationTypes.get(i),
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  false),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, false, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      assertTrue(resultTsBlock.getColumn(0).getBoolean(0));
      assertEquals(10499, resultTsBlock.getColumn(1).getInt(0));
      assertEquals(20199, resultTsBlock.getColumn(2).getLong(0));
      assertEquals(260.0, resultTsBlock.getColumn(3).getFloat(0), DELTA);
      assertEquals(0, resultTsBlock.getColumn(4).getLong(0));
      assertEquals(499, resultTsBlock.getColumn(5).getLong(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithTimeFilter1() throws IllegalPathException {
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    Filter timeFilter = TimeFilter.gtEq(120);
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, timeFilter, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      for (int i = 0; i < measurementSchemas.size(); i++) {
        assertEquals(resultTsBlock.getColumn(i).getLong(0), 380);
      }
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithTimeFilter2() throws IllegalPathException {
    Filter timeFilter = TimeFilter.ltEq(379);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, timeFilter, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      for (int i = 0; i < measurementSchemas.size(); i++) {
        assertEquals(resultTsBlock.getColumn(i).getLong(0), 380);
      }
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationWithTimeFilter3() throws IllegalPathException {
    Filter timeFilter = new AndFilter(TimeFilter.gtEq(100), TimeFilter.ltEq(399));
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, timeFilter, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      for (int i = 0; i < measurementSchemas.size(); i++) {
        assertEquals(resultTsBlock.getColumn(i).getLong(0), 300);
      }
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMultiAggregationWithTimeFilter() throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);
    aggregationTypes.add(TAggregationType.MIN_TIME);
    aggregationTypes.add(TAggregationType.MAX_TIME);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  aggregationTypes.get(i),
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    Filter timeFilter = new AndFilter(TimeFilter.gtEq(100), TimeFilter.ltEq(399));
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, timeFilter, true, null);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      assertTrue(resultTsBlock.getColumn(0).getBoolean(0));
      assertEquals(399, resultTsBlock.getColumn(1).getInt(0));
      assertEquals(20199, resultTsBlock.getColumn(2).getLong(0));
      assertEquals(260.0, resultTsBlock.getColumn(3).getFloat(0), DELTA);
      assertEquals(100, resultTsBlock.getColumn(4).getLong(0));
      assertEquals(399, resultTsBlock.getColumn(5).getLong(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testGroupByWithoutGlobalTimeFilter() throws IllegalPathException {
    int[] result = new int[] {100, 100, 100, 99};
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(pos));
        for (int i = 0; i < measurementSchemas.size(); i++) {
          assertEquals(result[count], resultTsBlock.getColumn(i).getLong(pos));
        }
        count++;
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupByWithGlobalTimeFilter() throws IllegalPathException {
    int[] result = new int[] {0, 80, 100, 80};
    Filter timeFilter = new AndFilter(TimeFilter.gtEq(120), TimeFilter.ltEq(379));
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    List<Aggregator> aggregators = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      TSDataType dataType = measurementSchemas.get(i).getType();
      List<InputLocation[]> inputLocations = new ArrayList<>();
      inputLocations.add(new InputLocation[] {new InputLocation(0, i)});
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.COUNT,
                  dataType,
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              AggregationStep.SINGLE,
              inputLocations));
    }
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(
            aggregators, timeFilter, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(pos));
        for (int i = 0; i < measurementSchemas.size(); i++) {
          assertEquals(result[count], resultTsBlock.getColumn(i).getLong(pos));
        }
        count++;
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupByWithMultiFunction() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    List<Aggregator> aggregators = new ArrayList<>();
    List<InputLocation[]> inputLocations =
        Collections.singletonList(new InputLocation[] {new InputLocation(0, 1)});
    AccumulatorFactory.createAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true)
        .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.SINGLE, inputLocations)));
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[0][count], resultTsBlock.getColumn(0).getInt(pos));
        assertEquals(result[1][count], resultTsBlock.getColumn(1).getInt(pos));
        assertEquals(result[2][count], resultTsBlock.getColumn(2).getInt(pos));
        assertEquals(result[3][count], resultTsBlock.getColumn(3).getInt(pos));
        count++;
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupByWithMultiFunctionOrderByTimeDesc() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    List<Aggregator> aggregators = new ArrayList<>();
    List<InputLocation[]> inputLocations =
        Collections.singletonList(new InputLocation[] {new InputLocation(0, 1)});
    AccumulatorFactory.createAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            false)
        .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.SINGLE, inputLocations)));
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, false, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(100 * (3 - count), resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[0][3 - count], resultTsBlock.getColumn(0).getInt(pos));
        assertEquals(result[1][3 - count], resultTsBlock.getColumn(1).getInt(pos));
        assertEquals(result[2][3 - count], resultTsBlock.getColumn(2).getInt(pos));
        assertEquals(result[3][3 - count], resultTsBlock.getColumn(3).getInt(pos));
        count++;
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupBySlidingTimeWindow() throws IllegalPathException {
    int[] result = new int[] {50, 50, 50, 50, 50, 50, 50, 49};
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 50, true);
    List<TAggregationType> aggregationTypes = Collections.singletonList(TAggregationType.COUNT);
    List<Aggregator> aggregators = new ArrayList<>();
    List<InputLocation[]> inputLocations =
        Collections.singletonList(new InputLocation[] {new InputLocation(0, 1)});
    AccumulatorFactory.createAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true)
        .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.SINGLE, inputLocations)));
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(50 * count, resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[count], resultTsBlock.getColumn(0).getLong(pos));
        count++;
      }
    }
    assertEquals(result.length, count);
  }

  @Test
  public void testGroupBySlidingTimeWindow2() throws IllegalPathException {
    int[] timeColumn = new int[] {0, 20, 30, 50, 60, 80, 90, 110, 120, 140};
    int[] result = new int[] {20, 10, 20, 10, 20, 10, 20, 10, 20, 9};
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 149, 50, 30, true);
    List<TAggregationType> aggregationTypes = Collections.singletonList(TAggregationType.COUNT);
    List<Aggregator> aggregators = new ArrayList<>();
    List<InputLocation[]> inputLocations =
        Collections.singletonList(new InputLocation[] {new InputLocation(0, 1)});
    AccumulatorFactory.createAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true)
        .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.SINGLE, inputLocations)));
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(timeColumn[count], resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[count], resultTsBlock.getColumn(0).getLong(pos));
        count++;
      }
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
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 149, 50, 30, true);
    List<Aggregator> aggregators = new ArrayList<>();
    List<InputLocation[]> inputLocations =
        Collections.singletonList(new InputLocation[] {new InputLocation(0, 1)});
    AccumulatorFactory.createAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true)
        .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.SINGLE, inputLocations)));
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        initAlignedSeriesAggregationScanOperator(aggregators, null, true, groupByTimeParameter);
    int count = 0;
    while (seriesAggregationScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregationScanOperator.next();
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(timeColumn[count], resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[0][count], resultTsBlock.getColumn(0).getInt(pos));
        assertEquals(result[1][count], resultTsBlock.getColumn(1).getInt(pos));
        assertEquals(result[2][count], resultTsBlock.getColumn(2).getInt(pos));
        assertEquals(result[3][count], resultTsBlock.getColumn(3).getInt(pos));
        count++;
      }
    }
    assertEquals(timeColumn.length, count);
  }

  public AlignedSeriesAggregationScanOperator initAlignedSeriesAggregationScanOperator(
      List<Aggregator> aggregators,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter)
      throws IllegalPathException {
    AlignedPath alignedPath =
        new AlignedPath(
            SERIES_AGGREGATION_SCAN_OPERATOR_TEST_SG + ".device0",
            measurementSchemas.stream()
                .map(MeasurementSchema::getMeasurementId)
                .collect(Collectors.toList()),
            measurementSchemas.stream()
                .map(m -> (IMeasurementSchema) m)
                .collect(Collectors.toList()));

    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId, SeriesScanOperator.class.getSimpleName());
    driverContext
        .getOperatorContexts()
        .forEach(operatorContext -> operatorContext.setMaxRunTime(TEST_TIME_SLICE));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(new HashSet<>(alignedPath.getMeasurementList()));
    scanOptionsBuilder.withGlobalTimeFilter(timeFilter);

    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        new AlignedSeriesAggregationScanOperator(
            planNodeId,
            alignedPath,
            ascending ? Ordering.ASC : Ordering.DESC,
            scanOptionsBuilder.build(),
            driverContext.getOperatorContexts().get(0),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, ascending, true),
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
    seriesAggregationScanOperator.initQueryDataSource(
        new QueryDataSource(seqResources, unSeqResources));
    return seriesAggregationScanOperator;
  }
}
