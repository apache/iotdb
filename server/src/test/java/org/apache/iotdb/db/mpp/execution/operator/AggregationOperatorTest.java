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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.Accumulator;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.AggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

public class AggregationOperatorTest {

  public static Duration TEST_TIME_SLICE = new Duration(50000, TimeUnit.MILLISECONDS);

  private static final String AGGREGATION_OPERATOR_TEST_SG = "root.AggregationOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, AGGREGATION_OPERATOR_TEST_SG);
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
    instanceNotificationExecutor.shutdown();
  }

  /** Try to aggregate unary intermediate result of one time series without group by interval. */
  @Test
  public void testAggregateIntermediateResult1()
      throws IllegalPathException, ExecutionException, InterruptedException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.COUNT);
    aggregationTypes.add(AggregationType.SUM);
    aggregationTypes.add(AggregationType.MIN_TIME);
    aggregationTypes.add(AggregationType.MAX_TIME);
    aggregationTypes.add(AggregationType.MAX_VALUE);
    aggregationTypes.add(AggregationType.MIN_VALUE);
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < aggregationTypes.size(); i++) {
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(1, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    AggregationOperator aggregationOperator =
        initAggregationOperator(aggregationTypes, null, inputLocations);
    int count = 0;
    while (true) {
      ListenableFuture<?> blocked = aggregationOperator.isBlocked();
      blocked.get();
      if (!aggregationOperator.hasNext()) {
        break;
      }
      TsBlock resultTsBlock = aggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      assertEquals(500, resultTsBlock.getColumn(0).getLong(0));
      assertEquals(6524750.0, resultTsBlock.getColumn(1).getDouble(0), 0.0001);
      assertEquals(0, resultTsBlock.getColumn(2).getLong(0));
      assertEquals(499, resultTsBlock.getColumn(3).getLong(0));
      assertEquals(20199, resultTsBlock.getColumn(4).getInt(0));
      assertEquals(260, resultTsBlock.getColumn(5).getInt(0));
      count++;
    }
    assertEquals(1, count);
  }

  /** Try to aggregate binary intermediate result of one time series without group by interval. */
  @Test
  public void testAggregateIntermediateResult2()
      throws IllegalPathException, ExecutionException, InterruptedException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.AVG);
    aggregationTypes.add(AggregationType.FIRST_VALUE);
    aggregationTypes.add(AggregationType.LAST_VALUE);
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < aggregationTypes.size(); i++) {
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(
          new InputLocation[] {new InputLocation(0, 2 * i), new InputLocation(0, 2 * i + 1)});
      inputLocationForOneAggregator.add(
          new InputLocation[] {new InputLocation(1, 2 * i), new InputLocation(1, 2 * i + 1)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    AggregationOperator aggregationOperator =
        initAggregationOperator(aggregationTypes, null, inputLocations);
    int count = 0;
    while (true) {
      ListenableFuture<?> blocked = aggregationOperator.isBlocked();
      blocked.get();
      if (!aggregationOperator.hasNext()) {
        break;
      }
      TsBlock resultTsBlock = aggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      assertEquals(13049.5, resultTsBlock.getColumn(0).getDouble(0), 0.001);
      assertEquals(20000, resultTsBlock.getColumn(1).getInt(0));
      assertEquals(10499, resultTsBlock.getColumn(2).getInt(0));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testGroupByIntermediateResult1()
      throws IllegalPathException, ExecutionException, InterruptedException {
    int[][] result =
        new int[][] {
          {100, 100, 100, 99},
          {2004950, 2014950, 624950, 834551},
          {0, 100, 200, 300},
          {99, 199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.COUNT);
    aggregationTypes.add(AggregationType.SUM);
    aggregationTypes.add(AggregationType.MIN_TIME);
    aggregationTypes.add(AggregationType.MAX_TIME);
    aggregationTypes.add(AggregationType.MAX_VALUE);
    aggregationTypes.add(AggregationType.MIN_VALUE);
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < aggregationTypes.size(); i++) {
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(1, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    AggregationOperator aggregationOperator =
        initAggregationOperator(aggregationTypes, groupByTimeParameter, inputLocations);
    int count = 0;
    while (true) {
      ListenableFuture<?> blocked = aggregationOperator.isBlocked();
      blocked.get();
      if (!aggregationOperator.hasNext()) {
        break;
      }
      TsBlock resultTsBlock = aggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[0][count], resultTsBlock.getColumn(0).getLong(pos));
        assertEquals(result[1][count], resultTsBlock.getColumn(1).getDouble(pos), 0.0001);
        assertEquals(result[2][count], resultTsBlock.getColumn(2).getLong(pos));
        assertEquals(result[3][count], resultTsBlock.getColumn(3).getLong(pos));
        assertEquals(result[4][count], resultTsBlock.getColumn(4).getInt(pos));
        assertEquals(result[5][count], resultTsBlock.getColumn(5).getInt(pos));
        count++;
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testGroupByIntermediateResult2()
      throws IllegalPathException, ExecutionException, InterruptedException {
    double[][] result =
        new double[][] {
          {20049.5, 20149.5, 6249.5, 8429.808},
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398},
        };
    List<AggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(AggregationType.AVG);
    aggregationTypes.add(AggregationType.FIRST_VALUE);
    aggregationTypes.add(AggregationType.LAST_VALUE);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < aggregationTypes.size(); i++) {
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(
          new InputLocation[] {new InputLocation(0, 2 * i), new InputLocation(0, 2 * i + 1)});
      inputLocationForOneAggregator.add(
          new InputLocation[] {new InputLocation(1, 2 * i), new InputLocation(1, 2 * i + 1)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    AggregationOperator aggregationOperator =
        initAggregationOperator(aggregationTypes, groupByTimeParameter, inputLocations);
    int count = 0;
    while (true) {
      ListenableFuture<?> blocked = aggregationOperator.isBlocked();
      blocked.get();
      if (!aggregationOperator.hasNext()) {
        break;
      }
      TsBlock resultTsBlock = aggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(pos));
        assertEquals(result[0][count], resultTsBlock.getColumn(0).getDouble(pos), 0.001);
        assertEquals((int) result[1][count], resultTsBlock.getColumn(1).getInt(pos));
        assertEquals((int) result[2][count], resultTsBlock.getColumn(2).getInt(pos));
        count++;
      }
    }
    assertEquals(4, count);
  }

  /**
   * @param aggregationTypes Aggregation function used in test
   * @param groupByTimeParameter group by time parameter
   * @param inputLocations each inputLocation is used in one aggregator
   */
  private AggregationOperator initAggregationOperator(
      List<AggregationType> aggregationTypes,
      GroupByTimeParameter groupByTimeParameter,
      List<List<InputLocation[]>> inputLocations)
      throws IllegalPathException {
    // Construct operator tree
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
        2, planNodeId2, SeriesAggregationScanOperator.class.getSimpleName());
    PlanNodeId planNodeId3 = new PlanNodeId("3");
    fragmentInstanceContext.addOperatorContext(
        3, planNodeId3, AggregationOperator.class.getSimpleName());
    fragmentInstanceContext
        .getOperatorContexts()
        .forEach(
            operatorContext -> {
              operatorContext.setMaxRunTime(TEST_TIME_SLICE);
            });

    MeasurementPath measurementPath1 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
    List<Aggregator> aggregators = new ArrayList<>();
    AccumulatorFactory.createAccumulators(aggregationTypes, TSDataType.INT32, true)
        .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.PARTIAL)));
    SeriesAggregationScanOperator seriesAggregationScanOperator1 =
        new SeriesAggregationScanOperator(
            planNodeId1,
            measurementPath1,
            Collections.singleton("sensor0"),
            fragmentInstanceContext.getOperatorContexts().get(0),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, true, true),
            null,
            true,
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
    List<TsFileResource> seqResources1 = new ArrayList<>();
    List<TsFileResource> unSeqResources1 = new ArrayList<>();
    seqResources1.add(seqResources.get(0));
    seqResources1.add(seqResources.get(1));
    seqResources1.add(seqResources.get(3));
    unSeqResources1.add(unSeqResources.get(0));
    unSeqResources1.add(unSeqResources.get(1));
    unSeqResources1.add(unSeqResources.get(3));
    unSeqResources1.add(unSeqResources.get(5));
    seriesAggregationScanOperator1.initQueryDataSource(
        new QueryDataSource(seqResources1, unSeqResources1));

    SeriesAggregationScanOperator seriesAggregationScanOperator2 =
        new SeriesAggregationScanOperator(
            planNodeId2,
            measurementPath1,
            Collections.singleton("sensor0"),
            fragmentInstanceContext.getOperatorContexts().get(1),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, true, true),
            null,
            true,
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
    List<TsFileResource> seqResources2 = new ArrayList<>();
    List<TsFileResource> unSeqResources2 = new ArrayList<>();
    seqResources2.add(seqResources.get(2));
    seqResources2.add(seqResources.get(4));
    unSeqResources2.add(unSeqResources.get(2));
    unSeqResources2.add(unSeqResources.get(4));
    seriesAggregationScanOperator2.initQueryDataSource(
        new QueryDataSource(seqResources2, unSeqResources2));

    List<Operator> children = new ArrayList<>();
    children.add(seriesAggregationScanOperator1);
    children.add(seriesAggregationScanOperator2);

    List<Aggregator> finalAggregators = new ArrayList<>();
    List<Accumulator> accumulators =
        AccumulatorFactory.createAccumulators(aggregationTypes, TSDataType.INT32, true);
    for (int i = 0; i < accumulators.size(); i++) {
      finalAggregators.add(
          new Aggregator(accumulators.get(i), AggregationStep.FINAL, inputLocations.get(i)));
    }

    return new AggregationOperator(
        fragmentInstanceContext.getOperatorContexts().get(2),
        finalAggregators,
        initTimeRangeIterator(groupByTimeParameter, true, true),
        children,
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
  }
}
