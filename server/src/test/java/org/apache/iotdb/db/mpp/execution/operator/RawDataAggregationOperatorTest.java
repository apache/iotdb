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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.aggregation.Accumulator;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.RawDataAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

public class RawDataAggregationOperatorTest {

  private static final String AGGREGATION_OPERATOR_TEST_SG = "root.RawDataAggregationOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor;

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

  /**
   * Test aggregating raw data without group by interval.
   *
   * <p>Example SQL: select count(s0), sum(s0), min_time(s0), max_time(s0), min_value(s0),
   * max_value(s0), count(s1), sum(s1), min_time(s1), max_time(s1), min_value(s1), max_value(s1)
   * from root.sg.d0 where s1 > 10 and s2 < 15
   *
   * <p>For convenience, we don't use FilterOperator in test though rawDataAggregateOperator is
   * always used with value filter.
   */
  @Test
  public void aggregateRawDataTest1() throws IllegalPathException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.COUNT);
      aggregationTypes.add(AggregationType.SUM);
      aggregationTypes.add(AggregationType.MIN_TIME);
      aggregationTypes.add(AggregationType.MAX_TIME);
      aggregationTypes.add(AggregationType.MAX_VALUE);
      aggregationTypes.add(AggregationType.MIN_VALUE);
      for (int j = 0; j < 6; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations);
    int count = 0;
    while (rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int i = 0; i < 2; i++) {
        assertEquals(500, resultTsBlock.getColumn(6 * i).getLong(0));
        assertEquals(6524750.0, resultTsBlock.getColumn(6 * i + 1).getDouble(0), 0.0001);
        assertEquals(0, resultTsBlock.getColumn(6 * i + 2).getLong(0));
        assertEquals(499, resultTsBlock.getColumn(6 * i + 3).getLong(0));
        assertEquals(20199, resultTsBlock.getColumn(6 * i + 4).getInt(0));
        assertEquals(260, resultTsBlock.getColumn(6 * i + 5).getInt(0));
      }
      count++;
    }
    assertEquals(1, count);
  }

  /**
   * Test aggregating raw data without group by interval.
   *
   * <p>Example SQL: select avg(s0), avg(s1), first_value(s0), first_value(s1), last_value(s0),
   * last_value(s1) from root.sg.d0 where s1 > 10 and s2 < 15
   *
   * <p>For convenience, we don't use FilterOperator in test though rawDataAggregateOperator is
   * always used with value filter.
   */
  @Test
  public void aggregateRawDataTest2() throws IllegalPathException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.AVG);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.LAST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations);
    int count = 0;
    while (rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int i = 0; i < 2; i++) {
        assertEquals(13049.5, resultTsBlock.getColumn(i).getDouble(0), 0.001);
      }
      for (int i = 2; i < 4; i++) {
        assertEquals(20000, resultTsBlock.getColumn(i).getInt(0));
      }
      for (int i = 4; i < 6; i++) {
        assertEquals(10499, resultTsBlock.getColumn(i).getInt(0));
      }
      count++;
    }
    assertEquals(1, count);
  }

  /** Test aggregating raw data by time interval. */
  @Test
  public void groupByRawDataTest1() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {100, 100, 100, 99},
          {2004950, 2014950, 624950, 834551},
          {0, 100, 200, 300},
          {99, 199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    List<AggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.COUNT);
      aggregationTypes.add(AggregationType.SUM);
      aggregationTypes.add(AggregationType.MIN_TIME);
      aggregationTypes.add(AggregationType.MAX_TIME);
      aggregationTypes.add(AggregationType.MAX_VALUE);
      aggregationTypes.add(AggregationType.MIN_VALUE);
      for (int j = 0; j < 6; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, groupByTimeParameter, inputLocations);
    int count = 0;
    while (rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(0));
      for (int i = 0; i < 2; i++) {
        assertEquals(result[0][count], resultTsBlock.getColumn(6 * i).getLong(0));
        assertEquals(result[1][count], resultTsBlock.getColumn(6 * i + 1).getDouble(0), 0.0001);
        assertEquals(result[2][count], resultTsBlock.getColumn(6 * i + 2).getLong(0));
        assertEquals(result[3][count], resultTsBlock.getColumn(6 * i + 3).getLong(0));
        assertEquals(result[4][count], resultTsBlock.getColumn(6 * i + 4).getInt(0));
        assertEquals(result[5][count], resultTsBlock.getColumn(6 * i + 5).getInt(0));
      }
      count++;
    }
    assertEquals(4, count);
  }

  /** Test aggregating raw data by time interval. */
  @Test
  public void groupByRawDataTest2() throws IllegalPathException {
    double[][] result =
        new double[][] {
          {20049.5, 20149.5, 6249.5, 8429.808},
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398},
        };
    List<AggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.AVG);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(AggregationType.LAST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);
    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, groupByTimeParameter, inputLocations);
    int count = 0;
    while (rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(0));
      for (int i = 0; i < 2; i++) {
        assertEquals(result[0][count], resultTsBlock.getColumn(i).getDouble(0), 0.001);
      }
      for (int i = 2; i < 4; i++) {
        assertEquals((int) result[1][count], resultTsBlock.getColumn(i).getInt(0));
      }
      for (int i = 4; i < 6; i++) {
        assertEquals((int) result[2][count], resultTsBlock.getColumn(i).getInt(0));
      }
      count++;
    }
    assertEquals(4, count);
  }

  private RawDataAggregationOperator initRawDataAggregationOperator(
      List<AggregationType> aggregationTypes,
      GroupByTimeParameter groupByTimeParameter,
      List<List<InputLocation[]>> inputLocations)
      throws IllegalPathException {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

    MeasurementPath measurementPath1 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
    Set<String> allSensors = new HashSet<>();
    allSensors.add("sensor0");
    allSensors.add("sensor1");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    fragmentInstanceContext.addOperatorContext(
        1, planNodeId1, SeriesScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    fragmentInstanceContext.addOperatorContext(
        2, planNodeId2, SeriesScanOperator.class.getSimpleName());
    fragmentInstanceContext.addOperatorContext(
        3, new PlanNodeId("3"), TimeJoinOperator.class.getSimpleName());
    fragmentInstanceContext.addOperatorContext(
        4, new PlanNodeId("4"), RawDataAggregationOperatorTest.class.getSimpleName());
    fragmentInstanceContext
        .getOperatorContexts()
        .forEach(
            operatorContext -> {
              operatorContext.setMaxRunTime(TEST_TIME_SLICE);
            });

    SeriesScanOperator seriesScanOperator1 =
        new SeriesScanOperator(
            planNodeId1,
            measurementPath1,
            allSensors,
            TSDataType.INT32,
            fragmentInstanceContext.getOperatorContexts().get(0),
            null,
            null,
            true);
    seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

    MeasurementPath measurementPath2 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
    SeriesScanOperator seriesScanOperator2 =
        new SeriesScanOperator(
            planNodeId2,
            measurementPath2,
            allSensors,
            TSDataType.INT32,
            fragmentInstanceContext.getOperatorContexts().get(1),
            null,
            null,
            true);
    seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

    TimeJoinOperator timeJoinOperator =
        new TimeJoinOperator(
            fragmentInstanceContext.getOperatorContexts().get(2),
            Arrays.asList(seriesScanOperator1, seriesScanOperator2),
            Ordering.ASC,
            Arrays.asList(TSDataType.INT32, TSDataType.INT32),
            Arrays.asList(
                new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
            new AscTimeComparator());

    List<Aggregator> aggregators = new ArrayList<>();
    List<Accumulator> accumulators =
        AccumulatorFactory.createAccumulators(aggregationTypes, TSDataType.INT32, true);
    for (int i = 0; i < accumulators.size(); i++) {
      aggregators.add(
          new Aggregator(accumulators.get(i), AggregationStep.SINGLE, inputLocations.get(i)));
    }
    return new RawDataAggregationOperator(
        fragmentInstanceContext.getOperatorContexts().get(3),
        aggregators,
        initTimeRangeIterator(groupByTimeParameter, true, true),
        timeJoinOperator,
        true,
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
  }
}
