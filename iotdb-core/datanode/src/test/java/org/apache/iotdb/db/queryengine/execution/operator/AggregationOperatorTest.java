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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.aggregation.Accumulator;
import org.apache.iotdb.db.queryengine.execution.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.AggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.TimeDuration;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.junit.Assert.assertEquals;

public class AggregationOperatorTest {

  public static Duration TEST_TIME_SLICE = new Duration(50000, TimeUnit.MILLISECONDS);

  private static final String AGGREGATION_OPERATOR_TEST_SG = "root.AggregationOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

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
  public void testAggregateIntermediateResult1() throws Exception {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.COUNT);
    aggregationTypes.add(TAggregationType.SUM);
    aggregationTypes.add(TAggregationType.MIN_TIME);
    aggregationTypes.add(TAggregationType.MAX_TIME);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);

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
  public void testAggregateIntermediateResult2() throws Exception {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.AVG);
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);

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
  public void testGroupByIntermediateResult1() throws Exception {
    int[][] result =
        new int[][] {
          {100, 100, 100, 99},
          {2004950, 2014950, 624950, 834551},
          {0, 100, 200, 300},
          {99, 199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 399, new TimeDuration(0, 100), new TimeDuration(0, 100), true);

    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.COUNT);
    aggregationTypes.add(TAggregationType.SUM);
    aggregationTypes.add(TAggregationType.MIN_TIME);
    aggregationTypes.add(TAggregationType.MAX_TIME);
    aggregationTypes.add(TAggregationType.MAX_VALUE);
    aggregationTypes.add(TAggregationType.MIN_VALUE);

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
  public void testGroupByIntermediateResult2() throws Exception {
    double[][] result =
        new double[][] {
          {20049.5, 20149.5, 6249.5, 8429.808},
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398},
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    aggregationTypes.add(TAggregationType.AVG);
    aggregationTypes.add(TAggregationType.FIRST_VALUE);
    aggregationTypes.add(TAggregationType.LAST_VALUE);

    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 399, new TimeDuration(0, 100), new TimeDuration(0, 100), true);

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
      List<TAggregationType> aggregationTypes,
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
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(
        1, planNodeId1, SeriesAggregationScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(
        2, planNodeId2, SeriesAggregationScanOperator.class.getSimpleName());
    PlanNodeId planNodeId3 = new PlanNodeId("3");
    driverContext.addOperatorContext(3, planNodeId3, AggregationOperator.class.getSimpleName());
    driverContext
        .getOperatorContexts()
        .forEach(operatorContext -> OperatorContext.setMaxRunTime(TEST_TIME_SLICE));

    NonAlignedFullPath measurementPath1 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(AGGREGATION_OPERATOR_TEST_SG + ".device0"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    List<TreeAggregator> aggregators = new ArrayList<>();
    AccumulatorFactory.createBuiltinAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true)
        .forEach(o -> aggregators.add(new TreeAggregator(o, AggregationStep.PARTIAL)));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(Collections.singleton("sensor0"));
    SeriesAggregationScanOperator seriesAggregationScanOperator1 =
        new SeriesAggregationScanOperator(
            planNodeId1,
            measurementPath1,
            Ordering.ASC,
            scanOptionsBuilder.build(),
            driverContext.getOperatorContexts().get(0),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, true, true),
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
            true);

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
            Ordering.ASC,
            scanOptionsBuilder.build(),
            driverContext.getOperatorContexts().get(0),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, true, true),
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
            true);

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

    List<TreeAggregator> finalAggregators = new ArrayList<>();
    List<Accumulator> accumulators =
        AccumulatorFactory.createBuiltinAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    for (int i = 0; i < accumulators.size(); i++) {
      finalAggregators.add(
          new TreeAggregator(accumulators.get(i), AggregationStep.FINAL, inputLocations.get(i)));
    }

    return new AggregationOperator(
        driverContext.getOperatorContexts().get(2),
        finalAggregators,
        initTimeRangeIterator(groupByTimeParameter, true, true),
        children,
        false,
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
  }
}
