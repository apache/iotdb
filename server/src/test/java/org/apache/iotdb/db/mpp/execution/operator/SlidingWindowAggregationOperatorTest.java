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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.slidingwindow.SlidingWindowAggregatorFactory;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.SlidingWindowAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SlidingWindowAggregationOperatorTest {

  private static final String AGGREGATION_OPERATOR_TEST_SG =
      "root.SlidingWindowAggregationOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

  private final List<TAggregationType> leafAggregationTypes =
      Arrays.asList(
          TAggregationType.COUNT,
          TAggregationType.SUM,
          TAggregationType.LAST_VALUE,
          TAggregationType.FIRST_VALUE,
          TAggregationType.MAX_VALUE,
          TAggregationType.MIN_VALUE);

  private final List<TAggregationType> rootAggregationTypes =
      Arrays.asList(
          TAggregationType.COUNT,
          TAggregationType.AVG,
          TAggregationType.SUM,
          TAggregationType.LAST_VALUE,
          TAggregationType.MIN_TIME,
          TAggregationType.MAX_TIME,
          TAggregationType.FIRST_VALUE,
          TAggregationType.MAX_VALUE,
          TAggregationType.MIN_VALUE);

  private final List<List<List<InputLocation>>> inputLocations =
      Arrays.asList(
          Collections.singletonList(Collections.singletonList(new InputLocation(0, 0))),
          Collections.singletonList(
              Arrays.asList(new InputLocation(0, 0), new InputLocation(0, 1))),
          Collections.singletonList(Collections.singletonList(new InputLocation(0, 1))),
          Collections.singletonList(
              Arrays.asList(new InputLocation(0, 2), new InputLocation(0, 3))),
          Collections.singletonList(Collections.singletonList(new InputLocation(0, 5))),
          Collections.singletonList(Collections.singletonList(new InputLocation(0, 3))),
          Collections.singletonList(
              Arrays.asList(new InputLocation(0, 4), new InputLocation(0, 5))),
          Collections.singletonList(Collections.singletonList(new InputLocation(0, 6))),
          Collections.singletonList(Collections.singletonList(new InputLocation(0, 7))));

  private final GroupByTimeParameter groupByTimeParameter =
      new GroupByTimeParameter(0, 300, 100, 50, true);

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

  @Test
  public void slidingWindowAggregationTest() throws IllegalPathException {
    String[] retArray =
        new String[] {
          "0,100,20049.5,2004950.0,20099,0,99,20000,20099,20000",
          "50,100,20099.5,2009950.0,20149,50,149,20050,20149,20050",
          "100,100,20149.5,2014950.0,20199,100,199,20100,20199,20100",
          "150,100,15199.5,1519950.0,10249,150,249,20150,20199,10200",
          "200,100,6249.5,624950.0,299,200,299,10200,10259,260",
          "250,50,2274.5,113725.0,299,250,299,10250,10259,260",
        };

    SlidingWindowAggregationOperator slidingWindowAggregationOperator1 =
        initSlidingWindowAggregationOperator(true);
    int count = 0;
    while (slidingWindowAggregationOperator1.hasNext()) {
      TsBlock resultTsBlock = slidingWindowAggregationOperator1.next();
      if (resultTsBlock == null) {
        continue;
      }
      Assert.assertEquals(rootAggregationTypes.size(), resultTsBlock.getValueColumnCount());
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        Assert.assertEquals(retArray[count], getResultString(resultTsBlock, pos));
        count++;
      }
    }
    Assert.assertEquals(retArray.length, count);

    SlidingWindowAggregationOperator slidingWindowAggregationOperator2 =
        initSlidingWindowAggregationOperator(false);
    while (slidingWindowAggregationOperator2.hasNext()) {
      TsBlock resultTsBlock = slidingWindowAggregationOperator2.next();
      if (resultTsBlock == null) {
        continue;
      }
      Assert.assertEquals(rootAggregationTypes.size(), resultTsBlock.getValueColumnCount());
      int positionCount = resultTsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        Assert.assertEquals(retArray[count - 1], getResultString(resultTsBlock, pos));
        count--;
      }
    }
    Assert.assertEquals(0, count);
  }

  private String getResultString(TsBlock resultTsBlock, int pos) {
    return resultTsBlock.getTimeColumn().getLong(pos)
        + ","
        + resultTsBlock.getColumn(0).getLong(pos)
        + ","
        + resultTsBlock.getColumn(1).getDouble(pos)
        + ","
        + resultTsBlock.getColumn(2).getDouble(pos)
        + ","
        + resultTsBlock.getColumn(3).getInt(pos)
        + ","
        + resultTsBlock.getColumn(4).getLong(pos)
        + ","
        + resultTsBlock.getColumn(5).getLong(pos)
        + ","
        + resultTsBlock.getColumn(6).getInt(pos)
        + ","
        + resultTsBlock.getColumn(7).getInt(pos)
        + ","
        + resultTsBlock.getColumn(8).getInt(pos);
  }

  private SlidingWindowAggregationOperator initSlidingWindowAggregationOperator(boolean ascending)
      throws IllegalPathException {
    // Construct operator tree
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId sourceId = queryId.genPlanNodeId();
    driverContext.addOperatorContext(
        0, sourceId, SeriesAggregationScanOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        1, queryId.genPlanNodeId(), SlidingWindowAggregationOperator.class.getSimpleName());
    driverContext
        .getOperatorContexts()
        .forEach(
            operatorContext -> {
              operatorContext.setMaxRunTime(TEST_TIME_SLICE);
            });

    MeasurementPath d0s0 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);

    List<Aggregator> aggregators = new ArrayList<>();
    AccumulatorFactory.createAccumulators(
            leafAggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            ascending)
        .forEach(
            accumulator -> aggregators.add(new Aggregator(accumulator, AggregationStep.PARTIAL)));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(Collections.singleton("sensor0"));
    SeriesAggregationScanOperator seriesAggregationScanOperator =
        new SeriesAggregationScanOperator(
            sourceId,
            d0s0,
            ascending ? Ordering.ASC : Ordering.DESC,
            scanOptionsBuilder.build(),
            driverContext.getOperatorContexts().get(0),
            aggregators,
            initTimeRangeIterator(groupByTimeParameter, ascending, true),
            groupByTimeParameter,
            DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
    seriesAggregationScanOperator.initQueryDataSource(
        new QueryDataSource(seqResources, unSeqResources));

    List<Aggregator> finalAggregators = new ArrayList<>();
    for (int i = 0; i < rootAggregationTypes.size(); i++) {
      finalAggregators.add(
          SlidingWindowAggregatorFactory.createSlidingWindowAggregator(
              rootAggregationTypes.get(i),
              TSDataType.INT32,
              Collections.emptyList(),
              Collections.emptyMap(),
              ascending,
              inputLocations.get(i).stream()
                  .map(tmpInputLocations -> tmpInputLocations.toArray(new InputLocation[0]))
                  .collect(Collectors.toList()),
              AggregationStep.FINAL));
    }

    return new SlidingWindowAggregationOperator(
        driverContext.getOperatorContexts().get(1),
        finalAggregators,
        initTimeRangeIterator(groupByTimeParameter, ascending, false),
        seriesAggregationScanOperator,
        ascending,
        groupByTimeParameter,
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
  }
}
