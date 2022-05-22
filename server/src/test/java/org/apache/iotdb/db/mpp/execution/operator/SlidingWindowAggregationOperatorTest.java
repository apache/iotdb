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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.slidingwindow.SlidingWindowAggregator;
import org.apache.iotdb.db.mpp.aggregation.slidingwindow.SlidingWindowAggregatorFactory;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.SlidingWindowAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.utils.SchemaUtils;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class SlidingWindowAggregationOperatorTest {

  private static final String AGGREGATION_OPERATOR_TEST_SG =
      "root.SlidingWindowAggregationOperatorTest";
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

  @Test
  public void slidingWindowAggregationOperatorTest1() throws IllegalPathException {
    List<AggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();

    aggregationTypes.add(AggregationType.AVG);
    List<InputLocation[]> inputLocationForOneAggregator1 = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      inputLocationForOneAggregator1.add(new InputLocation[] {new InputLocation(0, i)});
    }
    inputLocations.add(inputLocationForOneAggregator1);

    aggregationTypes.add(AggregationType.FIRST_VALUE);
    List<InputLocation[]> inputLocationForOneAggregator2 = new ArrayList<>();
    for (int i = 2; i < 4; i++) {
      inputLocationForOneAggregator2.add(new InputLocation[] {new InputLocation(0, i)});
    }
    inputLocations.add(inputLocationForOneAggregator2);

    aggregationTypes.add(AggregationType.LAST_VALUE);
    List<InputLocation[]> inputLocationForOneAggregator3 = new ArrayList<>();
    for (int i = 4; i < 6; i++) {
      inputLocationForOneAggregator3.add(new InputLocation[] {new InputLocation(0, i)});
    }
    inputLocations.add(inputLocationForOneAggregator3);
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 300, 100, 50, true);

    SlidingWindowAggregationOperator slidingWindowAggregationOperator =
        initSlidingWindowAggregationOperator(
            aggregationTypes, groupByTimeParameter, inputLocations, false, true);
    int count = 0;
    while (slidingWindowAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = slidingWindowAggregationOperator.next();
      long time = resultTsBlock.getTimeColumn().getLong(0);
      Assert.assertEquals(3, resultTsBlock.getValueColumnCount());
      System.out.println("Time: " + time);
      System.out.println("\tavg: " + resultTsBlock.getColumn(0).getDouble(0));
      System.out.println("\tfirst_value: " + resultTsBlock.getColumn(1).getInt(0));
      System.out.println("\tlast_value: " + resultTsBlock.getColumn(2).getInt(0));
      count++;
    }
    Assert.assertEquals(6, count);
  }

  /**
   * @param aggregationTypes Aggregation function used in test
   * @param groupByTimeParameter group by time parameter
   * @param inputLocations each inputLocation is used in one aggregator
   */
  private SlidingWindowAggregationOperator initSlidingWindowAggregationOperator(
      List<AggregationType> aggregationTypes,
      GroupByTimeParameter groupByTimeParameter,
      List<List<InputLocation[]>> inputLocations,
      boolean isPartial,
      boolean ascending)
      throws IllegalPathException {
    // Construct operator tree
    QueryId queryId = new QueryId("test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    PlanNodeId sourceId = queryId.genPlanNodeId();
    fragmentInstanceContext.addOperatorContext(
        0, sourceId, SeriesAggregationScanOperator.class.getSimpleName());
    fragmentInstanceContext.addOperatorContext(
        1, queryId.genPlanNodeId(), SlidingWindowAggregationOperator.class.getSimpleName());

    MeasurementPath d0s0 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);

    List<Aggregator> aggregators = new ArrayList<>();
    AccumulatorFactory.createAccumulators(aggregationTypes, TSDataType.INT32, ascending)
        .forEach(
            accumulator -> aggregators.add(new Aggregator(accumulator, AggregationStep.PARTIAL)));

    SeriesAggregationScanOperator seriesAggregationScanOperator =
        new SeriesAggregationScanOperator(
            sourceId,
            d0s0,
            Collections.singleton("sensor0"),
            fragmentInstanceContext.getOperatorContexts().get(0),
            aggregators,
            null,
            ascending,
            groupByTimeParameter);
    seriesAggregationScanOperator.initQueryDataSource(
        new QueryDataSource(seqResources, unSeqResources));

    List<SlidingWindowAggregator> finalAggregators = new ArrayList<>();
    for (int i = 0; i < aggregationTypes.size(); i++) {
      finalAggregators.add(
          SlidingWindowAggregatorFactory.createSlidingWindowAggregator(
              aggregationTypes.get(i),
              SchemaUtils.getSeriesTypeByPath(d0s0, aggregationTypes.get(i).name()),
              ascending,
              inputLocations.get(i),
              isPartial ? AggregationStep.INTERMEDIATE : AggregationStep.FINAL));
    }

    return new SlidingWindowAggregationOperator(
        fragmentInstanceContext.getOperatorContexts().get(1),
        finalAggregators,
        seriesAggregationScanOperator,
        ascending,
        groupByTimeParameter);
  }
}
