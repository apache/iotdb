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
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.join.HorizontallyConcatOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import io.airlift.units.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HorizontallyConcatOperatorTest {
  private static final String HORIZONTALLY_CONCAT_OPERATOR_TEST_SG =
      "root.HorizontallyConcatOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas,
        deviceIds,
        seqResources,
        unSeqResources,
        HORIZONTALLY_CONCAT_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest1() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);

      PlanNodeId planNodeId1 = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId1, SeriesAggregationScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(
          2, planNodeId2, SeriesAggregationScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), HorizontallyConcatOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(
              HORIZONTALLY_CONCAT_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      List<TAggregationType> aggregationTypes =
          Arrays.asList(TAggregationType.COUNT, TAggregationType.SUM, TAggregationType.FIRST_VALUE);
      GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 10, 1, 1, true);
      List<Aggregator> aggregators = new ArrayList<>();
      AccumulatorFactory.createAccumulators(
              aggregationTypes,
              TSDataType.INT32,
              Collections.emptyList(),
              Collections.emptyMap(),
              true)
          .forEach(o -> aggregators.add(new Aggregator(o, AggregationStep.SINGLE)));

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
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
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
      seriesAggregationScanOperator1.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));
      seriesAggregationScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath2 =
          new MeasurementPath(
              HORIZONTALLY_CONCAT_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
      SeriesAggregationScanOperator seriesAggregationScanOperator2 =
          new SeriesAggregationScanOperator(
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(1),
              aggregators,
              initTimeRangeIterator(groupByTimeParameter, true, true),
              groupByTimeParameter,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
      seriesAggregationScanOperator2.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));
      seriesAggregationScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      HorizontallyConcatOperator horizontallyConcatOperator =
          new HorizontallyConcatOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesAggregationScanOperator1, seriesAggregationScanOperator2),
              Arrays.asList(
                  TSDataType.INT64,
                  TSDataType.DOUBLE,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.DOUBLE,
                  TSDataType.INT32));

      int count = 0;
      while (horizontallyConcatOperator.isBlocked().isDone()
          && horizontallyConcatOperator.hasNext()) {
        TsBlock tsBlock = horizontallyConcatOperator.next();
        assertEquals(6, tsBlock.getValueColumnCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          assertEquals(1, tsBlock.getColumn(0).getLong(i));
          assertEquals(20000 + count, tsBlock.getColumn(1).getDouble(i), 0.00001);
          assertEquals(20000 + count, tsBlock.getColumn(2).getInt(i));
          assertEquals(1, tsBlock.getColumn(3).getLong(i));
          assertEquals(20000 + count, tsBlock.getColumn(4).getDouble(i), 0.00001);
          assertEquals(20000 + count, tsBlock.getColumn(5).getInt(i));
        }
      }
      assertEquals(10, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
