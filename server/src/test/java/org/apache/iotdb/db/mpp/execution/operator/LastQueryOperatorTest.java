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
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.mpp.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.ImmutableList;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LastQueryOperatorTest {

  private static final String SERIES_SCAN_OPERATOR_TEST_SG = "root.LastQueryOperatorTest";
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
  public void testLastQueryOperator1() {
    try {
      List<Aggregator> aggregators1 = LastQueryUtil.createAggregators(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      List<Aggregator> aggregators2 = LastQueryUtil.createAggregators(TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
      Set<String> allSensors = Sets.newHashSet("sensor0", "sensor1");
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
          2, planNodeId2, UpdateLastCacheOperator.class.getSimpleName());

      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(
          3, planNodeId3, SeriesAggregationScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(
          4, planNodeId4, UpdateLastCacheOperator.class.getSimpleName());

      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, LastQueryOperator.class.getSimpleName());

      driverContext
          .getOperatorContexts()
          .forEach(
              operatorContext -> {
                operatorContext.setMaxRunTime(TEST_TIME_SLICE);
              });

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesAggregationScanOperator seriesAggregationScanOperator1 =
          new SeriesAggregationScanOperator(
              planNodeId1,
              measurementPath1,
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(0),
              aggregators1,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);

      seriesAggregationScanOperator1.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator1 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(1),
              seriesAggregationScanOperator1,
              measurementPath1,
              measurementPath1.getSeriesType(),
              null,
              false);

      SeriesAggregationScanOperator seriesAggregationScanOperator2 =
          new SeriesAggregationScanOperator(
              planNodeId3,
              measurementPath2,
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(2),
              aggregators2,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
      seriesAggregationScanOperator2.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator2 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(3),
              seriesAggregationScanOperator2,
              measurementPath2,
              measurementPath2.getSeriesType(),
              null,
              false);

      LastQueryOperator lastQueryOperator =
          new LastQueryOperator(
              driverContext.getOperatorContexts().get(4),
              ImmutableList.of(updateLastCacheOperator1, updateLastCacheOperator2),
              LastQueryUtil.createTsBlockBuilder());

      int count = 0;
      while (!lastQueryOperator.isFinished()) {
        assertTrue(lastQueryOperator.isBlocked().isDone());
        assertTrue(lastQueryOperator.hasNext());
        TsBlock result = lastQueryOperator.next();
        if (result == null) {
          continue;
        }
        assertEquals(3, result.getValueColumnCount());

        for (int i = 0; i < result.getPositionCount(); i++) {
          assertEquals(499, result.getTimeByIndex(i));
          assertEquals(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor" + count,
              result.getColumn(0).getBinary(i).toString());
          assertEquals("10499", result.getColumn(1).getBinary(i).toString());
          assertEquals(TSDataType.INT32.name(), result.getColumn(2).getBinary(i).toString());
          count++;
        }
      }

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testLastQueryOperator2() {
    try {
      List<Aggregator> aggregators1 = LastQueryUtil.createAggregators(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      List<Aggregator> aggregators2 = LastQueryUtil.createAggregators(TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
      Set<String> allSensors = Sets.newHashSet("sensor0", "sensor1");
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
          2, planNodeId2, UpdateLastCacheOperator.class.getSimpleName());

      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(
          3, planNodeId3, SeriesAggregationScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(
          4, planNodeId4, UpdateLastCacheOperator.class.getSimpleName());

      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId4, LastQueryOperator.class.getSimpleName());

      driverContext
          .getOperatorContexts()
          .forEach(
              operatorContext -> {
                operatorContext.setMaxRunTime(TEST_TIME_SLICE);
              });

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);

      SeriesAggregationScanOperator seriesAggregationScanOperator1 =
          new SeriesAggregationScanOperator(
              planNodeId1,
              measurementPath1,
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(0),
              aggregators1,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
      seriesAggregationScanOperator1.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator1 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(1),
              seriesAggregationScanOperator1,
              measurementPath1,
              measurementPath1.getSeriesType(),
              null,
              false);

      SeriesAggregationScanOperator seriesAggregationScanOperator2 =
          new SeriesAggregationScanOperator(
              planNodeId3,
              measurementPath2,
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(2),
              aggregators2,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
      seriesAggregationScanOperator2.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator2 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(3),
              seriesAggregationScanOperator2,
              measurementPath2,
              measurementPath2.getSeriesType(),
              null,
              false);

      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(6);

      LastQueryUtil.appendLastValue(
          builder, 499, SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor2", "10499", "INT32");
      LastQueryUtil.appendLastValue(
          builder, 499, SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor3", "10499", "INT32");
      LastQueryUtil.appendLastValue(
          builder, 499, SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor4", "10499", "INT32");

      LastQueryOperator lastQueryOperator =
          new LastQueryOperator(
              driverContext.getOperatorContexts().get(4),
              ImmutableList.of(updateLastCacheOperator1, updateLastCacheOperator2),
              builder);

      int count = 0;
      int[] suffix = new int[] {2, 3, 4, 0, 1};
      while (!lastQueryOperator.isFinished()) {
        assertTrue(lastQueryOperator.isBlocked().isDone());
        assertTrue(lastQueryOperator.hasNext());
        TsBlock result = lastQueryOperator.next();
        if (result == null) {
          continue;
        }
        assertEquals(3, result.getValueColumnCount());

        for (int i = 0; i < result.getPositionCount(); i++) {
          assertEquals(499, result.getTimeByIndex(i));
          assertEquals(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor" + suffix[count],
              result.getColumn(0).getBinary(i).toString());
          assertEquals("10499", result.getColumn(1).getBinary(i).toString());
          assertEquals(TSDataType.INT32.name(), result.getColumn(2).getBinary(i).toString());
          count++;
        }
      }

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
