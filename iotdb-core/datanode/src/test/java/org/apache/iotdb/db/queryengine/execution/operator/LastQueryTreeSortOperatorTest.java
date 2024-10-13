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
package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQuerySortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LastQueryTreeSortOperatorTest {

  private static final String SERIES_SCAN_OPERATOR_TEST_SG = "root.LastQueryTreeSortOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

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
  public void testLastQuerySortOperatorAsc() {
    try {
      List<TreeAggregator> aggregators1 = LastQueryUtil.createAggregators(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      List<TreeAggregator> aggregators2 = LastQueryUtil.createAggregators(TSDataType.INT32);
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
              IFullPath.convertToIFullPath(measurementPath1),
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(0),
              aggregators1,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
              true);
      seriesAggregationScanOperator1.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator1 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(1),
              seriesAggregationScanOperator1,
              measurementPath1,
              measurementPath1.getSeriesType(),
              null,
              false,
              false);

      SeriesAggregationScanOperator seriesAggregationScanOperator2 =
          new SeriesAggregationScanOperator(
              planNodeId3,
              IFullPath.convertToIFullPath(measurementPath2),
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(2),
              aggregators2,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
              true);
      seriesAggregationScanOperator2.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator2 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(3),
              seriesAggregationScanOperator2,
              measurementPath2,
              measurementPath2.getSeriesType(),
              null,
              false,
              false);

      LastQuerySortOperator lastQuerySortOperator =
          new LastQuerySortOperator(
              driverContext.getOperatorContexts().get(4),
              LastQueryUtil.createTsBlockBuilder().build(),
              ImmutableList.of(updateLastCacheOperator1, updateLastCacheOperator2),
              Comparator.naturalOrder());

      int count = 0;
      while (!lastQuerySortOperator.isFinished()) {
        assertTrue(lastQuerySortOperator.isBlocked().isDone());
        assertTrue(lastQuerySortOperator.hasNext());
        TsBlock result = lastQuerySortOperator.next();
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

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testLastQuerySortOperatorDesc() {
    try {
      List<TreeAggregator> aggregators1 = LastQueryUtil.createAggregators(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      List<TreeAggregator> aggregators2 = LastQueryUtil.createAggregators(TSDataType.INT32);
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
              IFullPath.convertToIFullPath(measurementPath1),
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(0),
              aggregators1,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
              true);
      seriesAggregationScanOperator1.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator1 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(1),
              seriesAggregationScanOperator1,
              measurementPath1,
              measurementPath1.getSeriesType(),
              null,
              false,
              false);

      SeriesAggregationScanOperator seriesAggregationScanOperator2 =
          new SeriesAggregationScanOperator(
              planNodeId3,
              IFullPath.convertToIFullPath(measurementPath2),
              Ordering.DESC,
              scanOptionsBuilder.build(),
              driverContext.getOperatorContexts().get(2),
              aggregators2,
              initTimeRangeIterator(null, false, true),
              null,
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
              true);
      seriesAggregationScanOperator2.initQueryDataSource(
          new QueryDataSource(seqResources, unSeqResources));

      UpdateLastCacheOperator updateLastCacheOperator2 =
          new UpdateLastCacheOperator(
              driverContext.getOperatorContexts().get(3),
              seriesAggregationScanOperator2,
              measurementPath2,
              measurementPath2.getSeriesType(),
              null,
              false,
              false);

      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(6);

      LastQueryUtil.appendLastValue(
          builder, 499, SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor4", "10499", "INT32");
      LastQueryUtil.appendLastValue(
          builder, 499, SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor3", "10499", "INT32");
      LastQueryUtil.appendLastValue(
          builder, 499, SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor2", "10499", "INT32");

      LastQuerySortOperator lastQuerySortOperator =
          new LastQuerySortOperator(
              driverContext.getOperatorContexts().get(4),
              builder.build(),
              ImmutableList.of(updateLastCacheOperator2, updateLastCacheOperator1),
              Comparator.reverseOrder());

      int count = 0;
      int[] suffix = new int[] {4, 3, 2, 1, 0};
      while (!lastQuerySortOperator.isFinished()) {
        assertTrue(lastQuerySortOperator.isBlocked().isDone());
        assertTrue(lastQuerySortOperator.hasNext());
        TsBlock result = lastQuerySortOperator.next();
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

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
