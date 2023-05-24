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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.WindowConcatOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.WindowSplitOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.window.CountSplitWindowManager;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SplitConcatWindowOperatorTest {

  private static final String SPLIT_WINDOW_OPERATOR_TEST_SG = "root.SplitWindowOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();
  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, SPLIT_WINDOW_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testWindowIterator() {
    CountSplitWindowManager.CountIterator countIterator =
        new CountSplitWindowManager.CountIterator(10, 3);
    long[] res = new long[] {1, 2, 1, 2, 1, 2, 1, 2, 1};

    for (long re : res) {
      assert re == countIterator.nextCount();
    }
  }

  private Operator buildSplitWindowOperator(long interval, long step, boolean needConcatOperator) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query_for_split_window_operator");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(
              new PlanFragmentId(queryId, 0), "stub_query_for_split_window_operator");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);

      PlanNodeId planNodeId1 = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          4, new PlanNodeId("4"), WindowSplitOperator.class.getSimpleName());
      if (needConcatOperator) {
        driverContext.addOperatorContext(
            5, new PlanNodeId("5"), WindowConcatOperator.class.getSimpleName());
      }

      MeasurementPath measurementPath1 =
          new MeasurementPath(SPLIT_WINDOW_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(SPLIT_WINDOW_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);

      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath1));
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath2));
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      RowBasedTimeJoinOperator timeJoinOperator =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              Ordering.ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
              new AscTimeComparator());

      WindowSplitOperator windowSplitOperator =
          new WindowSplitOperator(
              driverContext.getOperatorContexts().get(3),
              timeJoinOperator,
              WindowType.RAW_DATA_COUNT_WINDOW,
              interval,
              step,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32));

      if (needConcatOperator) {
        return new WindowConcatOperator(
            driverContext.getOperatorContexts().get(4), windowSplitOperator, interval, step, 2);
      } else {
        return windowSplitOperator;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    return null;
  }

  @Test
  public void splitWindowOperatorTest() {
    try {
      Operator windowSplitOperator = buildSplitWindowOperator(10, 3, false);
      assert windowSplitOperator != null;
      int count = 0;
      int total = 0;
      int[] ans = new int[] {1, 2};
      while (windowSplitOperator.isBlocked().isDone() && windowSplitOperator.hasNext()) {
        TsBlock tsBlock = windowSplitOperator.next();
        if (tsBlock == null) continue;
        int size = tsBlock.getPositionCount();
        if (total == 499) {
          assertEquals(1, size);
        } else {
          assertEquals(ans[count % 2], size);
        }
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = i + total;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          checkValue(expectedTime, tsBlock, i);
        }
        total += size;
        count++;
      }
      assertEquals(500, total);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void splitWindowOperatorTest2() {
    try {
      Operator windowSplitOperator = buildSplitWindowOperator(5, 10, false);
      assert windowSplitOperator != null;
      int total = 0;
      while (windowSplitOperator.isBlocked().isDone() && windowSplitOperator.hasNext()) {
        TsBlock tsBlock = windowSplitOperator.next();
        if (tsBlock == null) continue;
        int size = tsBlock.getPositionCount();
        assertEquals(5, size);
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = i + total;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          checkValue(expectedTime, tsBlock, i);
        }
        total += size;
        total += 5; // step
      }
      assertEquals(500, total);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void splitWindowOperatorTest3() {
    try {
      Operator windowSplitOperator = buildSplitWindowOperator(20, 4, false);
      assert windowSplitOperator != null;
      int total = 0;
      while (windowSplitOperator.isBlocked().isDone() && windowSplitOperator.hasNext()) {
        TsBlock tsBlock = windowSplitOperator.next();
        if (tsBlock == null) continue;
        int size = tsBlock.getPositionCount();
        assertEquals(4, size);
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = i + total;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          checkValue(expectedTime, tsBlock, i);
        }
        total += size;
      }
      assertEquals(500, total);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void checkValue(long expectedTime, TsBlock tsBlock, int i) {
    if (expectedTime < 200) {
      assertEquals(20000 + expectedTime, tsBlock.getColumn(0).getInt(i));
      assertEquals(20000 + expectedTime, tsBlock.getColumn(1).getInt(i));
    } else if (expectedTime < 260
        || (expectedTime >= 300 && expectedTime < 380)
        || expectedTime >= 400) {
      assertEquals(10000 + expectedTime, tsBlock.getColumn(0).getInt(i));
      assertEquals(10000 + expectedTime, tsBlock.getColumn(1).getInt(i));
    } else {
      assertEquals(expectedTime, tsBlock.getColumn(0).getInt(i));
      assertEquals(expectedTime, tsBlock.getColumn(1).getInt(i));
    }
  }

  @Test
  public void testConcatWindowOperator() {
    try {
      Operator windowConcatOperator = buildSplitWindowOperator(10, 3, true);
      assert windowConcatOperator != null;

      long total = 0;
      long windowCount;
      long startTime = 0;

      long restWindow = 8;

      while (windowConcatOperator.isBlocked().isDone() && windowConcatOperator.hasNext()) {
        TsBlock tsBlock = windowConcatOperator.next();
        if (tsBlock == null) continue;
        int size = tsBlock.getPositionCount();
        windowCount = 0;
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = startTime + windowCount;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          checkValue(expectedTime, tsBlock, i);
          windowCount++;
        }

        if (startTime >= 492) {
          assertEquals(restWindow, windowCount);
          restWindow -= 3;
        } else {
          assertEquals(windowCount, 10);
        }
        startTime += 3;
        total += size;
      }
      assertEquals(1655, total);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testConcatWindowOperator2() {
    try {
      Operator windowConcatOperator = buildSplitWindowOperator(10, 5, true);
      assert windowConcatOperator != null;

      long total = 0;
      long windowCount;
      long startTime = 0;

      while (windowConcatOperator.isBlocked().isDone() && windowConcatOperator.hasNext()) {
        TsBlock tsBlock = windowConcatOperator.next();
        if (tsBlock == null) continue;
        int size = tsBlock.getPositionCount();
        windowCount = 0;
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = startTime + windowCount;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          checkValue(expectedTime, tsBlock, i);
          windowCount++;
        }
        if (startTime < 495) assertEquals(windowCount, 10);
        else assertEquals(windowCount, 5);
        total += size;
        startTime += 5;
      }
      assertEquals(995, total);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testConcatWindowOperator3() {
    try {
      Operator windowConcatOperator = buildSplitWindowOperator(5, 10, true);
      assert windowConcatOperator != null;

      long total = 0;
      long windowCount;
      long startTime = 0;

      while (windowConcatOperator.isBlocked().isDone() && windowConcatOperator.hasNext()) {
        TsBlock tsBlock = windowConcatOperator.next();
        if (tsBlock == null) continue;
        int size = tsBlock.getPositionCount();
        windowCount = 0;
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = startTime + windowCount;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          checkValue(expectedTime, tsBlock, i);
          windowCount++;
        }
        assertEquals(windowCount, 5);
        total += size;
        startTime += 10;
      }
      assertEquals(250, total);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
