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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.MergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TopKOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * The original data and structure of this test is same as {@link
 * org.apache.iotdb.db.queryengine.execution.operator.MergeSortOperatorTest}.
 */
public class TopKOperatorTest {
  private static final String TOP_K_OPERATOR_TEST_SG = "root.TopKOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private static final String DEVICE0 = TOP_K_OPERATOR_TEST_SG + ".device0";
  private static final String DEVICE1 = TOP_K_OPERATOR_TEST_SG + ".device1";
  private static final String DEVICE2 = TOP_K_OPERATOR_TEST_SG + ".device2";
  private static final String DEVICE3 = TOP_K_OPERATOR_TEST_SG + ".device3";

  private int dataNodeId;

  private int maxTsBlockLineNumber;

  private final int limitValue = 100;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    maxTsBlockLineNumber = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(3);
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, TOP_K_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(maxTsBlockLineNumber);
  }

  long getValue(long expectedTime) {
    if (expectedTime < 200) {
      return 20000 + expectedTime;
    } else if (expectedTime < 260
        || (expectedTime >= 300 && expectedTime < 380)
        || expectedTime >= 400) {
      return 10000 + expectedTime;
    } else {
      return expectedTime;
    }
  }

  // ----------------------------------------------------------------------------------------------
  //                                   order by time - 1
  // ----------------------------------------------------------------------------------------------
  //                                     TopKOperator
  //                              ____________|_______________
  //                              /           |               \
  //           SingleDeviceViewOperator SingleDeviceViewOperator SingleDeviceViewOperator
  //                     /                     |                              \
  //        SeriesScanOperator      TimeJoinOperator                TimeJoinOperator
  //                                  /                \              /               \
  //                  SeriesScanOperator SeriesScanOperator SeriesScanOperator   SeriesScanOperator
  // ----------------------------------------------------------------------------------------------
  public TopKOperator topKOperatorTest(
      Ordering timeOrdering, Ordering deviceOrdering, int limitValue) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          6, new PlanNodeId("6"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          7, new PlanNodeId("7"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          8, new PlanNodeId("8"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          9, new PlanNodeId("9"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          10, new PlanNodeId("10"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          11, new PlanNodeId("11"), TopKOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath1));
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath2 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath2));
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath3 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath3,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath3));
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath4 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath4,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath4));
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath5 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath5,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath5));
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      List<TSDataType> tsDataTypes =
          new LinkedList<>(
              Arrays.asList(
                  TSDataType.TEXT,
                  TSDataType.INT32,
                  TSDataType.INT32,
                  TSDataType.INT32,
                  TSDataType.INT32,
                  TSDataType.INT32));
      SingleDeviceViewOperator singleDeviceViewOperator1 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(5),
              DEVICE0,
              seriesScanOperator1,
              Collections.singletonList(1),
              tsDataTypes);

      RowBasedTimeJoinOperator timeJoinOperator1 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(6),
              Arrays.asList(seriesScanOperator2, seriesScanOperator3),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      SingleDeviceViewOperator singleDeviceViewOperator2 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(7),
              DEVICE1,
              timeJoinOperator1,
              Arrays.asList(2, 3),
              tsDataTypes);

      RowBasedTimeJoinOperator timeJoinOperator2 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(8),
              Arrays.asList(seriesScanOperator4, seriesScanOperator5),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      SingleDeviceViewOperator singleDeviceViewOperator3 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(9),
              DEVICE2,
              timeJoinOperator2,
              Arrays.asList(4, 5),
              tsDataTypes);

      TopKOperator topKOperator =
          new TopKOperator(
              driverContext.getOperatorContexts().get(10),
              Arrays.asList(
                  singleDeviceViewOperator1, singleDeviceViewOperator2, singleDeviceViewOperator3),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(OrderByKey.TIME, timeOrdering),
                      new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                  Arrays.asList(-1, 0),
                  Arrays.asList(TSDataType.INT64, TSDataType.TEXT)),
              limitValue,
              true);
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return topKOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testOrderByTime1() throws Exception {
    TopKOperator topKOperator = topKOperatorTest(Ordering.ASC, Ordering.ASC, limitValue);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(6, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          // make sure the device column is by asc
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(3).getInt(i));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(4).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(5).getInt(i));
          assertEquals(2, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(limitValue, count);
  }

  @Test
  public void testOrderByTime2() throws Exception {
    TopKOperator topKOperator = topKOperatorTest(Ordering.ASC, Ordering.DESC, limitValue);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(6, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(4).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(5).getInt(i));
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(3).getInt(i));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          // make sure the device column is by desc
          assertEquals(2, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(limitValue, count);
  }

  public TopKOperator topKOperatorTest2(
      Ordering timeOrdering, Ordering deviceOrdering, int limitValue) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId6 = new PlanNodeId("6");
      driverContext.addOperatorContext(6, planNodeId6, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId7 = new PlanNodeId("7");
      driverContext.addOperatorContext(7, planNodeId7, SeriesScanOperator.class.getSimpleName());

      driverContext.addOperatorContext(
          8, new PlanNodeId("8"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          9, new PlanNodeId("9"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          10, new PlanNodeId("10"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          11, new PlanNodeId("11"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          12, new PlanNodeId("12"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          13, new PlanNodeId("13"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          14, new PlanNodeId("14"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          15, new PlanNodeId("15"), TopKOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          16, new PlanNodeId("16"), TopKOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          17, new PlanNodeId("17"), TopKOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath1));
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath2 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath2));
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath3 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath3,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath3));
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator3
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath4 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath4,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath4));
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator4
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath5 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath5,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath5));
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator5
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath6 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device3.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator6 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(5),
              planNodeId6,
              measurementPath6,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath6));
      seriesScanOperator6.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator6
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath7 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device3.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator7 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(6),
              planNodeId7,
              measurementPath7,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath7));
      seriesScanOperator7.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator7
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      RowBasedTimeJoinOperator timeJoinOperator1 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(7),
              Arrays.asList(seriesScanOperator2, seriesScanOperator3),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      timeJoinOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      RowBasedTimeJoinOperator timeJoinOperator2 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(8),
              Arrays.asList(seriesScanOperator4, seriesScanOperator5),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      timeJoinOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      RowBasedTimeJoinOperator timeJoinOperator3 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(9),
              Arrays.asList(seriesScanOperator6, seriesScanOperator7),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      timeJoinOperator3
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      List<TSDataType> tsDataTypes =
          new LinkedList<>(Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32));
      SingleDeviceViewOperator singleDeviceViewOperator1 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(10),
              DEVICE0,
              seriesScanOperator1,
              Collections.singletonList(1),
              tsDataTypes);
      SingleDeviceViewOperator singleDeviceViewOperator2 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(11),
              DEVICE1,
              timeJoinOperator1,
              Arrays.asList(1, 2),
              tsDataTypes);
      SingleDeviceViewOperator singleDeviceViewOperator3 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(12),
              DEVICE2,
              timeJoinOperator2,
              Arrays.asList(1, 2),
              tsDataTypes);
      SingleDeviceViewOperator singleDeviceViewOperator4 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(13),
              DEVICE3,
              timeJoinOperator3,
              Arrays.asList(1, 2),
              tsDataTypes);

      TopKOperator topKOperator1 =
          new TopKOperator(
              driverContext.getOperatorContexts().get(14),
              Arrays.asList(singleDeviceViewOperator1, singleDeviceViewOperator2),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(OrderByKey.TIME, timeOrdering),
                      new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                  Arrays.asList(-1, 0),
                  Arrays.asList(TSDataType.INT64, TSDataType.TEXT)),
              limitValue,
              true);
      topKOperator1.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      TopKOperator topKOperator2 =
          new TopKOperator(
              driverContext.getOperatorContexts().get(15),
              Arrays.asList(singleDeviceViewOperator3, singleDeviceViewOperator4),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(OrderByKey.TIME, timeOrdering),
                      new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                  Arrays.asList(-1, 0),
                  Arrays.asList(TSDataType.INT64, TSDataType.TEXT)),
              limitValue,
              true);
      topKOperator2.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      TopKOperator topKOperator =
          new TopKOperator(
              driverContext.getOperatorContexts().get(16),
              Arrays.asList(topKOperator1, topKOperator2),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(OrderByKey.TIME, timeOrdering),
                      new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                  Arrays.asList(-1, 0),
                  Arrays.asList(TSDataType.INT64, TSDataType.TEXT)),
              limitValue,
              true);
      topKOperator.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return topKOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testOrderByTime1_2() throws Exception {
    TopKOperator topKOperator = topKOperatorTest2(Ordering.ASC, Ordering.ASC, limitValue);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by asc
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(2, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(3, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(limitValue, count);
  }

  @Test
  public void testOrderByTime2_2() throws Exception {
    TopKOperator topKOperator = topKOperatorTest2(Ordering.ASC, Ordering.DESC, limitValue);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;

    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertEquals(getValue(lastTime), tsBlock.getColumn(2).getInt(i));
          assertEquals(2, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(getValue(lastTime), tsBlock.getColumn(1).getInt(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by desc
          assertEquals(3, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(limitValue, count);
  }

  // -----------------------------------------------------------------------------------
  //                                   order by device
  // -----------------------------------------------------------------------------------
  // [SSO]:SeriesScanOperator
  //                                     TopKOperator
  //                                  ___________|___________
  //                                 /                      \
  //                    DeviceViewOperator              DeviceViewOperator
  //                    /               |                /               \
  //                [SSO]       RowBasedTimeJoinOperator RowBasedTimeJoinOperator
  // RowBasedTimeJoinOperator
  //                               /         \      /         \         /         \
  //                             [SSO]      [SSO] [SSO]      [SSO]  [SSO]       [SSO]
  // ------------------------------------------------------------------------------------
  public TopKOperator topKOperatorTest3(
      Ordering timeOrdering, Ordering deviceOrdering, int limitValue) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId6 = new PlanNodeId("6");
      driverContext.addOperatorContext(6, planNodeId6, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId7 = new PlanNodeId("7");
      driverContext.addOperatorContext(7, planNodeId7, SeriesScanOperator.class.getSimpleName());

      driverContext.addOperatorContext(
          8, new PlanNodeId("8"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          9, new PlanNodeId("9"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          10, new PlanNodeId("10"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          11, new PlanNodeId("11"), DeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          12, new PlanNodeId("12"), DeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          13, new PlanNodeId("13"), TopKOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath1));
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath2 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath2));
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath3 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath3,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath3));
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath4 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath4,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath4));
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath5 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath5,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath5));
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath6 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device3.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator6 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(5),
              planNodeId6,
              measurementPath6,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath6));
      seriesScanOperator6.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath7 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device3.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator7 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(6),
              planNodeId7,
              measurementPath7,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath7));
      seriesScanOperator7.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      List<TSDataType> tsDataTypes =
          new LinkedList<>(Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32));
      RowBasedTimeJoinOperator timeJoinOperator1 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(7),
              Arrays.asList(seriesScanOperator2, seriesScanOperator3),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      RowBasedTimeJoinOperator timeJoinOperator2 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(8),
              Arrays.asList(seriesScanOperator4, seriesScanOperator5),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      RowBasedTimeJoinOperator timeJoinOperator3 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(9),
              Arrays.asList(seriesScanOperator6, seriesScanOperator7),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      List<String> devices = new ArrayList<>(Arrays.asList(DEVICE0, DEVICE1, DEVICE2, DEVICE3));
      if (deviceOrdering == Ordering.DESC) {
        Collections.reverse(devices);
      }
      List<List<Integer>> deviceColumnIndex =
          Arrays.asList(Collections.singletonList(1), Arrays.asList(1, 2));
      if (deviceOrdering == Ordering.DESC) {
        Collections.reverse(deviceColumnIndex);
      }
      DeviceViewOperator deviceViewOperator1 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(10),
              deviceOrdering == Ordering.ASC
                  ? Arrays.asList(DEVICE0, DEVICE1)
                  : Arrays.asList(DEVICE1, DEVICE0),
              deviceOrdering == Ordering.ASC
                  ? Arrays.asList(seriesScanOperator1, timeJoinOperator1)
                  : Arrays.asList(timeJoinOperator1, seriesScanOperator1),
              deviceColumnIndex,
              tsDataTypes);
      deviceColumnIndex = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(1, 2));
      DeviceViewOperator deviceViewOperator2 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(11),
              deviceOrdering == Ordering.ASC
                  ? Arrays.asList(DEVICE2, DEVICE3)
                  : Arrays.asList(DEVICE3, DEVICE2),
              deviceOrdering == Ordering.ASC
                  ? Arrays.asList(timeJoinOperator2, timeJoinOperator3)
                  : Arrays.asList(timeJoinOperator3, timeJoinOperator2),
              deviceColumnIndex,
              tsDataTypes);
      TopKOperator topKOperator =
          new TopKOperator(
              driverContext.getOperatorContexts().get(12),
              Arrays.asList(deviceViewOperator1, deviceViewOperator2),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(OrderByKey.DEVICE, deviceOrdering),
                      new SortItem(OrderByKey.TIME, timeOrdering)),
                  Arrays.asList(0, -1),
                  Arrays.asList(TSDataType.TEXT, TSDataType.INT64)),
              limitValue,
              false);
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return topKOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testOrderByDevice1() throws Exception {
    TopKOperator topKOperator = topKOperatorTest3(Ordering.ASC, Ordering.ASC, limitValue);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by asc
          assertTrue(checkDevice < 500);
          checkDevice++;
          if (checkDevice == 500) {
            lastTime = -1;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 500 && checkDevice < 1000);
          checkDevice++;
          if (checkDevice == 1000) {
            lastTime = -1;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 1000 && checkDevice < 1500);
          checkDevice++;
          if (checkDevice == 1500) {
            lastTime = -1;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 1500 && checkDevice < 2000);
          checkDevice++;
        } else {
          fail();
        }
      }
    }

    assertEquals(limitValue, count);
  }

  @Test
  public void testOrderByDevice2() throws Exception {
    TopKOperator topKOperator = topKOperatorTest3(Ordering.ASC, Ordering.DESC, limitValue);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice < 500);
          checkDevice++;
          if (checkDevice == 500) {
            lastTime = -1;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 500 && checkDevice < 1000);
          checkDevice++;
          if (checkDevice == 1000) {
            lastTime = -1;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 1000 && checkDevice < 1500);
          checkDevice++;
          if (checkDevice == 1500) {
            lastTime = -1;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by desc
          assertTrue(checkDevice >= 1500 && checkDevice < 2000);
          checkDevice++;
        } else {
          fail();
        }
      }
    }

    assertEquals(limitValue, count);
  }

  @Mock Operator childOperator1 = Mockito.mock(Operator.class);
  @Mock Operator childOperator2 = Mockito.mock(Operator.class);

  private TsBlock[] constructOutput(long[] time) {
    TsBlockBuilder tsBlockBuilder1 =
        new TsBlockBuilder(3, Collections.singletonList(TSDataType.INT64));
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder1.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = tsBlockBuilder1.getValueColumnBuilders();
    for (int i = 0; i < time.length / 2; i++) {
      timeColumnBuilder.writeLong(time[i]);
      for (ColumnBuilder columnBuilder : columnBuilders) {
        columnBuilder.writeLong(time[i]);
      }
      tsBlockBuilder1.declarePosition();
    }
    TsBlock tsBlock1 = tsBlockBuilder1.build();
    TsBlockBuilder tsBlockBuilder2 =
        new TsBlockBuilder(3, Collections.singletonList(TSDataType.INT64));
    timeColumnBuilder = tsBlockBuilder2.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders2 = tsBlockBuilder2.getValueColumnBuilders();
    for (int i = time.length / 2; i < time.length; i++) {
      timeColumnBuilder.writeLong(time[i]);
      for (ColumnBuilder columnBuilder : columnBuilders2) {
        columnBuilder.writeLong(time[i]);
      }
      tsBlockBuilder2.declarePosition();
    }
    TsBlock tsBlock2 = tsBlockBuilder2.build();
    return new TsBlock[] {tsBlock1, tsBlock2};
  }

  @Test
  public void topKOperatorSortTest() throws Exception {
    AtomicInteger count1 = new AtomicInteger(0);
    AtomicInteger count2 = new AtomicInteger(0);
    long[] time1 = new long[] {1, 2, 3, 4, 5, 6};
    long[] time2 = new long[] {3, 3, 6, 7, 8, 9};
    long[] ans = new long[] {1, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9};

    TsBlock[] tsBlocks1 = constructOutput(time1);
    TsBlock[] tsBlocks2 = constructOutput(time2);

    ListenableFuture<Boolean> mockFuture = Mockito.mock(ListenableFuture.class);
    doReturn(mockFuture).when(childOperator1).isBlocked();
    doReturn(mockFuture).when(childOperator2).isBlocked();
    when(mockFuture.isDone()).thenReturn(true);
    when(childOperator1.nextWithTimer())
        .thenAnswer(
            (Answer<TsBlock>)
                invocationOnMock -> {
                  int count = count1.getAndIncrement();
                  if (count == 0) {
                    return tsBlocks1[0];
                  } else if (count == 1) {
                    return tsBlocks1[1];
                  } else {
                    return null;
                  }
                });
    when(childOperator2.nextWithTimer())
        .thenAnswer(
            (Answer<TsBlock>)
                invocationOnMock -> {
                  int count = count2.getAndIncrement();
                  if (count == 0) {
                    return tsBlocks2[0];
                  } else if (count == 1) {
                    return tsBlocks2[1];
                  } else {
                    return null;
                  }
                });

    when(childOperator1.hasNextWithTimer())
        .thenAnswer(
            (Answer<Boolean>)
                invocationOnMock -> {
                  int count = count1.get();
                  return count < 2;
                });

    when(childOperator2.hasNextWithTimer())
        .thenAnswer(
            (Answer<Boolean>)
                invocationOnMock -> {
                  int count = count2.get();
                  return count < 2;
                });

    QueryId queryId = new QueryId("stub_query");
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, MergeSortOperator.class.getSimpleName());

    TopKOperator topKOperator =
        new TopKOperator(
            driverContext.getOperatorContexts().get(0),
            Arrays.asList(childOperator1, childOperator2),
            Collections.singletonList(TSDataType.INT64),
            MergeSortComparator.getComparator(
                Collections.singletonList(new SortItem(OrderByKey.TIME, Ordering.ASC)),
                Collections.singletonList(-1),
                Collections.singletonList(TSDataType.INT64)),
            20,
            true);
    OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    int index = 0;
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock result = topKOperator.next();
      if (result == null) {
        continue;
      }
      for (int i = 0; i < result.getPositionCount(); i++) {
        long time = result.getTimeByIndex(i);
        assertEquals(time, ans[index++]);
      }
    }
    assertEquals(index, ans.length);
  }

  // ----------------------------------------------------------------------------------------------
  //                     order by time - all result of SeriesScan is empty
  // ----------------------------------------------------------------------------------------------
  //                                     TopKOperator
  //                              ____________|_______________
  //                              /           |               \
  //           SingleDeviceViewOperator SingleDeviceViewOperator SingleDeviceViewOperator
  //                     /                     |                              \
  //        SeriesScanOperator      TimeJoinOperator                TimeJoinOperator
  //                                  /                \              /               \
  //                  SeriesScanOperator SeriesScanOperator SeriesScanOperator   SeriesScanOperator
  // ----------------------------------------------------------------------------------------------
  public TopKOperator emptyTopKOperatorTest(
      Ordering timeOrdering, Ordering deviceOrdering, int limitValue) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          6, new PlanNodeId("6"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          7, new PlanNodeId("7"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          8, new PlanNodeId("8"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          9, new PlanNodeId("9"), RowBasedTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          10, new PlanNodeId("10"), SingleDeviceViewOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          11, new PlanNodeId("11"), TopKOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath1));
      seriesScanOperator1.initQueryDataSource(
          new QueryDataSource(Collections.emptyList(), Collections.emptyList()));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath2 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath2));
      seriesScanOperator2.initQueryDataSource(
          new QueryDataSource(Collections.emptyList(), Collections.emptyList()));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath3 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath3,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath3));
      seriesScanOperator3.initQueryDataSource(
          new QueryDataSource(Collections.emptyList(), Collections.emptyList()));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath4 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath4,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath4));
      seriesScanOperator4.initQueryDataSource(
          new QueryDataSource(Collections.emptyList(), Collections.emptyList()));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath5 =
          new MeasurementPath(TOP_K_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath5,
              timeOrdering,
              SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath5));
      seriesScanOperator5.initQueryDataSource(
          new QueryDataSource(Collections.emptyList(), Collections.emptyList()));
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      List<TSDataType> tsDataTypes =
          new LinkedList<>(
              Arrays.asList(
                  TSDataType.TEXT,
                  TSDataType.INT32,
                  TSDataType.INT32,
                  TSDataType.INT32,
                  TSDataType.INT32,
                  TSDataType.INT32));
      SingleDeviceViewOperator singleDeviceViewOperator1 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(5),
              DEVICE0,
              seriesScanOperator1,
              Collections.singletonList(1),
              tsDataTypes);

      RowBasedTimeJoinOperator timeJoinOperator1 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(6),
              Arrays.asList(seriesScanOperator2, seriesScanOperator3),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      SingleDeviceViewOperator singleDeviceViewOperator2 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(7),
              DEVICE1,
              timeJoinOperator1,
              Arrays.asList(2, 3),
              tsDataTypes);

      RowBasedTimeJoinOperator timeJoinOperator2 =
          new RowBasedTimeJoinOperator(
              driverContext.getOperatorContexts().get(8),
              Arrays.asList(seriesScanOperator4, seriesScanOperator5),
              timeOrdering,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(
                      new InputLocation(0, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()),
                  new SingleColumnMerger(
                      new InputLocation(1, 0),
                      timeOrdering == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator())),
              timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      SingleDeviceViewOperator singleDeviceViewOperator3 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(9),
              DEVICE2,
              timeJoinOperator2,
              Arrays.asList(4, 5),
              tsDataTypes);

      TopKOperator topKOperator =
          new TopKOperator(
              driverContext.getOperatorContexts().get(10),
              Arrays.asList(
                  singleDeviceViewOperator1, singleDeviceViewOperator2, singleDeviceViewOperator3),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(OrderByKey.TIME, timeOrdering),
                      new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                  Arrays.asList(-1, 0),
                  Arrays.asList(TSDataType.INT64, TSDataType.TEXT)),
              limitValue,
              true);
      OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return topKOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testEmptyTopKOperator() throws Exception {
    TopKOperator topKOperator = emptyTopKOperatorTest(Ordering.ASC, Ordering.ASC, limitValue);
    while (topKOperator.isBlocked().isDone() && topKOperator.hasNext()) {
      TsBlock tsBlock = topKOperator.next();
      if (tsBlock == null || tsBlock.isEmpty()) {
        continue;
      }
      fail();
    }
  }
}
