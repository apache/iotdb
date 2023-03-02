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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.MergeSortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.ShowQueriesOperator;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import io.airlift.units.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MergeSortOperatorTest {

  private static final String MERGE_SORT_OPERATOR_TEST_SG = "root.MergeSortOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private static final String DEVICE0 = MERGE_SORT_OPERATOR_TEST_SG + ".device0";
  private static final String DEVICE1 = MERGE_SORT_OPERATOR_TEST_SG + ".device1";
  private static final String DEVICE2 = MERGE_SORT_OPERATOR_TEST_SG + ".device2";
  private static final String DEVICE3 = MERGE_SORT_OPERATOR_TEST_SG + ".device3";

  private int dataNodeId;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, MERGE_SORT_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
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

  // ------------------------------------------------------------------------------------------------
  //                                   order by time - 1
  // ------------------------------------------------------------------------------------------------
  //                                    MergeSortOperator
  //                              ____________|_______________
  //                              /           |               \
  //           SingleDeviceViewOperator SingleDeviceViewOperator SingleDeviceViewOperator
  //                     /                     |                              \
  //        SeriesScanOperator      TimeJoinOperator                TimeJoinOperator
  //                                  /                \              /               \
  //                  SeriesScanOperator SeriesScanOperator SeriesScanOperator   SeriesScanOperator
  // ------------------------------------------------------------------------------------------------
  public MergeSortOperator mergeSortOperatorTest(Ordering timeOrdering, Ordering deviceOrdering) {
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
          11, new PlanNodeId("11"), MergeSortOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath3 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      MeasurementPath measurementPath4 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath5 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);

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
      SingleDeviceViewOperator singleDeviceViewOperator3 =
          new SingleDeviceViewOperator(
              driverContext.getOperatorContexts().get(9),
              DEVICE2,
              timeJoinOperator2,
              Arrays.asList(4, 5),
              tsDataTypes);

      MergeSortOperator mergeSortOperator =
          new MergeSortOperator(
              driverContext.getOperatorContexts().get(10),
              Arrays.asList(
                  singleDeviceViewOperator1, singleDeviceViewOperator2, singleDeviceViewOperator3),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(SortKey.TIME, timeOrdering),
                      new SortItem(SortKey.DEVICE, deviceOrdering)),
                  null,
                  null));
      mergeSortOperator
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return mergeSortOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testOrderByTime1() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest(Ordering.ASC, Ordering.ASC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
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
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          // make sure the device column is by asc
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(tsBlock.getColumn(4).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(5).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 2);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 1500);
  }

  @Test
  public void testOrderByTime2() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest(Ordering.ASC, Ordering.DESC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(6, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(tsBlock.getColumn(4).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(5).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          // make sure the device column is by desc
          assertEquals(checkDevice, 2);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 1500);
  }

  @Test
  public void testOrderByTime3() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest(Ordering.DESC, Ordering.DESC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(6, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(tsBlock.getColumn(4).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(5).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          // make sure the device column is by desc
          assertEquals(checkDevice, 2);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 1500);
  }

  @Test
  public void testOrderByTime4() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest(Ordering.DESC, Ordering.ASC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(6, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          // make sure the device column is by asc
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(tsBlock.getColumn(4).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(5).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 2);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 1500);
  }

  // ------------------------------------------------------------------------------------------------
  //                                        order by time - 2
  // ------------------------------------------------------------------------------------------------
  // [SSO]:SeriesScanOperator
  // [SDO]:SingleDeviceOperator
  //                                       MergeSortOperator
  //                                    ___________|___________
  //                                  /                         \
  //                   MergeSortOperator                        MergeSortOperator
  //                   /               |                       /               \
  //               [SDO]             [SDO]                   [SDO]            [SDO]
  //                 |                 |                       |                |
  //               [SSO]       RowBasedTimeJoinOperator        RowBasedTimeJoinOperator
  // RowBasedTimeJoinOperator
  //                              /         \              /         \       /         \
  //                            [SSO]      [SSO]         [SSO]      [SSO] [SSO]        [SSO]
  // ------------------------------------------------------------------------------------------------
  public MergeSortOperator mergeSortOperatorTest2(Ordering timeOrdering, Ordering deviceOrdering) {
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
          15, new PlanNodeId("15"), MergeSortOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          16, new PlanNodeId("16"), MergeSortOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          17, new PlanNodeId("17"), MergeSortOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath3 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      MeasurementPath measurementPath4 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath5 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      MeasurementPath measurementPath6 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device3.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath7 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device3.sensor1", TSDataType.INT32);

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

      MergeSortOperator mergeSortOperator1 =
          new MergeSortOperator(
              driverContext.getOperatorContexts().get(14),
              Arrays.asList(singleDeviceViewOperator1, singleDeviceViewOperator2),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(SortKey.TIME, timeOrdering),
                      new SortItem(SortKey.DEVICE, deviceOrdering)),
                  null,
                  null));
      mergeSortOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      MergeSortOperator mergeSortOperator2 =
          new MergeSortOperator(
              driverContext.getOperatorContexts().get(15),
              Arrays.asList(singleDeviceViewOperator3, singleDeviceViewOperator4),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(SortKey.TIME, timeOrdering),
                      new SortItem(SortKey.DEVICE, deviceOrdering)),
                  null,
                  null));
      mergeSortOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MergeSortOperator mergeSortOperator =
          new MergeSortOperator(
              driverContext.getOperatorContexts().get(16),
              Arrays.asList(mergeSortOperator1, mergeSortOperator2),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(SortKey.TIME, timeOrdering),
                      new SortItem(SortKey.DEVICE, deviceOrdering)),
                  null,
                  null));
      mergeSortOperator
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return mergeSortOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testOrderByTime1_2() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest2(Ordering.ASC, Ordering.ASC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by asc
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 2);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 3);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 2000);
  }

  @Test
  public void testOrderByTime2_2() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest2(Ordering.ASC, Ordering.DESC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 2);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by desc
          assertEquals(checkDevice, 3);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 2000);
  }

  @Test
  public void testOrderByTime3_2() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest2(Ordering.DESC, Ordering.DESC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 2);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by desc
          assertEquals(checkDevice, 3);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 2000);
  }

  @Test
  public void testOrderByTime4_2() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest2(Ordering.DESC, Ordering.ASC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by asc
          assertEquals(checkDevice, 0);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 1);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 2);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(checkDevice, 3);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }
    assertEquals(count, 2000);
  }
  // ------------------------------------------------------------------------------------------------
  //                                   order by device
  // ------------------------------------------------------------------------------------------------
  // [SSO]:SeriesScanOperator
  //                                     MergeSortOperator
  //                                  ___________|___________
  //                                 /                      \
  //                    DeviceViewOperator              DeviceViewOperator
  //                    /               |                /               \
  //                [SSO]       RowBasedTimeJoinOperator RowBasedTimeJoinOperator
  // RowBasedTimeJoinOperator
  //                               /         \      /         \         /         \
  //                             [SSO]      [SSO] [SSO]      [SSO]  [SSO]       [SSO]
  // ------------------------------------------------------------------------------------------------
  public MergeSortOperator mergeSortOperatorTest3(Ordering timeOrdering, Ordering deviceOrdering) {
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
          13, new PlanNodeId("13"), MergeSortOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device1.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath3 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      MeasurementPath measurementPath4 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath5 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      MeasurementPath measurementPath6 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device3.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath7 =
          new MeasurementPath(MERGE_SORT_OPERATOR_TEST_SG + ".device3.sensor1", TSDataType.INT32);

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

      List<String> devices = new ArrayList<>(Arrays.asList(DEVICE0, DEVICE1, DEVICE2, DEVICE3));
      if (deviceOrdering == Ordering.DESC) Collections.reverse(devices);
      List<List<Integer>> deviceColumnIndex = new ArrayList<>();
      deviceColumnIndex.add(Collections.singletonList(1));
      deviceColumnIndex.add(Arrays.asList(1, 2));
      if (deviceOrdering == Ordering.DESC) Collections.reverse(deviceColumnIndex);
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
      deviceColumnIndex = new ArrayList<>();
      deviceColumnIndex.add(Arrays.asList(1, 2));
      deviceColumnIndex.add(Arrays.asList(1, 2));
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
      MergeSortOperator mergeSortOperator =
          new MergeSortOperator(
              driverContext.getOperatorContexts().get(12),
              Arrays.asList(deviceViewOperator1, deviceViewOperator2),
              tsDataTypes,
              MergeSortComparator.getComparator(
                  Arrays.asList(
                      new SortItem(SortKey.DEVICE, deviceOrdering),
                      new SortItem(SortKey.TIME, timeOrdering)),
                  null,
                  null));
      mergeSortOperator
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
      return mergeSortOperator;
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
      return null;
    }
  }

  @Test
  public void testOrderByDevice1() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest3(Ordering.ASC, Ordering.ASC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
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
    assertEquals(count, 2000);
  }

  @Test
  public void testOrderByDevice2() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest3(Ordering.ASC, Ordering.DESC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
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
    assertEquals(count, 2000);
  }

  @Test
  public void testOrderByDevice3() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest3(Ordering.DESC, Ordering.ASC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by asc
          assertTrue(checkDevice < 500);
          checkDevice++;
          if (checkDevice == 500) {
            lastTime = Long.MAX_VALUE;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 500 && checkDevice < 1000);
          checkDevice++;
          if (checkDevice == 1000) {
            lastTime = Long.MAX_VALUE;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 1000 && checkDevice < 1500);
          checkDevice++;
          if (checkDevice == 1500) {
            lastTime = Long.MAX_VALUE;
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
    assertEquals(count, 2000);
  }

  @Test
  public void testOrderByDevice4() {
    MergeSortOperator mergeSortOperator = mergeSortOperatorTest3(Ordering.DESC, Ordering.DESC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (mergeSortOperator.isBlocked().isDone() && mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice < 500);
          checkDevice++;
          if (checkDevice == 500) {
            lastTime = Long.MAX_VALUE;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 500 && checkDevice < 1000);
          checkDevice++;
          if (checkDevice == 1000) {
            lastTime = Long.MAX_VALUE;
          }
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertTrue(checkDevice >= 1000 && checkDevice < 1500);
          checkDevice++;
          if (checkDevice == 1500) {
            lastTime = Long.MAX_VALUE;
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
    assertEquals(count, 2000);
  }

  // ------------------------------------------------------------------------------------------------
  //                                   order by Time, DataNodeID
  // ------------------------------------------------------------------------------------------------
  //
  //                                     MergeSortOperator
  //                                  ___________|__________
  //                                 /                      \
  //                           SortOperator             SortOperator
  //                                |                        |
  //                        ShowQueriesOperator      ShowQueriesOperator
  // ------------------------------------------------------------------------------------------------
  @Test
  public void mergeSortWithSortOperatorTest() {
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
      PlanNodeId planNodeId0 = new PlanNodeId("0");
      driverContext.addOperatorContext(0, planNodeId0, ShowQueriesOperator.class.getSimpleName());
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId1, ShowQueriesOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SortOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(3, planNodeId3, SortOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, MergeSortOperator.class.getSimpleName());

      List<OperatorContext> operatorContexts = driverContext.getOperatorContexts();
      List<TSDataType> dataTypes = DatasetHeaderFactory.getShowQueriesHeader().getRespDataTypes();
      Comparator<MergeSortKey> comparator =
          MergeSortComparator.getComparator(
              Arrays.asList(
                  new SortItem(SortKey.TIME, Ordering.ASC),
                  new SortItem(SortKey.DATANODEID, Ordering.DESC)),
              ImmutableList.of(-1, 1),
              ImmutableList.of(TSDataType.INT64, TSDataType.INT32));

      Coordinator coordinator1 = Mockito.mock(Coordinator.class);
      Mockito.when(coordinator1.getAllQueryExecutions())
          .thenReturn(
              ImmutableList.of(
                  new FakeQueryExecution(3, "20221229_000000_00003_1", "sql3_node1"),
                  new FakeQueryExecution(1, "20221229_000000_00001_1", "sql1_node1"),
                  new FakeQueryExecution(2, "20221229_000000_00002_1", "sql2_node1")));
      Coordinator coordinator2 = Mockito.mock(Coordinator.class);
      Mockito.when(coordinator2.getAllQueryExecutions())
          .thenReturn(
              ImmutableList.of(
                  new FakeQueryExecution(3, "20221229_000000_00003_2", "sql3_node2"),
                  new FakeQueryExecution(2, "20221229_000000_00002_2", "sql2_node2"),
                  new FakeQueryExecution(1, "20221229_000000_00001_2", "sql1_node2")));

      ShowQueriesOperator showQueriesOperator1 =
          new ShowQueriesOperator(operatorContexts.get(0), planNodeId0, coordinator1);
      ShowQueriesOperator showQueriesOperator2 =
          new ShowQueriesOperator(operatorContexts.get(1), planNodeId1, coordinator2);
      SortOperator sortOperator1 =
          new SortOperator(operatorContexts.get(2), showQueriesOperator1, dataTypes, comparator);
      SortOperator sortOperator2 =
          new SortOperator(operatorContexts.get(3), showQueriesOperator2, dataTypes, comparator);
      Operator root =
          new MergeSortOperator(
              operatorContexts.get(4),
              ImmutableList.of(sortOperator1, sortOperator2),
              dataTypes,
              comparator);
      root.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      int index = 0;
      // Time ASC
      long[] expectedTime = new long[] {1, 1, 2, 2, 3, 3};
      // DataNodeId DESC if Times are equal
      String[] expectedQueryId =
          new String[] {
            "20221229_000000_00001_2",
            "20221229_000000_00001_1",
            "20221229_000000_00002_2",
            "20221229_000000_00002_1",
            "20221229_000000_00003_2",
            "20221229_000000_00003_1"
          };
      int[] expectedDataNodeId = new int[] {2, 1, 2, 1, 2, 1};
      String[] expectedStatement =
          new String[] {
            "sql1_node2", "sql1_node1", "sql2_node2", "sql2_node1", "sql3_node2", "sql3_node1"
          };
      while (root.isBlocked().isDone() && root.hasNext()) {
        TsBlock result = root.next();
        if (result == null) {
          continue;
        }

        for (int i = 0; i < result.getPositionCount(); i++, index++) {
          assertEquals(expectedTime[index], result.getTimeColumn().getLong(i));
          assertEquals(expectedQueryId[index], result.getColumn(0).getBinary(i).toString());
          assertEquals(expectedDataNodeId[index], result.getColumn(1).getInt(i));
          assertEquals(expectedStatement[index], result.getColumn(3).getBinary(i).toString());
        }
      }
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  static class FakeQueryExecution implements IQueryExecution {
    private final long startTime;
    private final String queryId;
    private final String sql;

    FakeQueryExecution(long startTime, String queryId, String sql) {
      this.startTime = startTime;
      this.queryId = queryId;
      this.sql = sql;
    }

    @Override
    public String getQueryId() {
      return queryId;
    }

    @Override
    public long getStartExecutionTime() {
      return startTime;
    }

    @Override
    public void recordExecutionTime(long executionTime) {}

    @Override
    public long getTotalExecutionTime() {
      return 0;
    }

    @Override
    public Optional<String> getExecuteSQL() {
      return Optional.of(sql);
    }

    @Override
    public Statement getStatement() {
      return null;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void stopAndCleanup() {}

    @Override
    public void cancel() {}

    @Override
    public ExecutionResult getStatus() {
      return null;
    }

    @Override
    public Optional<TsBlock> getBatchResult() {
      return Optional.empty();
    }

    @Override
    public Optional<ByteBuffer> getByteBufferBatchResult() {
      return Optional.empty();
    }

    @Override
    public boolean hasNextResult() {
      return false;
    }

    @Override
    public int getOutputValueColumnCount() {
      return 0;
    }

    @Override
    public DatasetHeader getDatasetHeader() {
      return null;
    }

    @Override
    public boolean isQuery() {
      return false;
    }
  }
}
