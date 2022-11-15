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
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.MergeSortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.DeviceMergeToolKit;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeMergeToolKit;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

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

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, MERGE_SORT_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      fragmentInstanceContext.addOperatorContext(
          2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      fragmentInstanceContext.addOperatorContext(
          3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      fragmentInstanceContext.addOperatorContext(
          4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      fragmentInstanceContext.addOperatorContext(
          5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          6, new PlanNodeId("6"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          7, new PlanNodeId("7"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          8, new PlanNodeId("8"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          9, new PlanNodeId("9"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          10, new PlanNodeId("10"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
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
              planNodeId1,
              measurementPath1,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              planNodeId2,
              measurementPath2,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              planNodeId3,
              measurementPath3,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(2),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              planNodeId4,
              measurementPath4,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(3),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              planNodeId5,
              measurementPath5,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(4),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

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
              fragmentInstanceContext.getOperatorContexts().get(5),
              DEVICE0,
              seriesScanOperator1,
              Collections.singletonList(1),
              tsDataTypes);

      TimeJoinOperator timeJoinOperator1 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(6),
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
              fragmentInstanceContext.getOperatorContexts().get(7),
              DEVICE1,
              timeJoinOperator1,
              Arrays.asList(2, 3),
              tsDataTypes);

      TimeJoinOperator timeJoinOperator2 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(8),
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
              fragmentInstanceContext.getOperatorContexts().get(9),
              DEVICE2,
              timeJoinOperator2,
              Arrays.asList(4, 5),
              tsDataTypes);

      return new MergeSortOperator(
          fragmentInstanceContext.getOperatorContexts().get(10),
          Arrays.asList(
              singleDeviceViewOperator1, singleDeviceViewOperator2, singleDeviceViewOperator3),
          tsDataTypes,
          new TimeMergeToolKit(
              new ArrayList<>(
                  Arrays.asList(
                      new SortItem(SortKey.TIME, timeOrdering),
                      new SortItem(SortKey.DEVICE, deviceOrdering))),
              3));
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
  //               [SSO]       TimeJoinOperator        TimeJoinOperator   TimeJoinOperator
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      fragmentInstanceContext.addOperatorContext(
          2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      fragmentInstanceContext.addOperatorContext(
          3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      fragmentInstanceContext.addOperatorContext(
          4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      fragmentInstanceContext.addOperatorContext(
          5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId6 = new PlanNodeId("6");
      fragmentInstanceContext.addOperatorContext(
          6, planNodeId6, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId7 = new PlanNodeId("7");
      fragmentInstanceContext.addOperatorContext(
          7, planNodeId7, SeriesScanOperator.class.getSimpleName());

      fragmentInstanceContext.addOperatorContext(
          8, new PlanNodeId("8"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          9, new PlanNodeId("9"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          10, new PlanNodeId("10"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          11, new PlanNodeId("11"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          12, new PlanNodeId("12"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          13, new PlanNodeId("13"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          14, new PlanNodeId("14"), SingleDeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          15, new PlanNodeId("15"), MergeSortOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          16, new PlanNodeId("16"), MergeSortOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
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
              planNodeId1,
              measurementPath1,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              planNodeId2,
              measurementPath2,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              planNodeId3,
              measurementPath3,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(2),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              planNodeId4,
              measurementPath4,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(3),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              planNodeId5,
              measurementPath5,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(4),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator6 =
          new SeriesScanOperator(
              planNodeId6,
              measurementPath6,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(5),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator6.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator7 =
          new SeriesScanOperator(
              planNodeId7,
              measurementPath7,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(6),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator7.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

      List<TSDataType> tsDataTypes =
          new LinkedList<>(Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32));

      TimeJoinOperator timeJoinOperator1 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(7),
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

      TimeJoinOperator timeJoinOperator2 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(8),
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

      TimeJoinOperator timeJoinOperator3 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(9),
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
              fragmentInstanceContext.getOperatorContexts().get(10),
              DEVICE0,
              seriesScanOperator1,
              Collections.singletonList(1),
              tsDataTypes);
      SingleDeviceViewOperator singleDeviceViewOperator2 =
          new SingleDeviceViewOperator(
              fragmentInstanceContext.getOperatorContexts().get(11),
              DEVICE1,
              timeJoinOperator1,
              Arrays.asList(1, 2),
              tsDataTypes);
      SingleDeviceViewOperator singleDeviceViewOperator3 =
          new SingleDeviceViewOperator(
              fragmentInstanceContext.getOperatorContexts().get(12),
              DEVICE2,
              timeJoinOperator2,
              Arrays.asList(1, 2),
              tsDataTypes);
      SingleDeviceViewOperator singleDeviceViewOperator4 =
          new SingleDeviceViewOperator(
              fragmentInstanceContext.getOperatorContexts().get(13),
              DEVICE3,
              timeJoinOperator3,
              Arrays.asList(1, 2),
              tsDataTypes);

      MergeSortOperator mergeSortOperator1 =
          new MergeSortOperator(
              fragmentInstanceContext.getOperatorContexts().get(14),
              Arrays.asList(singleDeviceViewOperator1, singleDeviceViewOperator2),
              tsDataTypes,
              new TimeMergeToolKit(
                  new ArrayList<>(
                      Arrays.asList(
                          new SortItem(SortKey.TIME, timeOrdering),
                          new SortItem(SortKey.DEVICE, deviceOrdering))),
                  2));
      MergeSortOperator mergeSortOperator2 =
          new MergeSortOperator(
              fragmentInstanceContext.getOperatorContexts().get(15),
              Arrays.asList(singleDeviceViewOperator3, singleDeviceViewOperator4),
              tsDataTypes,
              new TimeMergeToolKit(
                  new ArrayList<>(
                      Arrays.asList(
                          new SortItem(SortKey.TIME, timeOrdering),
                          new SortItem(SortKey.DEVICE, deviceOrdering))),
                  2));

      return new MergeSortOperator(
          fragmentInstanceContext.getOperatorContexts().get(16),
          Arrays.asList(mergeSortOperator1, mergeSortOperator2),
          tsDataTypes,
          new TimeMergeToolKit(
              new ArrayList<>(
                  Arrays.asList(
                      new SortItem(SortKey.TIME, timeOrdering),
                      new SortItem(SortKey.DEVICE, deviceOrdering))),
              2));
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
  //                [SSO]       TimeJoinOperator TimeJoinOperator    TimeJoinOperator
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      fragmentInstanceContext.addOperatorContext(
          2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      fragmentInstanceContext.addOperatorContext(
          3, planNodeId3, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      fragmentInstanceContext.addOperatorContext(
          4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      fragmentInstanceContext.addOperatorContext(
          5, planNodeId5, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId6 = new PlanNodeId("6");
      fragmentInstanceContext.addOperatorContext(
          6, planNodeId6, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId7 = new PlanNodeId("7");
      fragmentInstanceContext.addOperatorContext(
          7, planNodeId7, SeriesScanOperator.class.getSimpleName());

      fragmentInstanceContext.addOperatorContext(
          8, new PlanNodeId("8"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          9, new PlanNodeId("9"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          10, new PlanNodeId("10"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          11, new PlanNodeId("11"), DeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          12, new PlanNodeId("12"), DeviceViewOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
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
              planNodeId1,
              measurementPath1,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              planNodeId2,
              measurementPath2,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              planNodeId3,
              measurementPath3,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(2),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              planNodeId4,
              measurementPath4,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(3),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              planNodeId5,
              measurementPath5,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(4),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator6 =
          new SeriesScanOperator(
              planNodeId6,
              measurementPath6,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(5),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator6.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      SeriesScanOperator seriesScanOperator7 =
          new SeriesScanOperator(
              planNodeId7,
              measurementPath7,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(6),
              null,
              null,
              timeOrdering == Ordering.ASC);
      seriesScanOperator7.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

      List<TSDataType> tsDataTypes =
          new LinkedList<>(Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32));

      TimeJoinOperator timeJoinOperator1 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(7),
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

      TimeJoinOperator timeJoinOperator2 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(8),
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

      TimeJoinOperator timeJoinOperator3 =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(9),
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
              fragmentInstanceContext.getOperatorContexts().get(10),
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
              fragmentInstanceContext.getOperatorContexts().get(11),
              deviceOrdering == Ordering.ASC
                  ? Arrays.asList(DEVICE2, DEVICE3)
                  : Arrays.asList(DEVICE3, DEVICE2),
              deviceOrdering == Ordering.ASC
                  ? Arrays.asList(timeJoinOperator2, timeJoinOperator3)
                  : Arrays.asList(timeJoinOperator3, timeJoinOperator2),
              deviceColumnIndex,
              tsDataTypes);
      return new MergeSortOperator(
          fragmentInstanceContext.getOperatorContexts().get(12),
          Arrays.asList(deviceViewOperator1, deviceViewOperator2),
          tsDataTypes,
          new DeviceMergeToolKit(
              new ArrayList<>(
                  Arrays.asList(
                      new SortItem(SortKey.DEVICE, deviceOrdering),
                      new SortItem(SortKey.TIME, timeOrdering))),
              2));
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
    while (mergeSortOperator.hasNext()) {
      TsBlock tsBlock = mergeSortOperator.next();
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
}
