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
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ShowQueriesOperator;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class MergeTreeSortOperatorTest {

  private static final String MERGE_SORT_OPERATOR_TEST_SG = "root.MergeTreeSortOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private static final String DEVICE0 = MERGE_SORT_OPERATOR_TEST_SG + ".device0";
  private static final IDeviceID DEVICE0_ID = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE0);
  private static final String DEVICE1 = MERGE_SORT_OPERATOR_TEST_SG + ".device1";
  private static final IDeviceID DEVICE1_ID = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE1);
  private static final String DEVICE2 = MERGE_SORT_OPERATOR_TEST_SG + ".device2";
  private static final IDeviceID DEVICE2_ID = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE2);
  private static final String DEVICE3 = MERGE_SORT_OPERATOR_TEST_SG + ".device3";
  private static final IDeviceID DEVICE3_ID = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE3);

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
  //                                    TreeMergeSortOperator
  //                              ____________|_______________
  //                              /           |               \
  //           SingleDeviceViewOperator SingleDeviceViewOperator SingleDeviceViewOperator
  //                     /                     |                              \
  //        SeriesScanOperator      TimeJoinOperator                TimeJoinOperator
  //                                  /                \              /               \
  //                  SeriesScanOperator SeriesScanOperator SeriesScanOperator   SeriesScanOperator
  // ------------------------------------------------------------------------------------------------
  public TreeMergeSortOperator mergeSortOperatorTest(
      Ordering timeOrdering, Ordering deviceOrdering) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
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
        7, new PlanNodeId("7"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        8, new PlanNodeId("8"), SingleDeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        9, new PlanNodeId("9"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        10, new PlanNodeId("10"), SingleDeviceViewOperator.class.getSimpleName());

    driverContext.addOperatorContext(
        11, new PlanNodeId("11"), TreeMergeSortOperator.class.getSimpleName());

    NonAlignedFullPath measurementPath1 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device0"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath2 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath3 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor1", TSDataType.INT32));
    NonAlignedFullPath measurementPath4 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device2"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath5 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device2"),
            new MeasurementSchema("sensor1", TSDataType.INT32));

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

    FullOuterTimeJoinOperator timeJoinOperator1 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator1.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
    SingleDeviceViewOperator singleDeviceViewOperator2 =
        new SingleDeviceViewOperator(
            driverContext.getOperatorContexts().get(7),
            DEVICE1,
            timeJoinOperator1,
            Arrays.asList(2, 3),
            tsDataTypes);

    FullOuterTimeJoinOperator timeJoinOperator2 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator2.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    SingleDeviceViewOperator singleDeviceViewOperator3 =
        new SingleDeviceViewOperator(
            driverContext.getOperatorContexts().get(9),
            DEVICE2,
            timeJoinOperator2,
            Arrays.asList(4, 5),
            tsDataTypes);

    TreeMergeSortOperator treeMergeSortOperator =
        new TreeMergeSortOperator(
            driverContext.getOperatorContexts().get(10),
            Arrays.asList(
                singleDeviceViewOperator1, singleDeviceViewOperator2, singleDeviceViewOperator3),
            tsDataTypes,
            MergeSortComparator.getComparator(
                Arrays.asList(
                    new SortItem(OrderByKey.TIME, timeOrdering),
                    new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                Arrays.asList(-1, 0),
                Arrays.asList(TSDataType.INT64, TSDataType.TEXT)));
    treeMergeSortOperator
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
    return treeMergeSortOperator;
  }

  @Test
  public void testOrderByTime1() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator = mergeSortOperatorTest(Ordering.ASC, Ordering.ASC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(tsBlock.getColumn(4).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(5).getInt(i), getValue(lastTime));
          assertEquals(2, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(1500, count);
  }

  @Test
  public void testOrderByTime2() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest(Ordering.ASC, Ordering.DESC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          // make sure the device column is by desc
          assertEquals(2, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(1500, count);
  }

  @Test
  public void testOrderByTime3() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest(Ordering.DESC, Ordering.DESC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          // make sure the device column is by desc
          assertEquals(2, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(1500, count);
  }

  @Test
  public void testOrderByTime4() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest(Ordering.DESC, Ordering.ASC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(4).isNull(i));
          assertTrue(tsBlock.getColumn(5).isNull(i));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(3).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertTrue(tsBlock.getColumn(1).isNull(i));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          assertTrue(tsBlock.getColumn(3).isNull(i));
          assertEquals(tsBlock.getColumn(4).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(5).getInt(i), getValue(lastTime));
          assertEquals(2, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(1500, count);
  }

  // ------------------------------------------------------------------------------------------------
  //                                        order by time - 2
  // ------------------------------------------------------------------------------------------------
  // [SSO]:SeriesScanOperator
  // [SDO]:SingleDeviceOperator
  //                                       TreeMergeSortOperator
  //                                    ___________|___________
  //                                  /                         \
  //                   TreeMergeSortOperator                        TreeMergeSortOperator
  //                   /               |                       /               \
  //               [SDO]             [SDO]                   [SDO]            [SDO]
  //                 |                 |                       |                |
  //               [SSO]       RowBasedTimeJoinOperator        RowBasedTimeJoinOperator
  // RowBasedTimeJoinOperator
  //                              /         \              /         \       /         \
  //                            [SSO]      [SSO]         [SSO]      [SSO] [SSO]        [SSO]
  // ------------------------------------------------------------------------------------------------
  public TreeMergeSortOperator mergeSortOperatorTest2(
      Ordering timeOrdering, Ordering deviceOrdering) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
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
        8, new PlanNodeId("8"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        9, new PlanNodeId("9"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        10, new PlanNodeId("10"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        11, new PlanNodeId("11"), SingleDeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        12, new PlanNodeId("12"), SingleDeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        13, new PlanNodeId("13"), SingleDeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        14, new PlanNodeId("14"), SingleDeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        15, new PlanNodeId("15"), TreeMergeSortOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        16, new PlanNodeId("16"), TreeMergeSortOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        17, new PlanNodeId("17"), TreeMergeSortOperator.class.getSimpleName());

    NonAlignedFullPath measurementPath1 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device0"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath2 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath3 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor1", TSDataType.INT32));
    NonAlignedFullPath measurementPath4 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device2"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath5 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device2"),
            new MeasurementSchema("sensor1", TSDataType.INT32));
    NonAlignedFullPath measurementPath6 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device3"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath7 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device3"),
            new MeasurementSchema("sensor1", TSDataType.INT32));

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

    FullOuterTimeJoinOperator timeJoinOperator1 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator1.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    FullOuterTimeJoinOperator timeJoinOperator2 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator2.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    FullOuterTimeJoinOperator timeJoinOperator3 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator3.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

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

    TreeMergeSortOperator treeMergeSortOperator1 =
        new TreeMergeSortOperator(
            driverContext.getOperatorContexts().get(14),
            Arrays.asList(singleDeviceViewOperator1, singleDeviceViewOperator2),
            tsDataTypes,
            MergeSortComparator.getComparator(
                Arrays.asList(
                    new SortItem(OrderByKey.TIME, timeOrdering),
                    new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                Arrays.asList(-1, 0),
                Arrays.asList(TSDataType.INT64, TSDataType.TEXT)));
    treeMergeSortOperator1
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
    TreeMergeSortOperator treeMergeSortOperator2 =
        new TreeMergeSortOperator(
            driverContext.getOperatorContexts().get(15),
            Arrays.asList(singleDeviceViewOperator3, singleDeviceViewOperator4),
            tsDataTypes,
            MergeSortComparator.getComparator(
                Arrays.asList(
                    new SortItem(OrderByKey.TIME, timeOrdering),
                    new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                Arrays.asList(-1, 0),
                Arrays.asList(TSDataType.INT64, TSDataType.TEXT)));
    treeMergeSortOperator2
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    TreeMergeSortOperator treeMergeSortOperator =
        new TreeMergeSortOperator(
            driverContext.getOperatorContexts().get(16),
            Arrays.asList(treeMergeSortOperator1, treeMergeSortOperator2),
            tsDataTypes,
            MergeSortComparator.getComparator(
                Arrays.asList(
                    new SortItem(OrderByKey.TIME, timeOrdering),
                    new SortItem(OrderByKey.DEVICE, deviceOrdering)),
                Arrays.asList(-1, 0),
                Arrays.asList(TSDataType.INT64, TSDataType.TEXT)));
    treeMergeSortOperator
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
    return treeMergeSortOperator;
  }

  @Test
  public void testOrderByTime1_2() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest2(Ordering.ASC, Ordering.ASC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(2, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(3, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(2000, count);
  }

  @Test
  public void testOrderByTime2_2() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest2(Ordering.ASC, Ordering.DESC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;

    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) >= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(2, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by desc
          assertEquals(3, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(2000, count);
  }

  @Test
  public void testOrderByTime3_2() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest2(Ordering.DESC, Ordering.DESC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
      if (tsBlock == null) continue;
      assertEquals(3, tsBlock.getValueColumnCount());
      count += tsBlock.getPositionCount();
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertTrue(tsBlock.getTimeByIndex(i) <= lastTime);
        lastTime = tsBlock.getTimeByIndex(i);
        if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(2, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE0)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertTrue(tsBlock.getColumn(2).isNull(i));
          // make sure the device column is by desc
          assertEquals(3, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(2000, count);
  }

  @Test
  public void testOrderByTime4_2() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest2(Ordering.DESC, Ordering.ASC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
          assertEquals(0, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE1)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(1, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE2)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(2, checkDevice);
          checkDevice++;
        } else if (Objects.equals(tsBlock.getColumn(0).getBinary(i).toString(), DEVICE3)) {
          assertEquals(tsBlock.getColumn(1).getInt(i), getValue(lastTime));
          assertEquals(tsBlock.getColumn(2).getInt(i), getValue(lastTime));
          assertEquals(3, checkDevice);
          checkDevice = 0;
        } else {
          fail();
        }
      }
    }

    assertEquals(2000, count);
  }

  // ------------------------------------------------------------------------------------------------
  //                                   order by device
  // ------------------------------------------------------------------------------------------------
  // [SSO]:SeriesScanOperator
  //                                     TreeMergeSortOperator
  //                                  ___________|___________
  //                                 /                      \
  //                    DeviceViewOperator              DeviceViewOperator
  //                    /               |                /               \
  //                [SSO]       RowBasedTimeJoinOperator RowBasedTimeJoinOperator
  // RowBasedTimeJoinOperator
  //                               /         \      /         \         /         \
  //                             [SSO]      [SSO] [SSO]      [SSO]  [SSO]       [SSO]
  // ------------------------------------------------------------------------------------------------
  public TreeMergeSortOperator mergeSortOperatorTest3(
      Ordering timeOrdering, Ordering deviceOrdering) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
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
        8, new PlanNodeId("8"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        9, new PlanNodeId("9"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        10, new PlanNodeId("10"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        11, new PlanNodeId("11"), DeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        12, new PlanNodeId("12"), DeviceViewOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        13, new PlanNodeId("13"), TreeMergeSortOperator.class.getSimpleName());

    NonAlignedFullPath measurementPath1 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device0"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath2 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath3 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor1", TSDataType.INT32));
    NonAlignedFullPath measurementPath4 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device2"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath5 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device2"),
            new MeasurementSchema("sensor1", TSDataType.INT32));
    NonAlignedFullPath measurementPath6 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device3"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath7 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_SORT_OPERATOR_TEST_SG + ".device3"),
            new MeasurementSchema("sensor1", TSDataType.INT32));

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

    FullOuterTimeJoinOperator timeJoinOperator1 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator1.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    FullOuterTimeJoinOperator timeJoinOperator2 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator2.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    FullOuterTimeJoinOperator timeJoinOperator3 =
        new FullOuterTimeJoinOperator(
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
    timeJoinOperator3.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

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
                ? Arrays.asList(DEVICE0_ID, DEVICE1_ID)
                : Arrays.asList(DEVICE1_ID, DEVICE0_ID),
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
                ? Arrays.asList(DEVICE2_ID, DEVICE3_ID)
                : Arrays.asList(DEVICE3_ID, DEVICE2_ID),
            deviceOrdering == Ordering.ASC
                ? Arrays.asList(timeJoinOperator2, timeJoinOperator3)
                : Arrays.asList(timeJoinOperator3, timeJoinOperator2),
            deviceColumnIndex,
            tsDataTypes);
    TreeMergeSortOperator treeMergeSortOperator =
        new TreeMergeSortOperator(
            driverContext.getOperatorContexts().get(12),
            Arrays.asList(deviceViewOperator1, deviceViewOperator2),
            tsDataTypes,
            MergeSortComparator.getComparator(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, deviceOrdering),
                    new SortItem(OrderByKey.TIME, timeOrdering)),
                Arrays.asList(0, -1),
                Arrays.asList(TSDataType.TEXT, TSDataType.INT64)));
    treeMergeSortOperator
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
    return treeMergeSortOperator;
  }

  @Test
  public void testOrderByDevice1() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest3(Ordering.ASC, Ordering.ASC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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

    assertEquals(2000, count);
  }

  @Test
  public void testOrderByDevice2() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest3(Ordering.ASC, Ordering.DESC);
    long lastTime = -1;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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

    assertEquals(2000, count);
  }

  @Test
  public void testOrderByDevice3() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest3(Ordering.DESC, Ordering.ASC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;

    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
    assertEquals(2000, count);
  }

  @Test
  public void testOrderByDevice4() throws Exception {
    TreeMergeSortOperator treeMergeSortOperator =
        mergeSortOperatorTest3(Ordering.DESC, Ordering.DESC);
    long lastTime = Long.MAX_VALUE;
    int checkDevice = 0;
    int count = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock tsBlock = treeMergeSortOperator.next();
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
    assertEquals(2000, count);
  }

  // ------------------------------------------------------------------------------------------------
  //                                   order by Time, DataNodeID
  // ------------------------------------------------------------------------------------------------
  //
  //                                     TreeMergeSortOperator
  //                                  ___________|__________
  //                                 /                      \
  //                           TreeSortOperator             TreeSortOperator
  //                                |                        |
  //                        ShowQueriesOperator      ShowQueriesOperator
  // ------------------------------------------------------------------------------------------------
  @Test
  public void mergeSortWithSortOperatorTest() throws Exception {
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
      driverContext.addOperatorContext(2, planNodeId2, TreeSortOperator.class.getSimpleName());
      PlanNodeId planNodeId3 = new PlanNodeId("3");
      driverContext.addOperatorContext(3, planNodeId3, TreeSortOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, TreeMergeSortOperator.class.getSimpleName());

      List<OperatorContext> operatorContexts = driverContext.getOperatorContexts();
      List<TSDataType> dataTypes = DatasetHeaderFactory.getShowQueriesHeader().getRespDataTypes();
      Comparator<SortKey> comparator =
          MergeSortComparator.getComparator(
              Arrays.asList(
                  new SortItem(OrderByKey.TIME, Ordering.ASC),
                  new SortItem(OrderByKey.DATANODEID, Ordering.DESC)),
              ImmutableList.of(-1, 1),
              ImmutableList.of(TSDataType.INT64, TSDataType.INT32));

      Coordinator coordinator1 = Mockito.mock(Coordinator.class);
      when(coordinator1.getAllQueryExecutions())
          .thenReturn(
              ImmutableList.of(
                  new FakeQueryExecution(3, "20221229_000000_00003_1", "sql3_node1"),
                  new FakeQueryExecution(1, "20221229_000000_00001_1", "sql1_node1"),
                  new FakeQueryExecution(2, "20221229_000000_00002_1", "sql2_node1")));
      Coordinator coordinator2 = Mockito.mock(Coordinator.class);
      when(coordinator2.getAllQueryExecutions())
          .thenReturn(
              ImmutableList.of(
                  new FakeQueryExecution(3, "20221229_000000_00003_2", "sql3_node2"),
                  new FakeQueryExecution(2, "20221229_000000_00002_2", "sql2_node2"),
                  new FakeQueryExecution(1, "20221229_000000_00001_2", "sql1_node2")));

      ShowQueriesOperator showQueriesOperator1 =
          new ShowQueriesOperator(operatorContexts.get(0), planNodeId0, coordinator1);
      ShowQueriesOperator showQueriesOperator2 =
          new ShowQueriesOperator(operatorContexts.get(1), planNodeId1, coordinator2);
      TreeSortOperator treeSortOperator1 =
          new TreeSortOperator(
              operatorContexts.get(2), showQueriesOperator1, dataTypes, "", comparator);
      TreeSortOperator treeSortOperator2 =
          new TreeSortOperator(
              operatorContexts.get(3), showQueriesOperator2, dataTypes, "", comparator);
      Operator root =
          new TreeMergeSortOperator(
              operatorContexts.get(4),
              new ArrayList<Operator>() {
                {
                  add(treeSortOperator1);
                  add(treeSortOperator2);
                }
              },
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
  public void mergeSortTest() throws Exception {
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
    driverContext.addOperatorContext(1, planNodeId1, TreeMergeSortOperator.class.getSimpleName());

    TreeMergeSortOperator treeMergeSortOperator =
        new TreeMergeSortOperator(
            driverContext.getOperatorContexts().get(0),
            Arrays.asList(childOperator1, childOperator2),
            Collections.singletonList(TSDataType.INT64),
            MergeSortComparator.getComparator(
                Collections.singletonList(new SortItem(OrderByKey.TIME, Ordering.ASC)),
                Collections.singletonList(-1),
                Collections.singletonList(TSDataType.INT64)));
    treeMergeSortOperator
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    int index = 0;
    while (treeMergeSortOperator.isBlocked().isDone() && treeMergeSortOperator.hasNext()) {
      TsBlock result = treeMergeSortOperator.next();
      for (int i = 0; i < result.getPositionCount(); i++) {
        long time = result.getTimeByIndex(i);
        assertEquals(time, ans[index++]);
      }
    }
    assertEquals(index, ans.length);
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
    public String getStatementType() {
      return null;
    }

    @Override
    public String getSQLDialect() {
      return IClientSession.SqlDialect.TREE.toString();
    }

    @Override
    public void start() {}

    @Override
    public void stop(Throwable t) {}

    @Override
    public void stopAndCleanup() {}

    @Override
    public void stopAndCleanup(Throwable t) {}

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
