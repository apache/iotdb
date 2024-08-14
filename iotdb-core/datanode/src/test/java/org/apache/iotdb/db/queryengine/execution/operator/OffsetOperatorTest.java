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
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import io.airlift.units.Duration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OffsetOperatorTest {

  private static final String TIME_JOIN_OPERATOR_TEST_SG = "root.LimitOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, TIME_JOIN_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest1() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IFullPath measurementPath1 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor0", TSDataType.INT32));
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), FullOuterTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          4, new PlanNodeId("4"), OffsetOperator.class.getSimpleName());
      driverContext.addOperatorContext(5, new PlanNodeId("5"), LimitOperator.class.getSimpleName());

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      IFullPath measurementPath2 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor1", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      FullOuterTimeJoinOperator timeJoinOperator =
          new FullOuterTimeJoinOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              Ordering.ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
              new AscTimeComparator());
      timeJoinOperator.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      OffsetOperator offsetOperator =
          new OffsetOperator(driverContext.getOperatorContexts().get(3), 100, timeJoinOperator);

      LimitOperator limitOperator =
          new LimitOperator(driverContext.getOperatorContexts().get(4), 250, offsetOperator);
      int count = 100;
      while (limitOperator.isBlocked().isDone() && limitOperator.hasNext()) {
        TsBlock tsBlock = limitOperator.next();
        assertEquals(2, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);

        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          if ((long) count < 200) {
            assertEquals(20000 + (long) count, tsBlock.getColumn(0).getInt(i));
            assertEquals(20000 + (long) count, tsBlock.getColumn(1).getInt(i));
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            assertEquals(10000 + (long) count, tsBlock.getColumn(0).getInt(i));
            assertEquals(10000 + (long) count, tsBlock.getColumn(1).getInt(i));
          } else {
            assertEquals(count, tsBlock.getColumn(0).getInt(i));
            assertEquals(count, tsBlock.getColumn(1).getInt(i));
          }
        }
      }
      assertEquals(350, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  /** offset is 0 in which case we will get all data */
  @Test
  public void batchTest2() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IFullPath measurementPath1 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor0", TSDataType.INT32));
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), FullOuterTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          4, new PlanNodeId("4"), OffsetOperator.class.getSimpleName());
      driverContext.addOperatorContext(5, new PlanNodeId("5"), LimitOperator.class.getSimpleName());

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      IFullPath measurementPath2 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor1", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      FullOuterTimeJoinOperator timeJoinOperator =
          new FullOuterTimeJoinOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              Ordering.ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
              new AscTimeComparator());
      timeJoinOperator.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      OffsetOperator offsetOperator =
          new OffsetOperator(driverContext.getOperatorContexts().get(3), 0, timeJoinOperator);

      int count = 0;
      while (offsetOperator.isBlocked().isDone() && offsetOperator.hasNext()) {
        TsBlock tsBlock = offsetOperator.next();
        assertEquals(2, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          if ((long) count < 200) {
            assertEquals(20000 + (long) count, tsBlock.getColumn(0).getInt(i));
            assertEquals(20000 + (long) count, tsBlock.getColumn(1).getInt(i));
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            assertEquals(10000 + (long) count, tsBlock.getColumn(0).getInt(i));
            assertEquals(10000 + (long) count, tsBlock.getColumn(1).getInt(i));
          } else {
            assertEquals(count, tsBlock.getColumn(0).getInt(i));
            assertEquals(count, tsBlock.getColumn(1).getInt(i));
          }
        }
      }
      assertEquals(500, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  /** offset is larger than max row number in which case we will get no data */
  @Test
  public void batchTest3() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IFullPath measurementPath1 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor0", TSDataType.INT32));
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), FullOuterTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          4, new PlanNodeId("4"), OffsetOperator.class.getSimpleName());
      driverContext.addOperatorContext(5, new PlanNodeId("5"), LimitOperator.class.getSimpleName());

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      IFullPath measurementPath2 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor1", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      FullOuterTimeJoinOperator timeJoinOperator =
          new FullOuterTimeJoinOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              Ordering.ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
              new AscTimeComparator());
      timeJoinOperator.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      OffsetOperator offsetOperator =
          new OffsetOperator(driverContext.getOperatorContexts().get(3), 500, timeJoinOperator);

      while (offsetOperator.isBlocked().isDone() && offsetOperator.hasNext()) {
        TsBlock tsBlock = offsetOperator.next();
        assertEquals(2, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertEquals(0, tsBlock.getPositionCount());
      }
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  /** offset is larger than Integer.MAX_VALUE in which case we will get no data */
  @Test
  public void batchTest4() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IFullPath measurementPath1 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor0", TSDataType.INT32));
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
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), FullOuterTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          4, new PlanNodeId("4"), OffsetOperator.class.getSimpleName());
      driverContext.addOperatorContext(5, new PlanNodeId("5"), LimitOperator.class.getSimpleName());

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      IFullPath measurementPath2 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TIME_JOIN_OPERATOR_TEST_SG + ".device0"),
              new MeasurementSchema("sensor1", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      FullOuterTimeJoinOperator timeJoinOperator =
          new FullOuterTimeJoinOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              Ordering.ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
              new AscTimeComparator());
      timeJoinOperator.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      OffsetOperator offsetOperator =
          new OffsetOperator(
              driverContext.getOperatorContexts().get(3), 98_784_247_808L, timeJoinOperator);

      while (offsetOperator.isBlocked().isDone() && offsetOperator.hasNext()) {
        TsBlock tsBlock = offsetOperator.next();
        assertEquals(2, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertEquals(0, tsBlock.getPositionCount());
      }
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
