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
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.join.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import io.airlift.units.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AlignedSeriesScanOperatorTest {

  private static final String SERIES_SCAN_OPERATOR_TEST_SG = "root.AlignedSeriesScanOperatorTest";
  private static final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private static final List<TsFileResource> seqResources = new ArrayList<>();
  private static final List<TsFileResource> unSeqResources = new ArrayList<>();

  private static final double DELTA = 0.000001;

  @BeforeClass
  public static void setUp() throws MetadataException, IOException, WriteProcessException {
    AlignedSeriesTestUtil.setUp(
        measurementSchemas, seqResources, unSeqResources, SERIES_SCAN_OPERATOR_TEST_SG);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    AlignedSeriesTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest1() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      AlignedPath alignedPath =
          new AlignedPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device0",
              measurementSchemas.stream()
                  .map(MeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList()),
              measurementSchemas.stream()
                  .map(m -> (IMeasurementSchema) m)
                  .collect(Collectors.toList()));
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId, AlignedSeriesScanOperator.class.getSimpleName());

      AlignedSeriesScanOperator seriesScanOperator =
          new AlignedSeriesScanOperator(
              planNodeId,
              alignedPath,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              true);
      seriesScanOperator.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      int count = 0;
      while (seriesScanOperator.hasNext()) {
        TsBlock tsBlock = seriesScanOperator.next();
        assertEquals(6, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(3) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(4) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(5) instanceof BinaryColumn);

        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          int delta = 0;
          if ((long) count < 200) {
            delta = 20000;
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            delta = 10000;
          }
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(0).getBoolean(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(1).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(2).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(3).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(4).getDouble(i), DELTA);
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(5).getBinary(i).toString());
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

  @Test
  public void batchTest2() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      AlignedPath alignedPath1 =
          new AlignedPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device0",
              measurementSchemas.stream()
                  .map(MeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList()),
              measurementSchemas.stream()
                  .map(m -> (IMeasurementSchema) m)
                  .collect(Collectors.toList()));
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, AlignedSeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      fragmentInstanceContext.addOperatorContext(
          2, planNodeId2, AlignedSeriesScanOperator.class.getSimpleName());
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
      PlanNodeId planNodeId8 = new PlanNodeId("8");
      fragmentInstanceContext.addOperatorContext(
          8, planNodeId8, SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          9, new PlanNodeId("9"), TimeJoinOperator.class.getSimpleName());
      AlignedSeriesScanOperator seriesScanOperator1 =
          new AlignedSeriesScanOperator(
              planNodeId1,
              alignedPath1,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              true);
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      AlignedPath alignedPath2 =
          new AlignedPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device1",
              measurementSchemas.stream()
                  .map(MeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList()),
              measurementSchemas.stream()
                  .map(m -> (IMeasurementSchema) m)
                  .collect(Collectors.toList()));
      AlignedSeriesScanOperator seriesScanOperator2 =
          new AlignedSeriesScanOperator(
              planNodeId2,
              alignedPath2,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              true);
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      allSensors.add("sensor1");
      allSensors.add("sensor2");
      allSensors.add("sensor3");
      allSensors.add("sensor4");
      allSensors.add("sensor5");

      MeasurementPath measurementPath3 =
          new MeasurementPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.BOOLEAN);
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath3,
              allSensors,
              TSDataType.BOOLEAN,
              null,
              null,
              true);
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator3
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath4 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath4,
              allSensors,
              TSDataType.INT32,
              null,
              null,
              true);
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator4
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath5 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor2", TSDataType.INT64);
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath5,
              allSensors,
              TSDataType.INT64,
              null,
              null,
              true);
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator5
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath6 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor3", TSDataType.FLOAT);
      SeriesScanOperator seriesScanOperator6 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(5),
              planNodeId6,
              measurementPath6,
              allSensors,
              TSDataType.FLOAT,
              null,
              null,
              true);
      seriesScanOperator6.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator6
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath7 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor4", TSDataType.DOUBLE);
      SeriesScanOperator seriesScanOperator7 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(6),
              planNodeId7,
              measurementPath7,
              allSensors,
              TSDataType.DOUBLE,
              null,
              null,
              true);
      seriesScanOperator7.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator7
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath8 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor5", TSDataType.DOUBLE);
      SeriesScanOperator seriesScanOperator8 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(7),
              planNodeId8,
              measurementPath8,
              allSensors,
              TSDataType.TEXT,
              null,
              null,
              true);
      seriesScanOperator8.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator8
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      TimeJoinOperator timeJoinOperator =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(8),
              Arrays.asList(
                  seriesScanOperator1,
                  seriesScanOperator2,
                  seriesScanOperator3,
                  seriesScanOperator4,
                  seriesScanOperator5,
                  seriesScanOperator6,
                  seriesScanOperator7,
                  seriesScanOperator8),
              Ordering.ASC,
              Arrays.asList(
                  TSDataType.BOOLEAN,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT,
                  TSDataType.BOOLEAN,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT,
                  TSDataType.BOOLEAN,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 1), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 2), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 3), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 4), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 5), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 1), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 2), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 3), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 4), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 5), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(2, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(3, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(4, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(5, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(6, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(7, 0), new AscTimeComparator())),
              new AscTimeComparator());
      int count = 0;
      while (timeJoinOperator.hasNext()) {
        TsBlock tsBlock = timeJoinOperator.next();
        assertEquals(18, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(3) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(4) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(5) instanceof BinaryColumn);
        assertTrue(tsBlock.getColumn(6) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(7) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(8) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(9) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(10) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(11) instanceof BinaryColumn);
        assertTrue(tsBlock.getColumn(12) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(13) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(14) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(15) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(16) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(17) instanceof BinaryColumn);

        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          int delta = 0;
          if ((long) count < 200) {
            delta = 20000;
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            delta = 10000;
          }
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(0).getBoolean(i));
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(6).getBoolean(i));
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(12).getBoolean(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(1).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(7).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(13).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(2).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(8).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(14).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(3).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(9).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(15).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(4).getDouble(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(10).getDouble(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(16).getDouble(i), DELTA);
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(5).getBinary(i).toString());
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(11).getBinary(i).toString());
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(17).getBinary(i).toString());
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

  /** order by time desc */
  @Test
  public void batchTest3() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      AlignedPath alignedPath1 =
          new AlignedPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device0",
              measurementSchemas.stream()
                  .map(MeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList()),
              measurementSchemas.stream()
                  .map(m -> (IMeasurementSchema) m)
                  .collect(Collectors.toList()));
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, AlignedSeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      fragmentInstanceContext.addOperatorContext(
          2, planNodeId2, AlignedSeriesScanOperator.class.getSimpleName());
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
      PlanNodeId planNodeId8 = new PlanNodeId("8");
      fragmentInstanceContext.addOperatorContext(
          8, planNodeId8, SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          9, new PlanNodeId("9"), TimeJoinOperator.class.getSimpleName());
      AlignedSeriesScanOperator seriesScanOperator1 =
          new AlignedSeriesScanOperator(
              planNodeId1,
              alignedPath1,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              false);
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      AlignedPath alignedPath2 =
          new AlignedPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device1",
              measurementSchemas.stream()
                  .map(MeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList()),
              measurementSchemas.stream()
                  .map(m -> (IMeasurementSchema) m)
                  .collect(Collectors.toList()));
      AlignedSeriesScanOperator seriesScanOperator2 =
          new AlignedSeriesScanOperator(
              planNodeId2,
              alignedPath2,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              false);
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      allSensors.add("sensor1");
      allSensors.add("sensor2");
      allSensors.add("sensor3");
      allSensors.add("sensor4");
      allSensors.add("sensor5");

      MeasurementPath measurementPath3 =
          new MeasurementPath(
              SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor0", TSDataType.BOOLEAN);
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath3,
              allSensors,
              TSDataType.BOOLEAN,
              null,
              null,
              false);
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator3
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath4 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath4,
              allSensors,
              TSDataType.INT32,
              null,
              null,
              false);
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator4
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath5 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor2", TSDataType.INT64);
      SeriesScanOperator seriesScanOperator5 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath5,
              allSensors,
              TSDataType.INT64,
              null,
              null,
              false);
      seriesScanOperator5.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator5
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath6 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor3", TSDataType.FLOAT);
      SeriesScanOperator seriesScanOperator6 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(5),
              planNodeId6,
              measurementPath6,
              allSensors,
              TSDataType.FLOAT,
              null,
              null,
              false);
      seriesScanOperator6.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator6
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath7 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor4", TSDataType.DOUBLE);
      SeriesScanOperator seriesScanOperator7 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(6),
              planNodeId7,
              measurementPath7,
              allSensors,
              TSDataType.DOUBLE,
              null,
              null,
              false);
      seriesScanOperator7.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator7
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath8 =
          new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device2.sensor5", TSDataType.DOUBLE);
      SeriesScanOperator seriesScanOperator8 =
          new SeriesScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(7),
              planNodeId8,
              measurementPath8,
              allSensors,
              TSDataType.TEXT,
              null,
              null,
              false);
      seriesScanOperator8.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator8
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      TimeJoinOperator timeJoinOperator =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(8),
              Arrays.asList(
                  seriesScanOperator1,
                  seriesScanOperator2,
                  seriesScanOperator3,
                  seriesScanOperator4,
                  seriesScanOperator5,
                  seriesScanOperator6,
                  seriesScanOperator7,
                  seriesScanOperator8),
              Ordering.DESC,
              Arrays.asList(
                  TSDataType.BOOLEAN,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT,
                  TSDataType.BOOLEAN,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT,
                  TSDataType.BOOLEAN,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 1), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 2), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 3), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 4), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(0, 5), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 1), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 2), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 3), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 4), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 5), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(2, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(3, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(4, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(5, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(6, 0), new DescTimeComparator()),
                  new SingleColumnMerger(new InputLocation(7, 0), new DescTimeComparator())),
              new DescTimeComparator());

      int count = 499;
      while (timeJoinOperator.hasNext()) {
        TsBlock tsBlock = timeJoinOperator.next();
        assertEquals(18, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(3) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(4) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(5) instanceof BinaryColumn);
        assertTrue(tsBlock.getColumn(6) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(7) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(8) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(9) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(10) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(11) instanceof BinaryColumn);
        assertTrue(tsBlock.getColumn(12) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(13) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(14) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(15) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(16) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(17) instanceof BinaryColumn);

        for (int i = 0; i < tsBlock.getPositionCount(); i++, count--) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          int delta = 0;
          if ((long) count < 200) {
            delta = 20000;
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            delta = 10000;
          }
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(0).getBoolean(i));
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(6).getBoolean(i));
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(12).getBoolean(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(1).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(7).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(13).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(2).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(8).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(14).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(3).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(9).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(15).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(4).getDouble(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(10).getDouble(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(16).getDouble(i), DELTA);
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(5).getBinary(i).toString());
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(11).getBinary(i).toString());
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(17).getBinary(i).toString());
        }
      }
      assertEquals(-1, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
