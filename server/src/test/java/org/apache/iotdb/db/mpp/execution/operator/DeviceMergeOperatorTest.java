/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeviceMergeOperatorTest {

  private static final String DEVICE_MERGE_OPERATOR_TEST_SG = "root.DeviceMergeOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, DEVICE_MERGE_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  /**
   * Construct DeviceMergeOperator with different devices in two DeviceViewOperators.
   *
   * <p>DeviceViewOperator1: [seriesScanOperator: [device0.sensor0]],
   *
   * <p>DeviceViewOperator2: [seriesScanOperator: [device1.sensor1]]
   *
   * <p>the result tsBlock should be like [Device, sensor0, sensor1]. The sensor1 column of device0
   * and the sensor0 column of device1 should be null.
   */
  @Test
  public void deviceMergeOperatorTest() {
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
      driverContext.addOperatorContext(
          3, planNodeId3, DeviceViewOperatorTest.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(
          4, planNodeId4, DeviceViewOperatorTest.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, DeviceMergeOperator.class.getSimpleName());

      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.TEXT);
      dataTypes.add(TSDataType.INT32);
      dataTypes.add(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(Collections.singleton("sensor0"));
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

      DeviceViewOperator deviceViewOperator1 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(2),
              Collections.singletonList(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0"),
              Collections.singletonList(seriesScanOperator1),
              Collections.singletonList(Collections.singletonList(1)),
              dataTypes);

      MeasurementPath measurementPath2 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      scanOptionsBuilder.withAllSensors(Collections.singleton("sensor1"));
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

      DeviceViewOperator deviceViewOperator2 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(3),
              Collections.singletonList(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1"),
              Collections.singletonList(seriesScanOperator2),
              Collections.singletonList(Collections.singletonList(2)),
              dataTypes);

      List<String> devices = new ArrayList<>();
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0");
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1");
      List<Operator> deviceOperators = new ArrayList<>();
      deviceOperators.add(deviceViewOperator1);
      deviceOperators.add(deviceViewOperator2);
      DeviceMergeOperator deviceMergeOperator =
          new DeviceMergeOperator(
              driverContext.getOperatorContexts().get(4),
              devices,
              deviceOperators,
              dataTypes,
              new TimeSelector(500, true),
              new AscTimeComparator());

      int count = 0;
      while (deviceMergeOperator.hasNext()) {
        TsBlock tsBlock = deviceMergeOperator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(3, tsBlock.getValueColumnCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          long expectedTime = count % 500;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          assertEquals(
              count < 500
                  ? DEVICE_MERGE_OPERATOR_TEST_SG + ".device0"
                  : DEVICE_MERGE_OPERATOR_TEST_SG + ".device1",
              tsBlock.getColumn(0).getBinary(i).getStringValue());
          if (expectedTime < 200) {
            if (!tsBlock.getColumn(1).isNull(i)) {
              assertEquals(20000 + expectedTime, tsBlock.getColumn(1).getInt(i));
              assertTrue(tsBlock.getColumn(2).isNull(i));
            } else {
              assertEquals(20000 + expectedTime, tsBlock.getColumn(2).getInt(i));
            }
          } else if (expectedTime < 260
              || (expectedTime >= 300 && expectedTime < 380)
              || expectedTime >= 400) {
            if (!tsBlock.getColumn(1).isNull(i)) {
              assertEquals(10000 + expectedTime, tsBlock.getColumn(1).getInt(i));
              assertTrue(tsBlock.getColumn(2).isNull(i));
            } else {
              assertEquals(10000 + expectedTime, tsBlock.getColumn(2).getInt(i));
            }
          } else {
            if (!tsBlock.getColumn(1).isNull(i)) {
              assertEquals(expectedTime, tsBlock.getColumn(1).getInt(i));
              assertTrue(tsBlock.getColumn(2).isNull(i));
            } else {
              assertEquals(expectedTime, tsBlock.getColumn(2).getInt(i));
            }
          }
        }
      }
      assertEquals(1000, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Construct DeviceMergeOperator with the same device in two DeviceViewOperators.
   *
   * <p>DeviceViewOperator1: [seriesScanOperator: [device0.sensor0]],
   *
   * <p>DeviceViewOperator2: [seriesScanOperator: [device0.sensor0]]
   *
   * <p>the result tsBlock should be like [Device, sensor0, sensor1]. The sensor1 column of device0
   * and the sensor0 column of device1 should be null.
   */
  @Test
  public void deviceMergeOperatorTest2() {
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
      driverContext.addOperatorContext(
          3, planNodeId3, DeviceViewOperatorTest.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(
          4, planNodeId4, DeviceViewOperatorTest.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, DeviceMergeOperator.class.getSimpleName());

      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.TEXT);
      dataTypes.add(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(Collections.singleton("sensor0"));
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      List<TsFileResource> seqResources1 = new ArrayList<>();
      List<TsFileResource> unSeqResources1 = new ArrayList<>();
      seqResources1.add(seqResources.get(0));
      seqResources1.add(seqResources.get(1));
      seqResources1.add(seqResources.get(3));
      unSeqResources1.add(unSeqResources.get(0));
      unSeqResources1.add(unSeqResources.get(1));
      unSeqResources1.add(unSeqResources.get(3));
      unSeqResources1.add(unSeqResources.get(5));
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources1, unSeqResources1));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      DeviceViewOperator deviceViewOperator1 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(2),
              Collections.singletonList(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0"),
              Collections.singletonList(seriesScanOperator1),
              Collections.singletonList(Collections.singletonList(1)),
              dataTypes);

      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      List<TsFileResource> seqResources2 = new ArrayList<>();
      List<TsFileResource> unSeqResources2 = new ArrayList<>();
      seqResources2.add(seqResources.get(2));
      seqResources2.add(seqResources.get(4));
      unSeqResources2.add(unSeqResources.get(2));
      unSeqResources2.add(unSeqResources.get(4));
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources2, unSeqResources2));
      DeviceViewOperator deviceViewOperator2 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(3),
              Collections.singletonList(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0"),
              Collections.singletonList(seriesScanOperator2),
              Collections.singletonList(Collections.singletonList(1)),
              dataTypes);

      List<String> devices = new ArrayList<>();
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0");
      List<Operator> deviceOperators = new ArrayList<>();
      deviceOperators.add(deviceViewOperator1);
      deviceOperators.add(deviceViewOperator2);
      DeviceMergeOperator deviceMergeOperator =
          new DeviceMergeOperator(
              driverContext.getOperatorContexts().get(4),
              devices,
              deviceOperators,
              dataTypes,
              new TimeSelector(500, true),
              new AscTimeComparator());

      int count = 0;
      while (deviceMergeOperator.hasNext()) {
        TsBlock tsBlock = deviceMergeOperator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(2, tsBlock.getValueColumnCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          assertEquals(
              DEVICE_MERGE_OPERATOR_TEST_SG + ".device0",
              tsBlock.getColumn(0).getBinary(i).getStringValue());
          if ((long) count < 200) {
            assertEquals(20000 + (long) count, tsBlock.getColumn(1).getInt(i));
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            assertEquals(10000 + (long) count, tsBlock.getColumn(1).getInt(i));
          } else {
            assertEquals(count, tsBlock.getColumn(1).getInt(i));
          }
        }
      }
      assertEquals(500, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Construct DeviceMergeOperator with the same and different device at the same time in two
   * DeviceViewOperators.
   *
   * <p>DeviceViewOperator1: [seriesScanOperator: [device0.sensor0], [device1.sensor1]],
   *
   * <p>DeviceViewOperator2: [seriesScanOperator: [device0.sensor0]]
   *
   * <p>the result tsBlock should be like [Device, sensor0, sensor1]. The sensor1 column of device0
   * and the sensor0 column of device1 should be null.
   */
  @Test
  public void deviceMergeOperatorTest3() {
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
      driverContext.addOperatorContext(
          4, new PlanNodeId("4"), DeviceViewOperatorTest.class.getSimpleName());
      driverContext.addOperatorContext(
          5, new PlanNodeId("5"), DeviceViewOperatorTest.class.getSimpleName());
      driverContext.addOperatorContext(
          6, new PlanNodeId("6"), DeviceMergeOperator.class.getSimpleName());

      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.TEXT);
      dataTypes.add(TSDataType.INT32);
      dataTypes.add(TSDataType.INT32);
      MeasurementPath measurementPath1 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(Collections.singleton("sensor0"));
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      List<TsFileResource> seqResources1 = new ArrayList<>();
      List<TsFileResource> unSeqResources1 = new ArrayList<>();
      seqResources1.add(seqResources.get(0));
      seqResources1.add(seqResources.get(1));
      seqResources1.add(seqResources.get(3));
      unSeqResources1.add(unSeqResources.get(0));
      unSeqResources1.add(unSeqResources.get(1));
      unSeqResources1.add(unSeqResources.get(3));
      unSeqResources1.add(unSeqResources.get(5));
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources1, unSeqResources1));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      MeasurementPath measurementPath2 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
      scanOptionsBuilder.withAllSensors(Collections.singleton("sensor1"));
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

      List<String> devices = new ArrayList<>();
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0");
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1");
      List<Operator> deviceOperators = new ArrayList<>();
      deviceOperators.add(seriesScanOperator1);
      deviceOperators.add(seriesScanOperator2);
      List<List<Integer>> deviceColumnIndex = new ArrayList<>();
      deviceColumnIndex.add(Collections.singletonList(1));
      deviceColumnIndex.add(Collections.singletonList(2));
      DeviceViewOperator deviceViewOperator1 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(3),
              devices,
              deviceOperators,
              deviceColumnIndex,
              dataTypes);

      scanOptionsBuilder.withAllSensors(Collections.singleton("sensor0"));
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(2),
              planNodeId3,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator3
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      List<TsFileResource> seqResources2 = new ArrayList<>();
      List<TsFileResource> unSeqResources2 = new ArrayList<>();
      seqResources2.add(seqResources.get(2));
      seqResources2.add(seqResources.get(4));
      unSeqResources2.add(unSeqResources.get(2));
      unSeqResources2.add(unSeqResources.get(4));
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources2, unSeqResources2));
      DeviceViewOperator deviceViewOperator2 =
          new DeviceViewOperator(
              driverContext.getOperatorContexts().get(4),
              Collections.singletonList(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0"),
              Collections.singletonList(seriesScanOperator3),
              Collections.singletonList(Collections.singletonList(1)),
              dataTypes);

      List<Operator> deviceViewOperators = new ArrayList<>();
      deviceViewOperators.add(deviceViewOperator1);
      deviceViewOperators.add(deviceViewOperator2);
      DeviceMergeOperator deviceMergeOperator =
          new DeviceMergeOperator(
              driverContext.getOperatorContexts().get(5),
              devices,
              deviceViewOperators,
              dataTypes,
              new TimeSelector(500, true),
              new AscTimeComparator());

      int count = 0;
      while (deviceMergeOperator.hasNext()) {
        TsBlock tsBlock = deviceMergeOperator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(3, tsBlock.getValueColumnCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          long expectedTime = count % 500;
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          assertEquals(
              count < 500
                  ? DEVICE_MERGE_OPERATOR_TEST_SG + ".device0"
                  : DEVICE_MERGE_OPERATOR_TEST_SG + ".device1",
              tsBlock.getColumn(0).getBinary(i).getStringValue());
          if (expectedTime < 200) {
            if (!tsBlock.getColumn(1).isNull(i)) {
              assertEquals(20000 + expectedTime, tsBlock.getColumn(1).getInt(i));
              assertTrue(tsBlock.getColumn(2).isNull(i));
            } else {
              assertEquals(20000 + expectedTime, tsBlock.getColumn(2).getInt(i));
            }
          } else if (expectedTime < 260
              || (expectedTime >= 300 && expectedTime < 380)
              || expectedTime >= 400) {
            if (!tsBlock.getColumn(1).isNull(i)) {
              assertEquals(10000 + expectedTime, tsBlock.getColumn(1).getInt(i));
              assertTrue(tsBlock.getColumn(2).isNull(i));
            } else {
              assertEquals(10000 + expectedTime, tsBlock.getColumn(2).getInt(i));
            }
          } else {
            if (!tsBlock.getColumn(1).isNull(i)) {
              assertEquals(expectedTime, tsBlock.getColumn(1).getInt(i));
              assertTrue(tsBlock.getColumn(2).isNull(i));
            } else {
              assertEquals(expectedTime, tsBlock.getColumn(2).getInt(i));
            }
          }
        }
      }
      assertEquals(1000, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
