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
import org.apache.iotdb.db.mpp.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SingleDeviceViewOperatorTest {
  private static final String SINGLE_DEVICE_MERGE_OPERATOR_TEST_SG =
      "root.SingleDeviceViewOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas,
        deviceIds,
        seqResources,
        unSeqResources,
        SINGLE_DEVICE_MERGE_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  /**
   * Construct seriesScanOperator:[device0.sensor0, device1.sensor1], the result tsBlock should be
   * like [Device, sensor0, sensor1]. The sensor1 column of device0 and the sensor0 column of
   * device1 should be null.
   */
  @Test
  public void singleDeviceViewOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      // construct operator tree
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
      fragmentInstanceContext.addOperatorContext(
          3, new PlanNodeId("3"), TimeJoinOperatorTest.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          4, new PlanNodeId("4"), SingleDeviceViewOperator.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(
              SINGLE_DEVICE_MERGE_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      MeasurementPath measurementPath2 =
          new MeasurementPath(
              SINGLE_DEVICE_MERGE_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              planNodeId1,
              measurementPath1,
              Collections.singleton("sensor0"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              true);
      seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              planNodeId2,
              measurementPath2,
              Collections.singleton("sensor1"),
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              true);
      seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator2
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      TimeJoinOperator timeJoinOperator =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              Ordering.ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32),
              Arrays.asList(
                  new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                  new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
              new AscTimeComparator());
      SingleDeviceViewOperator singleDeviceViewOperator =
          new SingleDeviceViewOperator(
              fragmentInstanceContext.getOperatorContexts().get(3),
              SINGLE_DEVICE_MERGE_OPERATOR_TEST_SG + ".device0",
              timeJoinOperator,
              Arrays.asList(1, 2),
              Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
      int count = 0;
      int total = 0;
      while (singleDeviceViewOperator.hasNext()) {
        TsBlock tsBlock = singleDeviceViewOperator.next();
        assertEquals(4, tsBlock.getValueColumnCount());
        total += tsBlock.getPositionCount();
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = i + 20L * (count % 25);
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          assertEquals(
              SINGLE_DEVICE_MERGE_OPERATOR_TEST_SG + ".device0",
              tsBlock.getColumn(0).getBinary(i).toString());
          if (expectedTime < 200) {
            assertEquals(20000 + expectedTime, tsBlock.getColumn(1).getInt(i));
            assertEquals(20000 + expectedTime, tsBlock.getColumn(2).getInt(i));
            assertTrue(tsBlock.getColumn(3).isNull(i));
          } else if (expectedTime < 260
              || (expectedTime >= 300 && expectedTime < 380)
              || expectedTime >= 400) {
            assertEquals(10000 + expectedTime, tsBlock.getColumn(1).getInt(i));
            assertEquals(10000 + expectedTime, tsBlock.getColumn(2).getInt(i));
            assertTrue(tsBlock.getColumn(3).isNull(i));
          } else {
            assertEquals(expectedTime, tsBlock.getColumn(1).getInt(i));
            assertEquals(expectedTime, tsBlock.getColumn(2).getInt(i));
            assertTrue(tsBlock.getColumn(3).isNull(i));
          }
        }
        count++;
      }
      assertEquals(500, total);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
