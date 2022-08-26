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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeviceViewOperatorTest {

  private static final String DEVICE_MERGE_OPERATOR_TEST_SG = "root.DeviceViewOperatorTest";
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
   * Construct seriesScanOperator:[device0.sensor0, device1.sensor1], the result tsBlock should be
   * like [Device, sensor0, sensor1]. The sensor1 column of device0 and the sensor0 column of
   * device1 should be null.
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      fragmentInstanceContext.addOperatorContext(
          2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          3, new PlanNodeId("3"), DeviceViewOperatorTest.class.getSimpleName());

      MeasurementPath measurementPath1 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
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

      MeasurementPath measurementPath2 =
          new MeasurementPath(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1.sensor1", TSDataType.INT32);
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

      List<String> devices = new ArrayList<>();
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device0");
      devices.add(DEVICE_MERGE_OPERATOR_TEST_SG + ".device1");
      List<Operator> deviceOperators = new ArrayList<>();
      deviceOperators.add(seriesScanOperator1);
      deviceOperators.add(seriesScanOperator2);
      List<List<Integer>> deviceColumnIndex = new ArrayList<>();
      deviceColumnIndex.add(Collections.singletonList(1));
      deviceColumnIndex.add(Collections.singletonList(2));
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.TEXT);
      dataTypes.add(TSDataType.INT32);
      dataTypes.add(TSDataType.INT32);

      DeviceViewOperator deviceViewOperator =
          new DeviceViewOperator(
              fragmentInstanceContext.getOperatorContexts().get(2),
              devices,
              deviceOperators,
              deviceColumnIndex,
              dataTypes);
      int count = 0;
      while (deviceViewOperator.hasNext()) {
        TsBlock tsBlock = deviceViewOperator.next();
        assertEquals(3, tsBlock.getValueColumnCount());
        assertEquals(20, tsBlock.getPositionCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = i + 20L * (count % 25);
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          assertEquals(
              count < 25
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
        count++;
      }
      assertEquals(50, count);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
