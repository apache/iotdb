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
package org.apache.iotdb.db.queryengine.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriver;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.StubSink;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.google.common.util.concurrent.ListenableFuture;
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
import org.mockito.Mockito;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataDriverTest {

  private static final String DATA_DRIVER_TEST_SG = "root.DataDriverTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private static final Duration EXECUTION_TIME_SLICE =
      new Duration(
          IoTDBDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs(),
          TimeUnit.MILLISECONDS);

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, DATA_DRIVER_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IFullPath measurementPath1 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(DATA_DRIVER_TEST_SG + ".device0"),
              new MeasurementSchema("sensor0", TSDataType.INT32));
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      allSensors.add("sensor1");
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      DataRegion dataRegion = Mockito.mock(DataRegion.class);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      fragmentInstanceContext.setDataRegion(dataRegion);
      DataDriverContext driverContext = new DataDriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId2 = new PlanNodeId("2");
      driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
      driverContext.addOperatorContext(
          3, new PlanNodeId("3"), FullOuterTimeJoinOperator.class.getSimpleName());
      driverContext.addOperatorContext(4, new PlanNodeId("4"), LimitOperator.class.getSimpleName());

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId1,
              measurementPath1,
              Ordering.ASC,
              scanOptionsBuilder.build());
      driverContext.addSourceOperator(seriesScanOperator1);
      driverContext.addPath(measurementPath1);
      seriesScanOperator1
          .getOperatorContext()
          .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

      IFullPath measurementPath2 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(DATA_DRIVER_TEST_SG + ".device0"),
              new MeasurementSchema("sensor1", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(1),
              planNodeId2,
              measurementPath2,
              Ordering.ASC,
              scanOptionsBuilder.build());
      driverContext.addSourceOperator(seriesScanOperator2);
      driverContext.addPath(measurementPath2);

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

      LimitOperator limitOperator =
          new LimitOperator(driverContext.getOperatorContexts().get(3), 250, timeJoinOperator);

      String deviceId = DATA_DRIVER_TEST_SG + ".device0";
      Mockito.when(
              dataRegion.query(
                  driverContext.getPaths(),
                  IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
                  fragmentInstanceContext,
                  null,
                  null))
          .thenReturn(new QueryDataSource(seqResources, unSeqResources));
      fragmentInstanceContext.initQueryDataSource(driverContext.getPaths());
      fragmentInstanceContext.initializeNumOfDrivers(1);

      StubSink stubSink = new StubSink(fragmentInstanceContext);
      driverContext.setSink(stubSink);
      IDriver dataDriver = null;
      try {
        dataDriver = new DataDriver(limitOperator, driverContext, 0);
        assertEquals(
            fragmentInstanceContext.getId(), dataDriver.getDriverTaskId().getFragmentInstanceId());

        assertFalse(dataDriver.isFinished());

        while (!dataDriver.isFinished()) {
          assertEquals(FragmentInstanceState.RUNNING, stateMachine.getState());
          ListenableFuture<?> blocked = dataDriver.processFor(EXECUTION_TIME_SLICE);
          assertTrue(blocked.isDone());
        }

        assertEquals(FragmentInstanceState.FLUSHING, stateMachine.getState());

        List<TsBlock> result = stubSink.getTsBlocks();

        int row = 0;
        for (TsBlock tsBlock : result) {
          assertEquals(2, tsBlock.getValueColumnCount());
          assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
          assertTrue(tsBlock.getColumn(1) instanceof IntColumn);

          for (int j = 0; j < tsBlock.getPositionCount(); j++, row++) {
            assertEquals(row, tsBlock.getTimeByIndex(j));
            if ((long) row < 200) {
              assertEquals(20000 + (long) row, tsBlock.getColumn(0).getInt(j));
              assertEquals(20000 + (long) row, tsBlock.getColumn(1).getInt(j));
            } else if ((long) row < 260
                || ((long) row >= 300 && (long) row < 380)
                || (long) row >= 400) {
              assertEquals(10000 + (long) row, tsBlock.getColumn(0).getInt(j));
              assertEquals(10000 + (long) row, tsBlock.getColumn(1).getInt(j));
            } else {
              assertEquals(row, tsBlock.getColumn(0).getInt(j));
              assertEquals(row, tsBlock.getColumn(1).getInt(j));
            }
          }
        }

        assertEquals(250, row);

      } finally {
        if (dataDriver != null) {
          dataDriver.close();
        }
      }
    } catch (QueryProcessException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
