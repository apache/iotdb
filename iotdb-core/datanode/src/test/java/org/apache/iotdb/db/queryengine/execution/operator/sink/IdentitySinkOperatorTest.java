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

package org.apache.iotdb.db.queryengine.execution.operator.sink;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
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
import org.junit.Assert;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IdentitySinkOperatorTest {
  private static final String IDENTITY_SINK_TEST = "root.identitySinkTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, IDENTITY_SINK_TEST);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest() throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      IFullPath measurementPath1 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(IDENTITY_SINK_TEST + ".device0"),
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
          3, new PlanNodeId("3"), IdentitySinkOperator.class.getSimpleName());
      PlanNodeId planNodeId4 = new PlanNodeId("4");
      driverContext.addOperatorContext(4, planNodeId4, SeriesScanOperator.class.getSimpleName());
      PlanNodeId planNodeId5 = new PlanNodeId("5");
      driverContext.addOperatorContext(5, planNodeId5, SeriesScanOperator.class.getSimpleName());

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
          .setMaxRunTime(new Duration(1000, TimeUnit.MILLISECONDS));

      IFullPath measurementPath2 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(IDENTITY_SINK_TEST + ".device0"),
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
          .setMaxRunTime(new Duration(1000, TimeUnit.MILLISECONDS));

      IFullPath measurementPath3 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(IDENTITY_SINK_TEST + ".device0"),
              new MeasurementSchema("sensor0", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator3 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(3),
              planNodeId4,
              measurementPath3,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator3.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator3
          .getOperatorContext()
          .setMaxRunTime(new Duration(1000, TimeUnit.MILLISECONDS));

      IFullPath measurementPath4 =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(IDENTITY_SINK_TEST + ".device0"),
              new MeasurementSchema("sensor1", TSDataType.INT32));
      SeriesScanOperator seriesScanOperator4 =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(4),
              planNodeId5,
              measurementPath4,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator4.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
      seriesScanOperator4
          .getOperatorContext()
          .setMaxRunTime(new Duration(1000, TimeUnit.MILLISECONDS));

      ISinkHandle sinkHandle = Mockito.mock(ShuffleSinkHandle.class);
      Mockito.when(sinkHandle.isChannelClosed(0)).thenReturn(false);
      Mockito.when(sinkHandle.isChannelClosed(1)).thenReturn(false);
      IdentitySinkOperator identitySinkOperator =
          new IdentitySinkOperator(
              driverContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              new DownStreamChannelIndex(0),
              sinkHandle);
      identitySinkOperator
          .getOperatorContext()
          .setMaxRunTime(new Duration(1200, TimeUnit.MILLISECONDS));

      Assert.assertEquals(
          seriesScanOperator3.calculateMaxPeekMemory(),
          identitySinkOperator.calculateMaxPeekMemory());
      Assert.assertEquals(
          seriesScanOperator3.calculateMaxReturnSize(),
          identitySinkOperator.calculateMaxReturnSize());

      while (seriesScanOperator3.hasNext()
          && identitySinkOperator.isBlocked().isDone()
          && identitySinkOperator.hasNext()) {
        TsBlock identityTsBlock = seriesScanOperator3.next();
        TsBlock tsBlock = identitySinkOperator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(1, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertNotNull(identityTsBlock);
        assertEquals(identityTsBlock.getPositionCount(), tsBlock.getPositionCount());
        for (int i = 0; i < identityTsBlock.getPositionCount(); i++) {
          assertEquals(identityTsBlock.getColumn(0).getInt(i), tsBlock.getColumn(0).getInt(i));
        }
      }

      while (seriesScanOperator4.hasNext()
          && identitySinkOperator.isBlocked().isDone()
          && identitySinkOperator.hasNext()) {
        TsBlock identityTsBlock = seriesScanOperator4.next();
        TsBlock tsBlock = identitySinkOperator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(1, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertNotNull(identityTsBlock);
        assertEquals(identityTsBlock.getPositionCount(), tsBlock.getPositionCount());
        for (int i = 0; i < identityTsBlock.getPositionCount(); i++) {
          assertEquals(identityTsBlock.getColumn(0).getInt(i), tsBlock.getColumn(0).getInt(i));
        }
      }

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
