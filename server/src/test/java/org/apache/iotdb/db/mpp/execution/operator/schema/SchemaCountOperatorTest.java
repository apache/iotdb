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
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaCountOperatorTest {
  private static final String SCHEMA_COUNT_OPERATOR_TEST_SG = "root.SchemaCountOperatorTest";

  @Test
  public void testDeviceCountOperator() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          fragmentInstanceContext.addOperatorContext(
              1, planNodeId, DevicesCountOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG);
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);

      List<IDeviceSchemaInfo> deviceSchemaInfoList = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        deviceSchemaInfoList.add(Mockito.mock(IDeviceSchemaInfo.class));
      }
      Iterator<IDeviceSchemaInfo> iterator = deviceSchemaInfoList.iterator();
      Mockito.when(
              schemaRegion.getDeviceReader(
                  SchemaRegionReadPlanFactory.getShowDevicesPlan(partialPath, true)))
          .thenReturn(
              new ISchemaReader<IDeviceSchemaInfo>() {
                @Override
                public void close() throws Exception {}

                @Override
                public boolean hasNext() {
                  return iterator.hasNext();
                }

                @Override
                public IDeviceSchemaInfo next() {
                  return iterator.next();
                }
              });

      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      DevicesCountOperator devicesCountOperator =
          new DevicesCountOperator(
              planNodeId, fragmentInstanceContext.getOperatorContexts().get(0), partialPath, true);
      TsBlock tsBlock = null;
      while (devicesCountOperator.hasNext()) {
        tsBlock = devicesCountOperator.next();
      }
      assertNotNull(tsBlock);
      assertEquals(tsBlock.getColumn(0).getLong(0), 10);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testTimeSeriesCountOperator() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          fragmentInstanceContext.addOperatorContext(
              1, planNodeId, TimeSeriesCountOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG);
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
      Mockito.when(
              schemaRegion.getTimeSeriesReader(
                  SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                      partialPath, Collections.emptyMap(), false, null, null, 0, 0, true)))
          .thenReturn(mockTimeSeriesReader(100));
      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      TimeSeriesCountOperator timeSeriesCountOperator =
          new TimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              partialPath,
              true,
              null,
              null,
              false,
              Collections.emptyMap());
      TsBlock tsBlock = null;
      while (timeSeriesCountOperator.hasNext()) {
        tsBlock = timeSeriesCountOperator.next();
      }
      assertNotNull(tsBlock);
      assertEquals(100, tsBlock.getColumn(0).getLong(0));

      Mockito.when(
              schemaRegion.getTimeSeriesReader(
                  SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                      new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device1.*"),
                      Collections.emptyMap(),
                      false,
                      null,
                      null,
                      0,
                      0,
                      false)))
          .thenReturn(mockTimeSeriesReader(10));
      TimeSeriesCountOperator timeSeriesCountOperator2 =
          new TimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device1.*"),
              false,
              null,
              null,
              false,
              Collections.emptyMap());
      tsBlock = timeSeriesCountOperator2.next();
      assertFalse(timeSeriesCountOperator2.hasNext());
      assertTrue(timeSeriesCountOperator2.isFinished());
      assertEquals(10, tsBlock.getColumn(0).getLong(0));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private ISchemaReader<ITimeSeriesSchemaInfo> mockTimeSeriesReader(int expectedCount) {
    List<ITimeSeriesSchemaInfo> timeSeriesSchemaInfoList = new ArrayList<>(expectedCount);
    for (int i = 0; i < expectedCount; i++) {
      timeSeriesSchemaInfoList.add(Mockito.mock(ITimeSeriesSchemaInfo.class));
    }
    Iterator<ITimeSeriesSchemaInfo> iterator = timeSeriesSchemaInfoList.iterator();
    return new ISchemaReader<ITimeSeriesSchemaInfo>() {
      @Override
      public void close() throws Exception {}

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public ITimeSeriesSchemaInfo next() {
        return iterator.next();
      }
    };
  }

  @Test
  public void testLevelTimeSeriesCountOperator() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          fragmentInstanceContext.addOperatorContext(
              1, planNodeId, LevelTimeSeriesCountOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG);
      ISchemaRegion schemaRegion = mockSchemaRegion();

      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      LevelTimeSeriesCountOperator timeSeriesCountOperator =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              partialPath,
              true,
              2,
              null,
              null,
              false);
      TsBlock tsBlock = null;
      List<TsBlock> tsBlockList = collectResult(timeSeriesCountOperator);
      assertEquals(1, tsBlockList.size());
      tsBlock = tsBlockList.get(0);

      for (int i = 0; i < 10; i++) {
        String path = tsBlock.getColumn(0).getBinary(i).getStringValue();
        assertTrue(path.startsWith(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device"));
        assertEquals(10, tsBlock.getColumn(1).getLong(i));
      }

      LevelTimeSeriesCountOperator timeSeriesCountOperator2 =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".**"),
              false,
              1,
              null,
              null,
              false);
      tsBlockList = collectResult(timeSeriesCountOperator2);
      assertEquals(1, tsBlockList.size());
      tsBlock = tsBlockList.get(0);

      assertEquals(100, tsBlock.getColumn(1).getLong(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private List<TsBlock> collectResult(LevelTimeSeriesCountOperator operator) {
    List<TsBlock> tsBlocks = new ArrayList<>();
    while (operator.hasNext()) {
      TsBlock tsBlock = operator.next();
      if (tsBlock == null || tsBlock.isEmpty()) {
        continue;
      }
      tsBlocks.add(tsBlock);
    }
    return tsBlocks;
  }

  private ISchemaRegion mockSchemaRegion() throws Exception {
    ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
    ISchemaReader<ITimeSeriesSchemaInfo> schemaReader = mockSchemaReader();
    Mockito.when(
            schemaRegion.getTimeSeriesReader(
                SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                    new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG),
                    null,
                    false,
                    null,
                    null,
                    0,
                    0,
                    true)))
        .thenReturn(schemaReader);
    schemaReader = mockSchemaReader();
    Mockito.when(
            schemaRegion.getTimeSeriesReader(
                SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                    new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".**"),
                    null,
                    false,
                    null,
                    null,
                    0,
                    0,
                    false)))
        .thenReturn(schemaReader);
    return schemaRegion;
  }

  private ISchemaReader<ITimeSeriesSchemaInfo> mockSchemaReader() throws IllegalPathException {
    List<ITimeSeriesSchemaInfo> timeSeriesSchemaInfoList = new ArrayList<>(1000);
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        ITimeSeriesSchemaInfo timeSeriesSchemaInfo = Mockito.mock(ITimeSeriesSchemaInfo.class);
        Mockito.when(timeSeriesSchemaInfo.getPartialPath())
            .thenReturn(new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device" + i + ".s" + j));
        timeSeriesSchemaInfoList.add(timeSeriesSchemaInfo);
      }
    }
    Iterator<ITimeSeriesSchemaInfo> iterator = timeSeriesSchemaInfoList.iterator();
    return new ISchemaReader<ITimeSeriesSchemaInfo>() {
      @Override
      public void close() throws Exception {}

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public ITimeSeriesSchemaInfo next() {
        return iterator.next();
      }
    };
  }
}
