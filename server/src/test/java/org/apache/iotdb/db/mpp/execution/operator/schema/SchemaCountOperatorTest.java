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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
      Mockito.when(schemaRegion.getDevicesNum(partialPath, true)).thenReturn(10L);
      operatorContext
          .getInstanceContext()
          .setDriverContext(new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
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
      Mockito.when(schemaRegion.getAllTimeseriesCount(partialPath, Collections.emptyMap(), true))
          .thenReturn(100L);
      Mockito.when(
              schemaRegion.getAllTimeseriesCount(
                  new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device1.*"),
                  Collections.emptyMap(),
                  false))
          .thenReturn(10L);
      operatorContext
          .getInstanceContext()
          .setDriverContext(new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
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
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);

      Map<PartialPath, Long> result = new HashMap<>();
      for (int i = 0; i < 10; i++) {
        result.put(new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device" + i), 10L);
      }

      Mockito.when(schemaRegion.getMeasurementCountGroupByLevel(partialPath, 2, true))
          .thenReturn(result);
      Mockito.when(schemaRegion.getMeasurementCountGroupByLevel(partialPath, 1, true))
          .thenReturn(
              Collections.singletonMap(new PartialPath(SCHEMA_COUNT_OPERATOR_TEST_SG), 100L));

      operatorContext
          .getInstanceContext()
          .setDriverContext(new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
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
      while (timeSeriesCountOperator.hasNext()) {
        tsBlock = timeSeriesCountOperator.next();
        assertFalse(timeSeriesCountOperator.hasNext());
      }
      assertNotNull(tsBlock);

      for (int i = 0; i < 10; i++) {
        String path = tsBlock.getColumn(0).getBinary(i).getStringValue();
        assertTrue(path.startsWith(SCHEMA_COUNT_OPERATOR_TEST_SG + ".device"));
        assertEquals(10, tsBlock.getColumn(1).getLong(i));
      }

      LevelTimeSeriesCountOperator timeSeriesCountOperator2 =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              partialPath,
              true,
              1,
              null,
              null,
              false);
      while (timeSeriesCountOperator2.hasNext()) {
        tsBlock = timeSeriesCountOperator2.next();
      }
      assertNotNull(tsBlock);
      assertEquals(100, tsBlock.getColumn(1).getLong(0));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
