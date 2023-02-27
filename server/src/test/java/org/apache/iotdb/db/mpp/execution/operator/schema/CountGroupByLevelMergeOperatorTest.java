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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.schema.SchemaOperatorTestUtil.EXCEPTION_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CountGroupByLevelMergeOperatorTest {
  private static final String OPERATOR_TEST_SG = "root.CountGroupByLevelMergeOperatorTest";

  @Test
  public void testCountMergeOperator() {
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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          driverContext.addOperatorContext(
              1, planNodeId, CountGroupByLevelScanOperator.class.getSimpleName());
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion, 0));
      CountGroupByLevelScanOperator<ITimeSeriesSchemaInfo> timeSeriesCountOperator1 =
          new CountGroupByLevelScanOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              2,
              mockSchemaSource(schemaRegion, new PartialPath(OPERATOR_TEST_SG + ".device2"), true));

      CountGroupByLevelScanOperator<ITimeSeriesSchemaInfo> timeSeriesCountOperator2 =
          new CountGroupByLevelScanOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              2,
              mockSchemaSource(schemaRegion, new PartialPath(OPERATOR_TEST_SG), true));

      CountGroupByLevelMergeOperator mergeOperator =
          new CountGroupByLevelMergeOperator(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              Arrays.asList(timeSeriesCountOperator1, timeSeriesCountOperator2));

      Assert.assertTrue(mergeOperator.isBlocked().isDone());

      List<TsBlock> tsBlocks = new ArrayList<>();
      while (mergeOperator.hasNext()) {
        TsBlock tsBlock = mergeOperator.next();
        if (tsBlock == null || tsBlock.isEmpty()) {
          continue;
        }
        tsBlocks.add(tsBlock);
      }
      assertFalse(tsBlocks.isEmpty());

      Set<String> pathSet = new HashSet<>(2001);
      for (TsBlock tsBlock : tsBlocks) {
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          String path = tsBlock.getColumn(0).getBinary(i).getStringValue();
          pathSet.add(path);
          assertTrue(path.startsWith(OPERATOR_TEST_SG));
          if (path.equals(OPERATOR_TEST_SG + ".device2")) {
            assertEquals(10, tsBlock.getColumn(1).getLong(i));
          } else {
            assertEquals(1, tsBlock.getColumn(1).getLong(i));
          }
        }
      }

      Assert.assertEquals(2001, pathSet.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testCountScanOperator() {
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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          driverContext.addOperatorContext(
              1, planNodeId, CountGroupByLevelScanOperator.class.getSimpleName());
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion, 0));
      CountGroupByLevelScanOperator<ITimeSeriesSchemaInfo> timeSeriesCountOperator =
          new CountGroupByLevelScanOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              2,
              mockSchemaSource(schemaRegion, new PartialPath(OPERATOR_TEST_SG), true));
      TsBlock tsBlock = null;
      while (timeSeriesCountOperator.hasNext()) {
        tsBlock = timeSeriesCountOperator.next();
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          assertEquals(1, tsBlock.getColumn(1).getLong(i));
        }
      }
      assertNotNull(tsBlock);

      // Assert failure if exception occurs
      CountGroupByLevelScanOperator<ITimeSeriesSchemaInfo> timeSeriesCountOperatorFailure =
          new CountGroupByLevelScanOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              2,
              mockSchemaSource(schemaRegion, new PartialPath(OPERATOR_TEST_SG), false));
      try {
        while (timeSeriesCountOperatorFailure.hasNext()) {
          timeSeriesCountOperatorFailure.next();
        }
      } catch (RuntimeException e) {
        Assert.assertTrue(e.getMessage().contains(EXCEPTION_MESSAGE));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private ISchemaSource<ITimeSeriesSchemaInfo> mockSchemaSource(
      ISchemaRegion schemaRegion, PartialPath path, boolean success) throws Exception {
    ISchemaSource<ITimeSeriesSchemaInfo> schemaSource = Mockito.mock(ISchemaSource.class);
    if (path.equals(new PartialPath(OPERATOR_TEST_SG + ".device2"))) {
      mockSchemaReader(schemaSource, schemaRegion, 10, OPERATOR_TEST_SG + ".device2", success);
    } else if (path.equals(new PartialPath(OPERATOR_TEST_SG))) {
      mockSchemaReader(schemaSource, schemaRegion, 2000, OPERATOR_TEST_SG, success);
    }
    return schemaSource;
  }

  private void mockSchemaReader(
      ISchemaSource<ITimeSeriesSchemaInfo> schemaSource,
      ISchemaRegion schemaRegion,
      int expectedNum,
      String prefix,
      boolean success)
      throws IllegalPathException {
    List<ITimeSeriesSchemaInfo> timeSeriesSchemaInfoList = new ArrayList<>(expectedNum);
    for (int i = 0; i < expectedNum; i++) {
      ITimeSeriesSchemaInfo timeSeriesSchemaInfo = Mockito.mock(ITimeSeriesSchemaInfo.class);
      Mockito.when(timeSeriesSchemaInfo.getPartialPath())
          .thenReturn(new PartialPath(prefix + ".d" + i + ".s"));
      timeSeriesSchemaInfoList.add(timeSeriesSchemaInfo);
    }
    Iterator<ITimeSeriesSchemaInfo> iterator = timeSeriesSchemaInfoList.iterator();
    SchemaOperatorTestUtil.mockGetSchemaReader(schemaSource, iterator, schemaRegion, success);
  }
}
