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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
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

public class CountMergeOperatorTest {
  private static final String COUNT_MERGE_OPERATOR_TEST_SG = "root.CountMergeOperatorTest";

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
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          fragmentInstanceContext.addOperatorContext(
              1, planNodeId, LevelTimeSeriesCountOperator.class.getSimpleName());
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
      Map<PartialPath, Long> sgResult = new HashMap<>();
      for (int i = 0; i < 10; i++) {
        sgResult.put(new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG + ".device" + i), 10L);
      }
      Mockito.when(
              schemaRegion.getMeasurementCountGroupByLevel(
                  new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG), 2, true))
          .thenReturn(sgResult);
      Mockito.when(
              schemaRegion.getMeasurementCountGroupByLevel(
                  new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG + ".device2"), 2, true))
          .thenReturn(
              Collections.singletonMap(
                  new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG + ".device2"), 10L));
      operatorContext
          .getInstanceContext()
          .setDriverContext(new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      LevelTimeSeriesCountOperator timeSeriesCountOperator1 =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG),
              true,
              2,
              null,
              null,
              false);
      LevelTimeSeriesCountOperator timeSeriesCountOperator2 =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG + ".device2"),
              true,
              2,
              null,
              null,
              false);
      CountMergeOperator countMergeOperator =
          new CountMergeOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              Arrays.asList(timeSeriesCountOperator1, timeSeriesCountOperator2));
      TsBlock tsBlock = null;
      Assert.assertTrue(countMergeOperator.isBlocked().isDone());
      while (countMergeOperator.hasNext()) {
        tsBlock = countMergeOperator.next();
        if (tsBlock != null) {
          assertFalse(countMergeOperator.hasNext());
        }
      }
      assertNotNull(tsBlock);
      for (int i = 0; i < 10; i++) {
        String path = tsBlock.getColumn(0).getBinary(i).getStringValue();
        assertTrue(path.startsWith(COUNT_MERGE_OPERATOR_TEST_SG + ".device"));
        if (path.equals(COUNT_MERGE_OPERATOR_TEST_SG + ".device2")) {
          assertEquals(20, tsBlock.getColumn(1).getLong(i));
        } else {
          assertEquals(10, tsBlock.getColumn(1).getLong(i));
        }
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
