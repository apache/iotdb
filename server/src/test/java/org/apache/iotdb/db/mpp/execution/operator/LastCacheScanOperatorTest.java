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
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.source.LastCacheScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import org.junit.Test;

import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LastCacheScanOperatorTest {

  @Test
  public void batchTest() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, SeriesScanOperator.class.getSimpleName());

      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(6);

      LastQueryUtil.appendLastValue(builder, 1, "root.sg.d.s1", "true", "BOOLEAN");
      LastQueryUtil.appendLastValue(builder, 2, "root.sg.d.s2", "2", "INT32");
      LastQueryUtil.appendLastValue(builder, 3, "root.sg.d.s3", "3", "INT64");
      LastQueryUtil.appendLastValue(builder, 4, "root.sg.d.s4", "4.4", "FLOAT");
      LastQueryUtil.appendLastValue(builder, 3, "root.sg.d.s5", "3.3", "DOUBLE");
      LastQueryUtil.appendLastValue(builder, 1, "root.sg.d.s6", "peace", "TEXT");

      TsBlock tsBlock = builder.build();

      LastCacheScanOperator lastCacheScanOperator =
          new LastCacheScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(0), planNodeId1, tsBlock);

      assertTrue(lastCacheScanOperator.isBlocked().isDone());
      assertTrue(lastCacheScanOperator.hasNext());
      TsBlock result = lastCacheScanOperator.next();
      assertEquals(tsBlock.getPositionCount(), result.getPositionCount());
      assertEquals(tsBlock.getValueColumnCount(), result.getValueColumnCount());
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        assertEquals(tsBlock.getTimeByIndex(i), result.getTimeByIndex(i));
        for (int j = 0; j < tsBlock.getValueColumnCount(); j++) {
          assertEquals(tsBlock.getColumn(j).getBinary(i), result.getColumn(j).getBinary(i));
        }
      }
      assertFalse(lastCacheScanOperator.hasNext());
      assertTrue(lastCacheScanOperator.isFinished());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
