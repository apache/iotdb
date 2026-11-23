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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableFunctionProcessorSplitTest {

  @Test
  public void testFunctionsNeedingWholePartitionAreNotSplit() {
    // the children of a partitioned GroupNode may each hold only part of one logical partition
    for (TableBuiltinTableFunction function :
        new TableBuiltinTableFunction[] {
          TableBuiltinTableFunction.SAMPLE,
          TableBuiltinTableFunction.DERIVATIVE,
          TableBuiltinTableFunction.FFT
        }) {
      assertFalse(
          function.getFunctionName(),
          TableDistributedPlanGenerator.canSplitTableFunctionProcessor(
              processor(function.getFunctionName(), false, partitionedGroup())));
    }
  }

  @Test
  public void testOtherProcessorsCanStillSplitPerChild() {
    assertTrue(
        TableDistributedPlanGenerator.canSplitTableFunctionProcessor(
            processor(
                TableBuiltinTableFunction.CLASSIFY.getFunctionName(), false, partitionedGroup())));
    // row semantics processes each row independently of the partition
    assertTrue(
        TableDistributedPlanGenerator.canSplitTableFunctionProcessor(
            processor(
                TableBuiltinTableFunction.SAMPLE.getFunctionName(), true, partitionedGroup())));
  }

  @Test
  public void testUnpartitionedChildIsNotSplit() {
    GroupNode unpartitionedGroup = Mockito.mock(GroupNode.class);
    Mockito.when(unpartitionedGroup.getPartitionKeyCount()).thenReturn(0);
    assertFalse(
        TableDistributedPlanGenerator.canSplitTableFunctionProcessor(
            processor(
                TableBuiltinTableFunction.CLASSIFY.getFunctionName(), false, unpartitionedGroup)));

    PlanNode scan = Mockito.mock(PlanNode.class);
    assertFalse(
        TableDistributedPlanGenerator.canSplitTableFunctionProcessor(
            processor(TableBuiltinTableFunction.CLASSIFY.getFunctionName(), false, scan)));
  }

  private static GroupNode partitionedGroup() {
    GroupNode group = Mockito.mock(GroupNode.class);
    Mockito.when(group.getPartitionKeyCount()).thenReturn(1);
    return group;
  }

  private static TableFunctionProcessorNode processor(
      String name, boolean rowSemantic, PlanNode child) {
    TableFunctionProcessorNode node = Mockito.mock(TableFunctionProcessorNode.class);
    Mockito.when(node.getName()).thenReturn(name);
    Mockito.when(node.isRowSemantic()).thenReturn(rowSemantic);
    Mockito.when(node.getChild()).thenReturn(child);
    return node;
  }
}
