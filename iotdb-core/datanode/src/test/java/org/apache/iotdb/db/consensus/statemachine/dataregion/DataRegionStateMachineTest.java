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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DataRegionStateMachineTest {

  @Test
  public void testMergeInsertRowNodes() throws IllegalPathException {
    List<InsertNode> list = new ArrayList<>();
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            1000L,
            new Object[] {1.0, 2f, 300L},
            false);
    list.add(node);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            999L,
            new Object[] {1.0, 2f, 300L},
            false);
    list.add(node);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            998L,
            new Object[] {1.0, 2f, 300L},
            false);
    list.add(node);
    DataRegionStateMachine fakeStateMachine = new DataRegionStateMachine(null);
    InsertNode mergedNode = list.get(0).mergeInsertNode(list);
    Assert.assertTrue(mergedNode instanceof InsertRowsNode);
  }

  @Test
  public void testMergeInsertRowsNodes() throws IllegalPathException {
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId("plan node 1"));
    List<InsertNode> list = new ArrayList<>();
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            1000L,
            new Object[] {1.0, 2f, 300L},
            false);
    insertRowsNode.addOneInsertRowNode(node, 0);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            999L,
            new Object[] {1.0, 2f, 300L},
            false);
    insertRowsNode.addOneInsertRowNode(node, 1);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            998L,
            new Object[] {1.0, 2f, 300L},
            false);
    insertRowsNode.addOneInsertRowNode(node, 2);
    DataRegionStateMachine fakeStateMachine = new DataRegionStateMachine(null);
    list.add(insertRowsNode);
    list.add(insertRowsNode);
    InsertNode mergedNode = list.get(0).mergeInsertNode(list);
    Assert.assertTrue(mergedNode instanceof InsertRowsNode);
  }
}
