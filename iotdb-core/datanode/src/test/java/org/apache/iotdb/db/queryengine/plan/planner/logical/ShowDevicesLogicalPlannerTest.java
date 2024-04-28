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

package org.apache.iotdb.db.queryengine.plan.planner.logical;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;

import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.planner.logical.LogicalPlannerTestUtil.parseSQLToPlanNode;

public class ShowDevicesLogicalPlannerTest {

  @Test
  public void testShowDevicesWithTimeCondition() throws IllegalPathException {
    String sql = "show devices where time > 1000";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, Boolean> devicePathsToAligned = new HashMap<>();
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d1")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2.a")), true);

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), devicePathsToAligned, null);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }

  @Test
  public void testShowDevicesWithTimeConditionWithLimitOffset() throws IllegalPathException {
    String sql = "show devices where time > 1000 limit 20 offset 10";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, Boolean> devicePathsToAligned = new HashMap<>();
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d1")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2.a")), true);

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), devicePathsToAligned, null);

    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), 20);
    limitNode.addChild(regionScanNode);
    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), 10);
    offsetNode.addChild(limitNode);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, offsetNode);
  }
}
