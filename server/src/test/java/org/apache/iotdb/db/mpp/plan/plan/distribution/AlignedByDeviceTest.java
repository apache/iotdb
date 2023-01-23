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

package org.apache.iotdb.db.mpp.plan.plan.distribution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignedByDeviceTest {

  @Test
  public void test1Device1Region() {}

  @Test
  public void testDiffFunction2Device3Region() {
    QueryId queryId = new QueryId("test_special_process_align_by_device_2_device_3_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // ------------------------------------------------------------------------------------------------
    // Note: d1.s1[1] means a SeriesScanNode with target series d1.s1 and its data region is 1
    //
    //                                       DeviceViewNode
    //                                  ___________|__________
    //                                 /                      \
    //                           TransformNode                 Exchange
    //                                |                             |
    //                           TimeJoinNode                  TransformNode
    //                           /     |      \                     |
    //                     d1.s1[1]  d1.s2[1]  Exchange         TimeJoinNode
    //                                            |               /      \
    //                                        TimeJoinNode  d22.s1[3]   d22.s2[3]
    //                                        /      \
    //                                  d1.s1[2]    d1.s2[2]
    // ------------------------------------------------------------------------------------------------
    String sql = "select diff(s1), diff(s2) from root.sg.d1,root.sg.d22 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root =
        plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f2Root =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f3Root =
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(f1Root instanceof DeviceViewNode);
    assertTrue(f2Root instanceof TimeJoinNode);
    assertTrue(f3Root instanceof TransformNode);
    assertTrue(f1Root.getChildren().get(0) instanceof TransformNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f3Root.getChildren().get(0) instanceof TimeJoinNode);
  }

  @Test
  public void testDiffFunctionWithOrderByTime2Device3Region() {
    QueryId queryId =
        new QueryId("test_special_process_align_by_device_with_order_by_time_2_device_3_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // ------------------------------------------------------------------------------------------------
    // Note: d1.s1[1] means a SeriesScanNode with target series d1.s1 and its data region is 1
    //
    //                                       MergeSortNode
    //                                  ___________|______________
    //                                 /                           \
    //                         SingleDeviceViewNode             Exchange
    //                                |                             |
    //                           TransformNode              SingleDeviceViewNode
    //                                |                             |
    //                           TimeJoinNode                  TransformNode
    //                           /     |      \                     |
    //                     d1.s1[1]  d1.s2[1]  Exchange         TimeJoinNode
    //                                            |               /      \
    //                                       TimeJoinNode  d22.s1[3]   d22.s2[3]
    //                                        /      \
    //                                  d1.s1[2]    d1.s2[2]
    // ------------------------------------------------------------------------------------------------
    String sql =
        "select diff(s1), diff(s2) from root.sg.d1,root.sg.d22 order by time align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root =
        plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f2Root =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f3Root =
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(f1Root instanceof MergeSortNode);
    assertTrue(f2Root instanceof TimeJoinNode);
    assertTrue(f3Root instanceof SingleDeviceViewNode);
    assertTrue(f1Root.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f3Root.getChildren().get(0).getChildren().get(0) instanceof TimeJoinNode);
  }

  private LogicalQueryPlan constructLogicalPlan(List<String> series) {
    return null;
  }
}
