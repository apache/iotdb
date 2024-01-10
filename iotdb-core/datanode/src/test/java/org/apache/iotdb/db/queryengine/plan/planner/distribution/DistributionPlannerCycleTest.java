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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributionPlannerCycleTest {

  // Query sql: `select * from root.sg.d1,root.sg.d2`
  // root.sg.d1 has 2 SeriesScanNodes, root.sg.d2 has 3 SeriesScanNodes.
  //
  // -----------------------------------------------------------------------------------------
  // Note: d1.s1[1] means a SeriesScanNode with target series d1.s1 and its data region is 1
  //
  //                                       IdentityNode
  //                           ____________________|_____________
  //                           |      |                          \
  //                      d1.s1[1]   d1.s2[1]                  Exchange
  //                                                             |
  //                                                          TimeJoinNode
  //                                                         /      \      \
  //                                                    d2.s1[2]  d2.s2[2] d2.s3[2]
  // ------------------------------------------------------------------------------------------
  @Test
  public void timeJoinNodeTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "select * from root.sg.d1,root.sg.d2";
    Analysis analysis = Util2.analyze(sql, context);
    PlanNode logicalPlanNode = Util2.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode firstNode =
        plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    assertEquals(3, firstNode.getChildren().size());
    assertTrue(firstNode.getChildren().get(0) instanceof SeriesScanNode);
    assertTrue(firstNode.getChildren().get(1) instanceof SeriesScanNode);
    assertTrue(firstNode.getChildren().get(2) instanceof ExchangeNode);

    PlanNode secondNode =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    assertEquals(3, secondNode.getChildren().size());
    assertTrue(secondNode.getChildren().get(0) instanceof SeriesScanNode);
    assertTrue(secondNode.getChildren().get(1) instanceof SeriesScanNode);
    assertTrue(secondNode.getChildren().get(2) instanceof SeriesScanNode);
  }
}
