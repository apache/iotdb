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

package org.apache.iotdb.db.queryengine.plan.plan.distribution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DistributionPlannerCycleTest {

  /**
   * Query sql: `select * from root.sg.d1,root.sg.d2`
   *
   * <p>root.sg.d1 has 2 SeriesScanNodes, root.sg.d2 has 3 SeriesScanNodes.
   *
   * <p>Identity | d2-Scan1 d2-Scan2 d2-Scan3 TimeJoin | d1-Scan1 d1-Scan2
   */
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
    PlanNode secondNode =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    assertEquals(4, firstNode.getChildren().size());
    assertEquals(2, secondNode.getChildren().size());
  }
}
