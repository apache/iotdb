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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignByDeviceOrderByLimitOffsetTest {

  @Test
  public void orderByTimeTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 1. order by time, no value filter
    String sql = "select * from root.sg.** ORDER BY TIME LIMIT 10 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof TopKNode);

    // 2. order by time, has value filter
    sql = "select * from root.sg.** where s1>1 ORDER BY TIME LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan2 = planner.planFragments();
    assertEquals(4, plan2.getInstances().size());

    // 3. order by time, expression

    // 4. order by time, offset limit
    // on top of TOP-K NODE, LIMIT-NODE is necessary
  }
}
