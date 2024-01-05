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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignByTimeOrderByLimitOffsetTest {

  private static final long LIMIT_VALUE = 10;

  @Test
  public void alignByTimeOrderByExpressionLimitTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 1. select * order by expression + limit N
    String sql =
        String.format("select * from root.sg.** ORDER BY root.sg.d1.s1 DESC LIMIT %s", LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    PlanNode firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);

    // 2. select * order by expression + offset M + limit N
    sql =
        String.format(
            "select * from root.sg.** ORDER BY root.sg.d1.s1 DESC OFFSET 5 LIMIT %s", LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof LimitNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof OffsetNode);

    // 3. select s1 order by s2 + limit N
    sql =
        String.format(
            "select s1 from root.sg.d1 ORDER BY root.sg.d22.s2 DESC LIMIT %s", LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TransformNode);

    // 4. select s1 order by s2 + offset M + limit N
    sql =
        String.format(
            "select s1 from root.sg.d1 ORDER BY root.sg.d22.s2 DESC OFFSET 5 LIMIT %s",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof LimitNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof OffsetNode);
  }

  private void assertScanNodeLimitValue(PlanNode root, long limitValue) {
    for (PlanNode node : root.getChildren()) {
      if (node instanceof SeriesScanNode) {
        assertEquals(limitValue, ((SeriesScanNode) node).getPushDownLimit());
      } else if (node instanceof AlignedSeriesScanNode) {
        assertEquals(limitValue, ((AlignedSeriesScanNode) node).getPushDownLimit());
      } else {
        assertScanNodeLimitValue(node, limitValue);
      }
    }
  }
}
