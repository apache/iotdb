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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignByDeviceOrderByLimitOffsetTest {

  private static final long LIMIT_VALUE = 10;

  @Test
  public void orderByTimeNoValueFilterTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 1. order by time, no value filter
    String sql =
        String.format(
            "select * from root.sg.** ORDER BY TIME DESC LIMIT %s align by device", LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    PlanNode firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertEquals(4, firstFiRoot.getChildren().get(0).getChildren().size());
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof SingleDeviceViewNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(
        plan.getInstances().get(0).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(3).getFragment().getPlanNodeTree(), LIMIT_VALUE);
  }

  @Test
  public void orderByTimeWithValueFilterTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 2. order by time, has value filter
    // put LIMIT-NODE above of SingleDeviceViewNode
    String sql =
        String.format(
            "select * from root.sg.** where s1>1 ORDER BY TIME DESC LIMIT %s align by device",
            LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    PlanNode firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof SingleDeviceViewNode);
      assertTrue(node.getChildren().get(0) instanceof LimitNode);
      assertTrue(node.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(plan.getInstances().get(0).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(1).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(2).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(3).getFragment().getPlanNodeTree(), 0);
  }

  @Test
  public void orderByTimeAndExpressionNoValueFilterTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 3. order by time, expression
    // need read all data, use DeviceViewNode instead of SingleDeviceViewNode
    // no LIMIT-NODE above of DeviceViewNode
    // can push down LIMIT value to ScanNode
    String sql =
        String.format(
            "select * from root.sg.** ORDER BY TIME DESC, s1 DESC LIMIT %s align by device",
            LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    PlanNode firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof DeviceViewNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(
        plan.getInstances().get(0).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(3).getFragment().getPlanNodeTree(), LIMIT_VALUE);
  }

  @Test
  public void orderByTimeAndExpressionWithValueFilterTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 4. order by time, expression, has value filter
    // need read all data, use DeviceViewNode instead of SingleDeviceViewNode
    // no LIMIT-NODE above of DeviceViewNode
    String sql =
        String.format(
            "select * from root.sg.** where s1>1 ORDER BY TIME DESC, s1 DESC LIMIT %s align by device",
            LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan4 = planner.planFragments();
    assertEquals(4, plan4.getInstances().size());
    PlanNode firstFiRoot = plan4.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof DeviceViewNode);
      assertTrue(node.getChildren().get(0) instanceof LimitNode);
      assertTrue(node.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(plan4.getInstances().get(0).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan4.getInstances().get(1).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan4.getInstances().get(2).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan4.getInstances().get(3).getFragment().getPlanNodeTree(), 0);
  }

  @Test
  public void orderByExpressionTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 5. only order by expression
    String sql =
        String.format(
            "select * from root.sg.** ORDER BY s1 DESC LIMIT %s align by device", LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan5 = planner.planFragments();
    assertEquals(4, plan5.getInstances().size());
    PlanNode firstFiRoot = plan5.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof DeviceViewNode);
      assertTrue(node.getChildren().get(0) instanceof TimeJoinNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(plan5.getInstances().get(0).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan5.getInstances().get(1).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan5.getInstances().get(2).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan5.getInstances().get(3).getFragment().getPlanNodeTree(), 0);
  }

  @Test
  public void orderByTimeWithOffsetTest() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    // 6. order by time, offset + limit
    // on top of TOP-K NODE, LIMIT-NODE is necessary
    String sql =
        String.format(
            "select * from root.sg.** ORDER BY time DESC OFFSET %s LIMIT %s align by device",
            LIMIT_VALUE, LIMIT_VALUE);
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan6 = planner.planFragments();
    assertEquals(4, plan6.getInstances().size());
    PlanNode firstFiRoot = plan6.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFIFirstNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFIFirstNode instanceof LimitNode);
    PlanNode firstFiTopNode = ((LimitNode) firstFIFirstNode).getChild().getChildren().get(0);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof SingleDeviceViewNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(
        plan6.getInstances().get(0).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
    assertScanNodeLimitValue(
        plan6.getInstances().get(1).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
    assertScanNodeLimitValue(
        plan6.getInstances().get(2).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
    assertScanNodeLimitValue(
        plan6.getInstances().get(3).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
  }

  private void assertScanNodeLimitValue(PlanNode root, long limitValue) {
    for (PlanNode node : root.getChildren()) {
      if (node instanceof SeriesScanNode) {
        assertEquals(limitValue, ((SeriesScanNode) node).getLimit());
      } else if (node instanceof AlignedSeriesScanNode) {
        assertEquals(limitValue, ((AlignedSeriesScanNode) node).getLimit());
      } else {
        assertScanNodeLimitValue(node, limitValue);
      }
    }
  }
}
