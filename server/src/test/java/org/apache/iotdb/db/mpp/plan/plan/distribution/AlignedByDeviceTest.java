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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesSourceNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignedByDeviceTest {
  @Test
  public void testAggregation2Device2Region() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql = "select count(s1) from root.sg.d333,root.sg.d4444 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f2Root.getChildren().get(1) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql = "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof HorizontallyConcatNode);
    assertTrue(f2Root.getChildren().get(1) instanceof HorizontallyConcatNode);
  }

  @Test
  public void testAggregation2Device2RegionWithValueFilter() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql = "select count(s1) from root.sg.d333,root.sg.d4444 where s1 <= 4 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f2Root.getChildren().get(1) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 where s1 <= 4 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f2Root.getChildren().get(1) instanceof TimeJoinNode);
  }

  @Test
  public void testAggregation2Device2RegionOrderByTime() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql = "select count(s1) from root.sg.d333,root.sg.d4444 order by time align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f2Root.getChildren().get(1) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof HorizontallyConcatNode);
    assertTrue(f2Root.getChildren().get(1) instanceof HorizontallyConcatNode);
  }

  @Test
  public void testAggregation2Device2RegionWithValueFilterOrderByTime() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql =
        "select count(s1) from root.sg.d333,root.sg.d4444 where s1 <= 4 order by time align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f2Root.getChildren().get(1) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 where s1 <= 4 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f2Root.getChildren().get(1) instanceof TimeJoinNode);
  }

  @Test
  public void testAggregation2Device3Region() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql = "select count(s1) from root.sg.d1,root.sg.d333 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    PlanNode f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql = "select count(s1),count(s2) from root.sg.d1,root.sg.d333 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof HorizontallyConcatNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof HorizontallyConcatNode);
  }

  @Test
  public void testAggregation2Device3RegionWithValueFilter() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql = "select count(s1) from root.sg.d1,root.sg.d333 where s1 <= 4 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    PlanNode f3Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql = "select count(s1),count(s2) from root.sg.d1,root.sg.d333 where s1 <= 4 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof TimeJoinNode);
  }

  @Test
  public void testAggregation2Device3RegionOrderByTime() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql = "select count(s1) from root.sg.d1,root.sg.d333 order by time align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    PlanNode f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql = "select count(s1),count(s2) from root.sg.d1,root.sg.d333 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof HorizontallyConcatNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof HorizontallyConcatNode);
  }

  @Test
  public void testAggregation2Device3RegionWithValueFilterOrderByTime() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    // test of SINGLE_SERIES
    String sql =
        "select count(s1) from root.sg.d1,root.sg.d333 where s1 <= 4 order by time align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    PlanNode f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesSourceNode);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d1,root.sg.d333 where s1 <= 4 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TimeJoinNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
            instanceof SeriesSourceNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof TimeJoinNode);
  }

  @Test
  public void testDiffFunction2Device2Region() {
    QueryId queryId = new QueryId("test_special_process_align_by_device_2_device_2_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select diff(s1), diff(s2) from root.sg.d333,root.sg.d4444 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof TransformNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
  }

  @Test
  public void testDiffFunctionWithOrderByTime2Device2Region() {
    QueryId queryId =
        new QueryId("test_special_process_align_by_device_with_order_by_time_2_device_2_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql =
        "select diff(s1), diff(s2) from root.sg.d333,root.sg.d4444 order by time align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
  }

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
    //                                |                        IdentityNode
    //                                |                             |
    //                           TimeJoinNode                  TransformNode
    //                           /     |      \                     |
    //                     d1.s1[1]  d1.s2[1]  Exchange         TimeJoinNode
    //                                            |               /      \
    //                                        IdentityNode  d22.s1[3]   d22.s2[3]
    //                                            |
    //                                        TimeJoinNode
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
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    PlanNode f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f3Root.getChildren().get(0) instanceof TransformNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof TransformNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof ExchangeNode);
    assertTrue(f3Root.getChildren().get(0).getChildren().get(0) instanceof TimeJoinNode);
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
    //                                |                        ShuffleSinkNode
    //                                |                             |
    //                           TransformNode              SingleDeviceViewNode
    //                                |                             |
    //                           TimeJoinNode                  TransformNode
    //                           /     |      \                     |
    //                     d1.s1[1]  d1.s2[1]  Exchange         TimeJoinNode
    //                                            |               /      \
    //                                        IdentityNode  d22.s1[3]   d22.s2[3]
    //                                            |
    //                                      ShuffleSinkNode
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
    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode f2Root = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    PlanNode f3Root = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f1Root.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(f2Root.getChildren().get(0) instanceof TimeJoinNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(2)
            instanceof ExchangeNode);
    assertTrue(
        f3Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TimeJoinNode);
  }
}
