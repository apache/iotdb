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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignByDeviceOrderByLimitOffsetTest {

  private static final long LIMIT_VALUE = 10;

  QueryId queryId = new QueryId("test");
  MPPQueryContext context =
      new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
  String sql;
  Analysis analysis;
  PlanNode logicalPlanNode;
  DistributionPlanner planner;
  DistributedQueryPlan plan;
  PlanNode firstFiRoot;
  PlanNode mergeSortNode;

  /*
   * IdentitySinkNode-27
   *   └──LimitNode-22
   *       └──MergeSort-21
   *           ├──DeviceView-12
   *           │   └──FullOuterTimeJoinNode-11
   *           │       ├──SeriesScanNode-9:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │       └──SeriesScanNode-10:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           ├──ExchangeNode-23: [SourceAddress:192.0.3.1/test.2.0/25]
   *           └──ExchangeNode-24: [SourceAddress:192.0.2.1/test.3.0/26]
   *
   * IdentitySinkNode-25
   *   └──DeviceView-16
   *       └──FullOuterTimeJoinNode-15
   *           ├──SeriesScanNode-13:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *           └──SeriesScanNode-14:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-26
   *   └──DeviceView-20
   *       └──FullOuterTimeJoinNode-19
   *           ├──SeriesScanNode-17:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *           └──SeriesScanNode-18:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   */
  @Test
  public void orderByDeviceTest1() {
    // no order by
    sql = "select * from root.sg.d1, root.sg.d22 LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof LimitNode);
    mergeSortNode = ((LimitNode) firstFiRoot.getChildren().get(0)).getChild();
    assertTrue(mergeSortNode instanceof MergeSortNode);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        mergeSortNode.getChildren().get(0).getChildren().get(0) instanceof FullOuterTimeJoinNode);

    // order by device, time
    sql =
        "select * from root.sg.d1, root.sg.d22 order by device asc, time desc LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof LimitNode);
    mergeSortNode = ((LimitNode) firstFiRoot.getChildren().get(0)).getChild();
    assertTrue(mergeSortNode instanceof MergeSortNode);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        mergeSortNode.getChildren().get(0).getChildren().get(0) instanceof FullOuterTimeJoinNode);
    assertScanNodeLimitValue(
        plan.getInstances().get(0).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree(), LIMIT_VALUE);
  }

  /*
   * IdentitySinkNode-32
   *   └──LimitNode-27
   *       └──MergeSort-26
   *           ├──DeviceView-15
   *           │   └──SortNode-14
   *           │       └──FullOuterTimeJoinNode-13
   *           │           ├──SeriesScanNode-11:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │           └──SeriesScanNode-12:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           ├──ExchangeNode-28: [SourceAddress:192.0.3.1/test.2.0/30]
   *           └──ExchangeNode-29: [SourceAddress:192.0.2.1/test.3.0/31]
   *
   *  IdentitySinkNode-30
   *   └──DeviceView-20
   *       └──SortNode-19
   *           └──FullOuterTimeJoinNode-18
   *               ├──SeriesScanNode-16:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-17:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-31
   *   └──DeviceView-25
   *       └──SortNode-24
   *           └──FullOuterTimeJoinNode-23
   *               ├──SeriesScanNode-21:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesScanNode-22:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   */
  @Test
  public void orderByDeviceTest2() {
    // order by device, expression
    sql =
        "select * from root.sg.d1, root.sg.d22 order by device asc, s1 desc LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof LimitNode);
    mergeSortNode = ((LimitNode) firstFiRoot.getChildren().get(0)).getChild();
    assertTrue(mergeSortNode instanceof MergeSortNode);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(mergeSortNode.getChildren().get(0).getChildren().get(0) instanceof SortNode);
    assertScanNodeLimitValue(
        plan.getInstances().get(0).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree(), LIMIT_VALUE);
    assertScanNodeLimitValue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree(), LIMIT_VALUE);
  }

  /*
   * IdentitySinkNode-38
   *   └──LimitNode-33
   *       └──TransformNode-12
   *           └──MergeSort-32
   *               ├──DeviceView-19
   *               │   └──SortNode-18
   *               │       └──FilterNode-17
   *               │           └──FullOuterTimeJoinNode-16
   *               │               ├──SeriesScanNode-14:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │               └──SeriesScanNode-15:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──ExchangeNode-34: [SourceAddress:192.0.3.1/test.2.0/36]
   *               └──ExchangeNode-35: [SourceAddress:192.0.2.1/test.3.0/37]
   *
   *  IdentitySinkNode-36
   *   └──DeviceView-25
   *       └──SortNode-24
   *           └──FilterNode-23
   *               └──FullOuterTimeJoinNode-22
   *                   ├──SeriesScanNode-20:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *                   └──SeriesScanNode-21:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-37
   *   └──DeviceView-31
   *       └──SortNode-30
   *           └──FilterNode-29
   *               └──FullOuterTimeJoinNode-28
   *                   ├──SeriesScanNode-26:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *                   └──SeriesScanNode-27:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   */
  @Test
  public void orderByDeviceTest3() {
    // order by device, expression; with value filter
    sql =
        "select s1 from root.sg.d1, root.sg.d22 WHERE s2=1 order by device asc, s2 desc LIMIT 5 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof LimitNode);
    PlanNode transformNode = ((LimitNode) firstFiRoot.getChildren().get(0)).getChild();
    assertTrue(transformNode instanceof TransformNode);
    assertTrue(transformNode.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(transformNode.getChildren().get(0).getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        transformNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SortNode);
  }

  /*
   * IdentitySinkNode-27
   *   └──LimitNode-22
   *       └──FilterNode-12
   *           └──DeviceView-14
   *               ├──AggregationNode-5
   *               │   └──FilterNode-4
   *               │       └──FullOuterTimeJoinNode-3
   *               │           ├──SeriesScanNode-15:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │           ├──SeriesScanNode-17:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │           └──ExchangeNode-23: [SourceAddress:192.0.2.1/test.2.0/25]
   *               └──ExchangeNode-24: [SourceAddress:192.0.3.1/test.3.0/26]
   *
   *  IdentitySinkNode-25
   *   └──FullOuterTimeJoinNode-19
   *       ├──SeriesScanNode-16:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *       └──SeriesScanNode-18:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-26
   *   └──AggregationNode-10
   *       └──FilterNode-9
   *           └──FullOuterTimeJoinNode-8
   *               ├──SeriesScanNode-20:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-21:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   */
  @Test
  public void orderByDeviceTest4() {
    // aggregation + order by device, expression; with value filter
    sql =
        "select count(s1) from root.sg.d1, root.sg.d22 WHERE s2=1 having(count(s1)>1) LIMIT 5 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof LimitNode);
    PlanNode filterNode = ((LimitNode) firstFiRoot.getChildren().get(0)).getChild();
    assertTrue(filterNode instanceof FilterNode);
    assertTrue(filterNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(filterNode.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        filterNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    PlanNode thirdFiRoot = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(thirdFiRoot instanceof IdentitySinkNode);
    assertTrue(thirdFiRoot.getChildren().get(0) instanceof AggregationNode);
    assertTrue(thirdFiRoot.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
  }

  @Test
  public void orderByTimeNoValueFilterTest() {
    // 1. order by time, no value filter
    sql =
        String.format(
            "select * from root.sg.** ORDER BY TIME DESC LIMIT %s align by device", LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
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
    // put LIMIT-NODE below of SingleDeviceViewNode
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
    // no LIMIT-NODE below DeviceViewNode
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
    // no LIMIT-NODE below DeviceViewNode
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
      assertTrue(node.getChildren().get(0) instanceof FullOuterTimeJoinNode);
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
        assertEquals(limitValue, ((SeriesScanNode) node).getPushDownLimit());
      } else if (node instanceof AlignedSeriesScanNode) {
        assertEquals(limitValue, ((AlignedSeriesScanNode) node).getPushDownLimit());
      } else {
        assertScanNodeLimitValue(node, limitValue);
      }
    }
  }
}
