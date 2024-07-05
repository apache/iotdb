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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
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
  PlanNode firstFiTopNode;
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
   * IdentitySinkNode-37
   *   └──LimitNode-32
   *       └──TransformNode-12
   *           └──MergeSort-31
   *               ├──DeviceView-20
   *               │   └──SortNode-19
   *               │       └──LeftOuterTimeJoinNode-18
   *               │           ├──SeriesScanNode-16:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │           └──SeriesScanNode-17:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──ExchangeNode-33: [SourceAddress:192.0.3.1/test.2.0/35]
   *               └──ExchangeNode-34: [SourceAddress:192.0.2.1/test.3.0/36]
   *
   *  IdentitySinkNode-36
   *   └──DeviceView-25
   *       └──SortNode-24
   *           └──LeftOuterTimeJoinNode-23
   *               ├──SeriesScanNode-21:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-22:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-37
   *   └──DeviceView-30
   *       └──SortNode-29
   *           └──LeftOuterTimeJoinNode-28
   *               ├──SeriesScanNode-26:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesScanNode-27:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
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
   * IdentitySinkNode-33
   *   └──LimitNode-27
   *       └──FilterNode-12
   *           └──DeviceView-18
   *               ├──AggregationNode-5
   *               │   └──ProjectNode-15
   *               │       └──LeftOuterTimeJoinNode-14
   *               │           ├──FullOuterTimeJoinNode-19
   *               │           │   ├──SeriesScanNode-20:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │           │   └──ExchangeNode-28: [SourceAddress:192.0.2.1/test.2.0/31]
   *               │           └──FullOuterTimeJoinNode-22
   *               │               ├──SeriesScanNode-23:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │               └──ExchangeNode-29: [SourceAddress:192.0.2.1/test.2.0/31]
   *               └──ExchangeNode-30: [SourceAddress:192.0.3.1/test.3.0/32]
   *
   *  IdentitySinkNode-31
   *   ├──SeriesScanNode-21:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *   └──SeriesScanNode-24:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-32
   *   └──AggregationNode-10
   *       └──ProjectNode-17
   *           └──LeftOuterTimeJoinNode-16
   *               ├──SeriesScanNode-7:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-6:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
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
    assertTrue(filterNode.getChildren().get(0) instanceof AggregationMergeSortNode);
    assertTrue(filterNode.getChildren().get(0).getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        filterNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof RawDataAggregationNode);
    PlanNode thirdFiRoot = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(thirdFiRoot instanceof IdentitySinkNode);
    assertTrue(thirdFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        thirdFiRoot.getChildren().get(0).getChildren().get(0) instanceof RawDataAggregationNode);
  }

  /*
   * IdentitySinkNode-44
   *   └──TopK-10
   *       ├──TopK-34
   *       │   ├──SingleDeviceView-17
   *       │   │   └──FullOuterTimeJoinNode-16
   *       │   │       ├──SeriesScanNode-14:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │   │       └──SeriesScanNode-15:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │   └──SingleDeviceView-29
   *       │       └──FullOuterTimeJoinNode-28
   *       │           ├──SeriesScanNode-26:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │           └──SeriesScanNode-27:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       ├──ExchangeNode-38: [SourceAddress:192.0.3.1/test.2.0/41]
   *       ├──ExchangeNode-39: [SourceAddress:192.0.2.1/test.3.0/42]
   *       └──ExchangeNode-40: [SourceAddress:192.0.4.1/test.4.0/43]
   *
   *  IdentitySinkNode-41
   *   └──TopK-36
   *       └──SingleDeviceView-25
   *           └──FullOuterTimeJoinNode-24
   *               ├──SeriesScanNode-22:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-23:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-42
   *   └──TopK-35
   *       └──SingleDeviceView-21
   *           └──FullOuterTimeJoinNode-20
   *               ├──SeriesScanNode-18:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesScanNode-19:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-43
   *   └──TopK-37
   *       └──SingleDeviceView-33
   *           └──FullOuterTimeJoinNode-32
   *               ├──SeriesScanNode-30:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *               └──SeriesScanNode-31:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByTimeTest1() {
    // only order by time, no filter
    sql =
        String.format(
            "select * from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY TIME DESC LIMIT %s align by device",
            LIMIT_VALUE);
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

  /*
   * IdentitySinkNode-66
   *   └──TopK-13
   *       ├──TopK-56
   *       │   ├──SingleDeviceView-31
   *       │   │   └──LimitNode-30
   *       │   │       └──ProjectNode-29
   *       │   │           └──LeftOuterTimeJoinNode-28
   *       │   │               ├──SeriesScanNode-26:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │   │               └──SeriesScanNode-27:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │   └──SingleDeviceView-49
   *       │       └──LimitNode-48
   *       │           └──ProjectNode-47
   *       │               └──LeftOuterTimeJoinNode-46
   *       │                   ├──SeriesScanNode-44:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │                   └──SeriesScanNode-45:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       ├──ExchangeNode-60: [SourceAddress:192.0.3.1/test.2.0/63]
   *       ├──ExchangeNode-61: [SourceAddress:192.0.2.1/test.3.0/64]
   *       └──ExchangeNode-62: [SourceAddress:192.0.4.1/test.4.0/65]
   *
   *  IdentitySinkNode-63
   *   └──TopK-58
   *       └──SingleDeviceView-43
   *           └──LimitNode-42
   *               └──ProjectNode-41
   *                   └──LeftOuterTimeJoinNode-40
   *                       ├──SeriesScanNode-38:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *                       └──SeriesScanNode-39:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-64
   *   └──TopK-57
   *       └──SingleDeviceView-37
   *           └──LimitNode-36
   *               └──ProjectNode-35
   *                   └──LeftOuterTimeJoinNode-34
   *                       ├──SeriesScanNode-32:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *                       └──SeriesScanNode-33:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-65
   *   └──TopK-59
   *       └──SingleDeviceView-55
   *           └──LimitNode-54
   *               └──ProjectNode-53
   *                   └──LeftOuterTimeJoinNode-52
   *                       ├──SeriesScanNode-50:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *                       └──SeriesScanNode-51:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByTimeTest2() {
    // only order by time, has value filter
    // put LIMIT-NODE below of SingleDeviceViewNode
    sql =
        String.format(
            "select s1 from root.sg.d1,root.sg.d22,root.sg.d333 where s2>1 ORDER BY TIME DESC LIMIT %s align by device",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    PlanNode firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof SingleDeviceViewNode);
      assertTrue(node.getChildren().get(0) instanceof LimitNode);
      assertTrue(node.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(plan.getInstances().get(0).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(1).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(2).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(3).getFragment().getPlanNodeTree(), 0);
  }

  /*
   * IdentitySinkNode-41
   *   └──TopK-10
   *       ├──TopK-31
   *       │   └──DeviceView-18
   *       │       ├──FullOuterTimeJoinNode-14
   *       │       │   ├──SeriesScanNode-12:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       │   └──SeriesScanNode-13:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       └──FullOuterTimeJoinNode-17
   *       │           ├──SeriesScanNode-15:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │           └──SeriesScanNode-16:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       ├──ExchangeNode-35: [SourceAddress:192.0.3.1/test.6.0/38]
   *       ├──ExchangeNode-36: [SourceAddress:192.0.2.1/test.7.0/39]
   *       └──ExchangeNode-37: [SourceAddress:192.0.4.1/test.8.0/40]
   *
   *  IdentitySinkNode-38
   *   └──TopK-32
   *       └──DeviceView-22
   *           └──FullOuterTimeJoinNode-21
   *               ├──SeriesScanNode-19:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-20:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-39
   *   └──TopK-33
   *       └──DeviceView-26
   *           └──FullOuterTimeJoinNode-25
   *               ├──SeriesScanNode-23:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesScanNode-24:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-40
   *   └──TopK-34
   *       └──DeviceView-30
   *           └──FullOuterTimeJoinNode-29
   *               ├──SeriesScanNode-27:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *               └──SeriesScanNode-28:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByTimeTest3() {
    // order by time and expression, no filter
    // need read all data, use DeviceViewNode instead of SingleDeviceViewNode
    // no LimitNode below DeviceViewNode
    // can push down LIMIT value to ScanNode
    sql =
        String.format(
            "select s1 from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY TIME DESC, s2 DESC LIMIT %s align by device",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
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

    // order by time and expression, has value filter
    // need read all data, use DeviceViewNode instead of SingleDeviceViewNode
    // has LimitNode below DeviceViewNode
    // can not push down LIMIT value to ScanNode
    sql =
        String.format(
            "select s1 from root.sg.d1,root.sg.d22,root.sg.d333 where s2>1 ORDER BY TIME DESC, s2 DESC LIMIT %s align by device",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof DeviceViewNode);
      assertTrue(node.getChildren().get(0) instanceof LimitNode);
      assertTrue(node.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
      assertTrue(
          node.getChildren().get(0).getChildren().get(0).getChildren().get(0)
              instanceof LeftOuterTimeJoinNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    assertScanNodeLimitValue(plan.getInstances().get(0).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(1).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(2).getFragment().getPlanNodeTree(), 0);
    assertScanNodeLimitValue(plan.getInstances().get(3).getFragment().getPlanNodeTree(), 0);
  }

  /*
   * IdentitySinkNode-21
   *   └──MergeSort-8
   *       ├──SingleDeviceView-5
   *       │   └──AggregationNode-9
   *       │       ├──SeriesAggregationScanNode-10:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       └──ExchangeNode-15: [SourceAddress:192.0.2.1/test.2.0/18]
   *       ├──ExchangeNode-17: [SourceAddress:192.0.3.1/test.3.0/19]
   *       └──SingleDeviceView-7
   *           └──AggregationNode-12
   *               ├──SeriesAggregationScanNode-13:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               └──ExchangeNode-16: [SourceAddress:192.0.4.1/test.4.0/20]
   *
   * ShuffleSinkNode-18
   *   └──SeriesAggregationScanNode-11:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * ShuffleSinkNode-19
   *   └──SingleDeviceView-6
   *       └──SeriesAggregationScanNode-2:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * ShuffleSinkNode-20
   *   └──SeriesAggregationScanNode-14:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByTimeTest4() {
    // aggregation + order by time, no LIMIT
    // SingleDeviceViewNode + MergeSortNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY TIME DESC align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertEquals(3, firstFiTopNode.getChildren().size());
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof SingleDeviceViewNode);

    // aggregation + order by time + group by time, no LIMIT
    // SingleDeviceViewNode + MergeSortNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 group by ((1,10], 1ms) ORDER BY TIME DESC align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertEquals(3, firstFiTopNode.getChildren().size());
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof SingleDeviceViewNode);
  }

  /*
   * IdentitySinkNode-22
   *   └──TopK-4
   *       ├──TopK-16
   *       │   ├──SingleDeviceView-5
   *       │   │   └──AggregationNode-8
   *       │   │       ├──SeriesAggregationScanNode-9:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │   │       └──ExchangeNode-14: [SourceAddress:192.0.2.1/test.2.0/19]
   *       │   └──SingleDeviceView-7
   *       │       └──AggregationNode-11
   *       │           ├──SeriesAggregationScanNode-12:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │           └──ExchangeNode-15: [SourceAddress:192.0.4.1/test.3.0/20]
   *       └──ExchangeNode-18: [SourceAddress:192.0.3.1/test.4.0/21]
   *
   * IdentitySinkNode-19
   *   └──SeriesAggregationScanNode-10:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-20
   *   └──SeriesAggregationScanNode-13:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *
   * IdentitySinkNode-21
   *   └──TopK-17
   *       └──SingleDeviceView-6
   *           └──SeriesAggregationScanNode-2:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   */
  @Test
  public void orderByTimeTest5() {
    // aggregation + order by time, has LIMIT
    // SingleDeviceViewNode + TopKNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY TIME DESC LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    assertEquals(2, firstFiTopNode.getChildren().size());
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        plan.getInstances().get(3).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof TopKNode);

    // aggregation + order by time + group by time, has LIMIT
    // SingleDeviceViewNode + TopKNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 group by ((1,10], 1ms) ORDER BY TIME DESC LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        plan.getInstances().get(3).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof TopKNode);
  }

  /*
   * IdentitySinkNode-24
   *   └──LimitNode-17
   *       └──FilterNode-8
   *           └──MergeSort-10
   *               ├──SingleDeviceView-5
   *               │   └──AggregationNode-11
   *               │       ├──SeriesAggregationScanNode-12:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │       └──ExchangeNode-18: [SourceAddress:192.0.2.1/test.2.0/21]
   *               ├──ExchangeNode-20: [SourceAddress:192.0.3.1/test.3.0/22]
   *               └──SingleDeviceView-7
   *                   └──AggregationNode-14
   *                       ├──SeriesAggregationScanNode-15:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                       └──ExchangeNode-19: [SourceAddress:192.0.4.1/test.4.0/23]
   *
   * IdentitySinkNode-21
   *   └──SeriesAggregationScanNode-13:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-22
   *   └──SingleDeviceView-6
   *       └──SeriesAggregationScanNode-2:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-23
   *   └──SeriesAggregationScanNode-16:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByTimeTest6() {
    // aggregation + order by time + having, has LIMIT
    // SingleDeviceViewNode + MergeSortNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 having(count(s1)>1) ORDER BY TIME DESC  LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof LimitNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof FilterNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof MergeSortNode);
    assertEquals(3, firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().size());
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SingleDeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(2)
            instanceof SingleDeviceViewNode);
    assertTrue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SingleDeviceViewNode);
    assertTrue(
        plan.getInstances().get(3).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SeriesAggregationScanNode);
  }

  @Test
  public void orderByTimeWithOffsetTest() {
    // order by time, offset + limit
    // on top of TOP-K NODE, LIMIT-NODE is necessary
    sql =
        String.format(
            "select * from root.sg.** ORDER BY time DESC OFFSET %s LIMIT %s align by device",
            LIMIT_VALUE, LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
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
        plan.getInstances().get(0).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
    assertScanNodeLimitValue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
    assertScanNodeLimitValue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
    assertScanNodeLimitValue(
        plan.getInstances().get(3).getFragment().getPlanNodeTree(), LIMIT_VALUE * 2);
  }

  /*
   * IdentitySinkNode-43
   *   └──TransformNode-12
   *       └──MergeSort-32
   *           ├──SortNode-33
   *           │   └──DeviceView-19
   *           │       ├──FullOuterTimeJoinNode-15
   *           │       │   ├──SeriesScanNode-13:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │       │   └──SeriesScanNode-14:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │       └──FullOuterTimeJoinNode-18
   *           │           ├──SeriesScanNode-16:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │           └──SeriesScanNode-17:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           ├──ExchangeNode-37: [SourceAddress:192.0.3.1/test.2.0/40]
   *           ├──ExchangeNode-38: [SourceAddress:192.0.2.1/test.3.0/41]
   *           └──ExchangeNode-39: [SourceAddress:192.0.4.1/test.4.0/42]
   *
   * IdentitySinkNode-40
   *   └──SortNode-34
   *       └──DeviceView-23
   *           └──FullOuterTimeJoinNode-22
   *               ├──SeriesScanNode-20:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-21:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-41
   *   └──SortNode-35
   *       └──DeviceView-27
   *           └──FullOuterTimeJoinNode-26
   *               ├──SeriesScanNode-24:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesScanNode-25:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-42
   *   └──SortNode-36
   *       └──DeviceView-31
   *           └──FullOuterTimeJoinNode-30
   *               ├──SeriesScanNode-28:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *               └──SeriesScanNode-29:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByExpressionTest1() {
    // only order by expression, no LIMIT
    // use MergeSortNode + SortNode + TransformNode
    sql = "select s1 from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY s2 DESC align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TransformNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof SortNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof DeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(3) instanceof ExchangeNode);
    for (int i = 1; i < 4; i++) {
      assertTrue(
          plan.getInstances().get(i).getFragment().getPlanNodeTree().getChildren().get(0)
              instanceof SortNode);
      assertTrue(
          plan.getInstances()
                  .get(i)
                  .getFragment()
                  .getPlanNodeTree()
                  .getChildren()
                  .get(0)
                  .getChildren()
                  .get(0)
              instanceof DeviceViewNode);
    }
    for (int i = 0; i < 4; i++) {
      assertScanNodeLimitValue(plan.getInstances().get(i).getFragment().getPlanNodeTree(), 0);
    }
  }

  /*
   * IdentitySinkNode-41
   *   └──TopK-10
   *       ├──TopK-31
   *       │   └──DeviceView-18
   *       │       ├──FullOuterTimeJoinNode-14
   *       │       │   ├──SeriesScanNode-12:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       │   └──SeriesScanNode-13:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       └──FullOuterTimeJoinNode-17
   *       │           ├──SeriesScanNode-15:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │           └──SeriesScanNode-16:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       ├──ExchangeNode-35: [SourceAddress:192.0.3.1/test.2.0/38]
   *       ├──ExchangeNode-36: [SourceAddress:192.0.2.1/test.3.0/39]
   *       └──ExchangeNode-37: [SourceAddress:192.0.4.1/test.4.0/40]
   *
   * IdentitySinkNode-38
   *   └──TopK-32
   *       └──DeviceView-22
   *           └──FullOuterTimeJoinNode-21
   *               ├──SeriesScanNode-19:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-20:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-39
   *   └──TopK-33
   *       └──DeviceView-26
   *           └──FullOuterTimeJoinNode-25
   *               ├──SeriesScanNode-23:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesScanNode-24:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-40
   *   └──TopK-34
   *       └──DeviceView-30
   *           └──FullOuterTimeJoinNode-29
   *               ├──SeriesScanNode-27:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *               └──SeriesScanNode-28:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByExpressionTest2() {
    // only order by expression, has LIMIT
    // use TopKNode
    sql =
        "select s1 from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY s2 DESC LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    for (PlanNode node : firstFiTopNode.getChildren().get(0).getChildren()) {
      assertTrue(node instanceof DeviceViewNode);
      assertTrue(node.getChildren().get(0) instanceof FullOuterTimeJoinNode);
    }
    assertTrue(firstFiTopNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(2) instanceof ExchangeNode);
    assertTrue(firstFiTopNode.getChildren().get(3) instanceof ExchangeNode);
    for (int i = 0; i < 4; i++) {
      assertScanNodeLimitValue(plan.getInstances().get(i).getFragment().getPlanNodeTree(), 0);
    }
  }

  /* BEFORE:
   * IdentitySinkNode-35
   *   └──TransformNode-12
   *       └──SortNode-11
   *           └──DeviceView-13
   *               ├──AggregationNode-18
   *               │   ├──SeriesAggregationScanNode-14:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │   ├──SeriesAggregationScanNode-16:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │   └──ExchangeNode-29: [SourceAddress:192.0.2.1/test.2.0/32]
   *               ├──ExchangeNode-31: [SourceAddress:192.0.3.1/test.3.0/33]
   *               └──AggregationNode-27
   *                   ├──SeriesAggregationScanNode-23:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                   ├──SeriesAggregationScanNode-25:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                   └──ExchangeNode-30: [SourceAddress:192.0.4.1/test.4.0/34]
   *
   * IdentitySinkNode-32
   *   └──HorizontallyConcatNode-19
   *       ├──SeriesAggregationScanNode-15:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *       └──SeriesAggregationScanNode-17:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-33
   *   └──HorizontallyConcatNode-22
   *       ├──SeriesAggregationScanNode-20:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *       └──SeriesAggregationScanNode-21:[SeriesPath: root.sg.d22.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-34
   *   └──HorizontallyConcatNode-28
   *       ├──SeriesAggregationScanNode-24:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *       └──SeriesAggregationScanNode-26:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *
   * AFTER:
   * IdentitySinkNode-43
   *   └──TransformNode-12
   *       └──MergeSort-32
   *           ├──SortNode-33
   *           │   └──DeviceView-19
   *           │       ├──FullOuterTimeJoinNode-15
   *           │       │   ├──SeriesAggregationScanNode-13:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │       │   └──SeriesAggregationScanNode-14:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │       └──FullOuterTimeJoinNode-18
   *           │           ├──SeriesAggregationScanNode-16:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │           └──SeriesAggregationScanNode-17:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           ├──ExchangeNode-37: [SourceAddress:192.0.3.1/test.2.0/40]
   *           ├──ExchangeNode-38: [SourceAddress:192.0.2.1/test.3.0/41]
   *           └──ExchangeNode-39: [SourceAddress:192.0.4.1/test.4.0/42]
   *
   *  IdentitySinkNode-40
   *   └──SortNode-34
   *       └──DeviceView-23
   *           └──FullOuterTimeJoinNode-22
   *               ├──SeriesAggregationScanNode-20:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesAggregationScanNode-21:[SeriesPath: root.sg.d22.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-41
   *   └──SortNode-35
   *       └──DeviceView-27
   *           └──FullOuterTimeJoinNode-26
   *               ├──SeriesAggregationScanNode-24:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesAggregationScanNode-25:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   */
  @Test
  public void orderByExpressionTest3() {
    // aggregation, order by expression, no LIMIT
    // use MergeSortNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY count(s2) DESC align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TransformNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof SortNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof DeviceViewNode);
    assertTrue(
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    for (int i = 0; i < 4; i++) {
      assertScanNodeLimitValue(plan.getInstances().get(i).getFragment().getPlanNodeTree(), 0);
    }
  }

  /*
   *  BEFORE:
   *  IdentitySinkNode-34
   *   └──TopK-10
   *       └──DeviceView-12
   *           ├──AggregationNode-17
   *           │   ├──SeriesAggregationScanNode-13:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │   ├──SeriesAggregationScanNode-15:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *           │   └──ExchangeNode-28: [SourceAddress:192.0.2.1/test.2.0/31]
   *           ├──ExchangeNode-30: [SourceAddress:192.0.3.1/test.3.0/32]
   *           └──AggregationNode-26
   *               ├──SeriesAggregationScanNode-22:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──SeriesAggregationScanNode-24:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               └──ExchangeNode-29: [SourceAddress:192.0.4.1/test.4.0/33]
   *
   * IdentitySinkNode-31
   *   └──HorizontallyConcatNode-18
   *       ├──SeriesAggregationScanNode-14:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *       └──SeriesAggregationScanNode-16:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-32
   *   └──HorizontallyConcatNode-21
   *       ├──SeriesAggregationScanNode-19:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *       └──SeriesAggregationScanNode-20:[SeriesPath: root.sg.d22.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-33
   *   └──HorizontallyConcatNode-27
   *       ├──SeriesAggregationScanNode-23:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *       └──SeriesAggregationScanNode-25:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */

  /* AFTER:
   * IdentitySinkNode-41
   *   └──TopK-10
   *       ├──TopK-31
   *       │   └──DeviceView-18
   *       │       ├──FullOuterTimeJoinNode-14
   *       │       │   ├──SeriesAggregationScanNode-12:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       │   └──SeriesAggregationScanNode-13:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │       └──FullOuterTimeJoinNode-17
   *       │           ├──SeriesAggregationScanNode-15:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │           └──SeriesAggregationScanNode-16:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       ├──ExchangeNode-35: [SourceAddress:192.0.3.1/test.2.0/38]
   *       ├──ExchangeNode-36: [SourceAddress:192.0.2.1/test.3.0/39]
   *       └──ExchangeNode-37: [SourceAddress:192.0.4.1/test.4.0/40]
   *
   *  IdentitySinkNode-38
   *   └──TopK-32
   *       └──DeviceView-22
   *           └──FullOuterTimeJoinNode-21
   *               ├──SeriesAggregationScanNode-19:[SeriesPath: root.sg.d22.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesAggregationScanNode-20:[SeriesPath: root.sg.d22.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   *  IdentitySinkNode-39
   *   └──TopK-33
   *       └──DeviceView-26
   *           └──FullOuterTimeJoinNode-25
   *               ├──SeriesAggregationScanNode-23:[SeriesPath: root.sg.d1.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *               └──SeriesAggregationScanNode-24:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-40
   *   └──TopK-34
   *       └──DeviceView-30
   *           └──FullOuterTimeJoinNode-29
   *               ├──SeriesAggregationScanNode-27:[SeriesPath: root.sg.d333.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *               └──SeriesAggregationScanNode-28:[SeriesPath: root.sg.d333.s1, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByExpressionTest4() {
    // aggregation, order by expression, has LIMIT
    // use TopKNode, not need MergeSortNode and LimitNode
    sql =
        "select count(s1) from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY count(s2) DESC LIMIT 10 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    assertTrue(
        firstFiTopNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FullOuterTimeJoinNode);
    for (int i = 1; i < 4; i++) {
      assertTrue(
          plan.getInstances()
                  .get(i)
                  .getFragment()
                  .getPlanNodeTree()
                  .getChildren()
                  .get(0)
                  .getChildren()
                  .get(0)
              instanceof DeviceViewNode);
    }
    for (int i = 0; i < 4; i++) {
      assertScanNodeLimitValue(plan.getInstances().get(i).getFragment().getPlanNodeTree(), 0);
    }
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
