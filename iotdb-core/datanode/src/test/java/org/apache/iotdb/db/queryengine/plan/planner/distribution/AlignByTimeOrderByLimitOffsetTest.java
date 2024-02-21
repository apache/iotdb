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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignByTimeOrderByLimitOffsetTest {

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

  /*
   * IdentitySinkNode-63
   *   └──LimitNode-55
   *       └──OffsetNode-40
   *           └──TopK-62
   *               └──FullOuterTimeJoinNode-38
   *                   ├──SeriesScanNode-42:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                   ├──SeriesScanNode-46:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                   ├──SeriesScanNode-48:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                   ├──SeriesScanNode-50:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                   ├──ExchangeNode-56: [SourceAddress:192.0.3.1/test.6.0/59]
   *                   ├──ExchangeNode-57: [SourceAddress:192.0.2.1/test.7.0/60]
   *                   └──ExchangeNode-58: [SourceAddress:192.0.4.1/test.8.0/61]
   *
   * IdentitySinkNode-59
   *   └──FullOuterTimeJoinNode-52
   *       ├──SeriesScanNode-44:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *       └──SeriesScanNode-45:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-60
   *   └──FullOuterTimeJoinNode-53
   *       ├──SeriesScanNode-47:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *       └──SeriesScanNode-49:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-61
   *   └──FullOuterTimeJoinNode-54
   *       ├──SeriesScanNode-43:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *       └──SeriesScanNode-51:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByExpressionTest1() {
    // select * order by expression + limit N
    // use TopKNode to replace SortNode + LimitNode
    sql =
        String.format(
            "select * from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY root.sg.d1.s1 DESC LIMIT %s",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof FullOuterTimeJoinNode);
    for (int i = 1; i < 4; i++) {
      assertTrue(
          plan.getInstances().get(i).getFragment().getPlanNodeTree().getChildren().get(0)
              instanceof FullOuterTimeJoinNode);
    }

    // select * order by expression + offset M + limit N
    // use TopKNode to replace SortNode
    sql =
        String.format(
            "select * from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY root.sg.d1.s1 DESC OFFSET 5 LIMIT %s",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof LimitNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof OffsetNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof TopKNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FullOuterTimeJoinNode);
    for (int i = 1; i < 4; i++) {
      assertTrue(
          plan.getInstances().get(i).getFragment().getPlanNodeTree().getChildren().get(0)
              instanceof FullOuterTimeJoinNode);
    }
  }

  /*
   * IdentitySinkNode-33
   *   └──LimitNode-27
   *       └──OffsetNode-22
   *           └──TransformNode-21
   *               └──TopK-32
   *                   └──FullOuterTimeJoinNode-19
   *                       ├──SeriesScanNode-25:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *                       ├──ExchangeNode-28: [SourceAddress:192.0.3.1/test.5.0/30]
   *                       └──ExchangeNode-29: [SourceAddress:192.0.2.1/test.6.0/31]
   *
   * IdentitySinkNode-30
   *   └──SeriesScanNode-24:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-31
   *   └──SeriesScanNode-26:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   */
  @Test
  public void orderByExpressionTest2() {
    // select s1 order by s2 + limit N
    // use TopKNode to replace SortNode + LimitNode
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
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof TopKNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof FullOuterTimeJoinNode);

    // select s1 order by s2 + offset M + limit N
    // use TopKNode to replace SortNode
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
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof TransformNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TopKNode);
  }

  /*
   * IdentitySinkNode-17
   *   └──TransformNode-5
   *       └──TopK-16
   *           └──AggregationNode-10
   *               ├──SeriesAggregationScanNode-7:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──ExchangeNode-12: [SourceAddress:192.0.3.1/test.2.0/14]
   *               └──ExchangeNode-13: [SourceAddress:192.0.2.1/test.3.0/15]
   *
   * IdentitySinkNode-14
   *   └──SeriesAggregationScanNode-9:[SeriesPath: root.sg.d22.s2, Descriptor: [AggregationDescriptor(count, SINGLE)], DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *
   * IdentitySinkNode-15
   *   └──SeriesAggregationScanNode-8:[SeriesPath: root.sg.d1.s1, Descriptor: [AggregationDescriptor(count, PARTIAL)], DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   */
  @Test
  public void orderByExpressionTest3() {
    // aggregation, select count(root.sg.d1.s1) order by count(root.sg.d22.s2), LIMIT N
    sql =
        String.format(
            "select count(s1) from root.sg.d1 ORDER BY count(root.sg.d22.s2) DESC LIMIT %s",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TransformNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof TopKNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);

    // aggregation, select count(root.sg.d1.s1) order by count(root.sg.d22.s2), OFFSET M LIMIT N
    sql =
        String.format(
            "select count(s1) from root.sg.d1 ORDER BY count(root.sg.d22.s2) DESC OFFSET 5 LIMIT %s",
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
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof TransformNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TopKNode);
  }

  /*
   * IdentitySinkNode-32
   *   └──FillNode-9
   *       └──TopK-31
   *           └──FullOuterTimeJoinNode-7
   *               ├──SeriesScanNode-11:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──SeriesScanNode-15:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──SeriesScanNode-17:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──SeriesScanNode-19:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               ├──ExchangeNode-25: [SourceAddress:192.0.3.1/test.2.0/28]
   *               ├──ExchangeNode-26: [SourceAddress:192.0.2.1/test.3.0/29]
   *               └──ExchangeNode-27: [SourceAddress:192.0.4.1/test.4.0/30]
   */
  @Test
  public void orderByFillTest() {
    // previous and constant fill can use TopKNode
    sql =
        String.format(
            "select * from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY root.sg.d1.s1 DESC fill(previous) LIMIT %s",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof FillNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof TopKNode);
    for (int i = 1; i < 4; i++) {
      assertTrue(
          plan.getInstances().get(i).getFragment().getPlanNodeTree().getChildren().get(0)
              instanceof FullOuterTimeJoinNode);
    }

    /*
     * IdentitySinkNode-63
     *   └──LimitNode-56
     *       └──FillNode-41
     *           └──SortNode-40
     *               └──FullOuterTimeJoinNode-39
     *                   ├──SeriesScanNode-43:[SeriesPath: root.sg.d333.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
     *                   ├──SeriesScanNode-47:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
     *                   ├──SeriesScanNode-49:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
     *                   ├──SeriesScanNode-51:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
     *                   ├──ExchangeNode-57: [SourceAddress:192.0.3.1/test.6.0/60]
     *                   ├──ExchangeNode-58: [SourceAddress:192.0.2.1/test.7.0/61]
     *                   └──ExchangeNode-59: [SourceAddress:192.0.4.1/test.8.0/62]
     */
    // linear fill can not use TopKNode
    sql =
        String.format(
            "select * from root.sg.d1,root.sg.d22,root.sg.d333 ORDER BY root.sg.d1.s1 DESC fill(linear) LIMIT %s",
            LIMIT_VALUE);
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(4, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof LimitNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof FillNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof SortNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FullOuterTimeJoinNode);
    for (int i = 1; i < 4; i++) {
      assertTrue(
          plan.getInstances().get(i).getFragment().getPlanNodeTree().getChildren().get(0)
              instanceof FullOuterTimeJoinNode);
    }
  }
}
