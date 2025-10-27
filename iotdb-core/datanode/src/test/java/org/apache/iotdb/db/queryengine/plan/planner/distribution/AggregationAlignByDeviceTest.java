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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationAlignByDeviceTest {
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
  PlanNode secondFiRoot;
  PlanNode firstFiTopNode;

  // ================= no scaling situation, i.e., each device only in one data region ===========

  /*
   * IdentitySinkNode-10
   *   └──MergeSort-7
   *       ├──DeviceView-5
   *       │   └──SeriesAggregationScanNode-1:[SeriesPath: root.sg.d22.s1,
   *              Descriptor: [AggregationDescriptor(first_value, SINGLE)],
   *              DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *       └──ExchangeNode-8: [SourceAddress:192.0.4.1/test.2.0/9]
   *
   *  IdentitySinkNode-9
   *   └──DeviceView-6
   *       └──SeriesAggregationScanNode-2:[SeriesPath: root.sg.d55555.s1,
   *          Descriptor: [AggregationDescriptor(first_value, SINGLE)],
   *          DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByDeviceTest1() {
    // one aggregation measurement, two devices
    sql = "select first_value(s1) from root.sg.d22, root.sg.d55555 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0)
            instanceof SeriesAggregationScanNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof IdentitySinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0)
            instanceof SeriesAggregationScanNode);

    // one aggregation measurement, one device
    sql = "select first_value(s1) from root.sg.d22 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof DeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof SeriesAggregationScanNode);

    // two aggregation measurement, two devices
    sql = "select first_value(s1), count(s2) from root.sg.d22, root.sg.d55555 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof HorizontallyConcatNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof IdentitySinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0) instanceof HorizontallyConcatNode);

    // two aggregation measurement, one device
    sql = "select first_value(s1), count(s2) from root.sg.d22 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof DeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof HorizontallyConcatNode);
  }

  /*
   * IdentitySinkNode-31
   *   └──MergeSort-28
   *       ├──DeviceView-12
   *       │   └──AggregationNode-17
   *       │       └──FilterNode-16
   *       │           └──FullOuterTimeJoinNode-15
   *       │               ├──SeriesScanNode-18:[SeriesPath: root.sg.d22.s1,
   *                          DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *       │               └──SeriesScanNode-19:[SeriesPath: root.sg.d22.s2,
   *                          DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *       └──ExchangeNode-29: [SourceAddress:192.0.4.1/test.2.0/30]
   *
   * IdentitySinkNode-30
   *   └──DeviceView-20
   *       └──AggregationNode-25
   *           └──FilterNode-24
   *               └──FullOuterTimeJoinNode-23
   *                   ├──SeriesScanNode-26:[SeriesPath: root.sg.d55555.s1,
   *                      DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *                   └──SeriesScanNode-27:[SeriesPath: root.sg.d55555.s2,
   *                      DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */

  /**
   * IdentitySinkNode-35 └──MergeSort-32 ├──DeviceView-16 │ └──AggregationNode-21 │
   * └──ProjectNode-20 │ └──LeftOuterTimeJoinNode-19 │ ├──SeriesScanNode-17:[SeriesPath:
   * root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)] │
   * └──SeriesScanNode-18:[SeriesPath: root.sg.d22.s1, DataRegion:
   * TConsensusGroupId(type:DataRegion, id:3)] └──ExchangeNode-33:
   * [SourceAddress:192.0.4.1/test.2.0/34]
   *
   * <p>IdentitySinkNode-34 └──DeviceView-24 └──AggregationNode-29 └──ProjectNode-28
   * └──LeftOuterTimeJoinNode-27 ├──SeriesScanNode-25:[SeriesPath: root.sg.d55555.s2, DataRegion:
   * TConsensusGroupId(type:DataRegion, id:4)] └──SeriesScanNode-26:[SeriesPath: root.sg.d55555.s1,
   * DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByDeviceTest2() {
    // one aggregation measurement, two devices, with filter
    sql = "select first_value(s1) from root.sg.d22, root.sg.d55555 where s2>1 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof RawDataAggregationNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof LeftOuterTimeJoinNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof IdentitySinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0) instanceof RawDataAggregationNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof LeftOuterTimeJoinNode);

    // two aggregation measurement, two devices, with filter
    sql =
        "select first_value(s1), count(s2) from root.sg.d22, root.sg.d55555 where s2>1 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof RawDataAggregationNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof LeftOuterTimeJoinNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof IdentitySinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0) instanceof RawDataAggregationNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof LeftOuterTimeJoinNode);
  }

  /*
   * IdentitySinkNode-32
   *   └──TransformNode-10
   *       └──MergeSort-27
   *           ├──SortNode-28
   *           │   └──TransformNode-25
   *           │       └──DeviceView-11
   *           │           └──HorizontallyConcatNode-17
   *           │               ├──SeriesAggregationScanNode-15:[SeriesPath: root.sg.d22.s1,
   *                              Descriptor: [AggregationDescriptor(avg, SINGLE)],
   *                              DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *           │               └──SeriesAggregationScanNode-16:[SeriesPath: root.sg.d22.s2,
   *                              Descriptor: [AggregationDescriptor(avg, SINGLE),
   *                              AggregationDescriptor(max_value, SINGLE)],
   *                              DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *           └──ExchangeNode-30: [SourceAddress:192.0.4.1/test.2.0/31]
   *
   * IdentitySinkNode-31
   *   └──SortNode-29
   *       └──TransformNode-26
   *           └──DeviceView-18
   *               └──HorizontallyConcatNode-24
   *                   ├──SeriesAggregationScanNode-22:[SeriesPath: root.sg.d55555.s1,
   *                      Descriptor: [AggregationDescriptor(avg, SINGLE)],
   *                      DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   *                   └──SeriesAggregationScanNode-23:[SeriesPath: root.sg.d55555.s2,
   *                      Descriptor: [AggregationDescriptor(avg, SINGLE), AggregationDescriptor(max_value, SINGLE)],
   *                      DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
  @Test
  public void orderByExpressionTest() {
    sql =
        "select avg(s1)+avg(s2) from root.sg.d22, root.sg.d55555 order by max_value(s2) align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof TransformNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof SortNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TransformNode);
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
            instanceof DeviceViewNode);
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
                .getChildren()
                .get(0)
            instanceof HorizontallyConcatNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof IdentitySinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof SortNode);
    assertTrue(secondFiRoot.getChildren().get(0).getChildren().get(0) instanceof TransformNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof DeviceViewNode);
    assertTrue(
        secondFiRoot
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof HorizontallyConcatNode);
  }

  @Test
  public void orderByTimeTest1() {
    // one aggregation measurement, two devices
    sql =
        "select first_value(s1) from root.sg.d22, root.sg.d55555 order by time desc align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0)
            instanceof SeriesAggregationScanNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof ShuffleSinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0)
            instanceof SeriesAggregationScanNode);

    // one aggregation measurement, one device
    sql = "select first_value(s1) from root.sg.d22 order by time desc align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof DeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof SeriesAggregationScanNode);

    // two aggregation measurement, two devices
    sql =
        "select first_value(s1), count(s2) from root.sg.d22, root.sg.d55555 "
            + "order by time desc align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof MergeSortNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof HorizontallyConcatNode);

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree();
    assertTrue(secondFiRoot instanceof ShuffleSinkNode);
    assertTrue(secondFiRoot.getChildren().get(0) instanceof SingleDeviceViewNode);
    assertTrue(secondFiRoot.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
    assertTrue(
        secondFiRoot.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof HorizontallyConcatNode);

    // two aggregation measurement, one device
    sql = "select first_value(s1), count(s2) from root.sg.d22 order by time desc align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    firstFiTopNode = firstFiRoot.getChildren().get(0);
    assertTrue(firstFiTopNode instanceof DeviceViewNode);
    assertTrue(firstFiTopNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(
        firstFiTopNode.getChildren().get(0).getChildren().get(0) instanceof HorizontallyConcatNode);
  }

  @Test
  public void crossRegionTest() {
    // one aggregation measurement, two devices
    sql = "select last_value(s1),last_value(s2)from root.sg.d1 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(firstFiRoot instanceof AggregationMergeSortNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    if (firstFiRoot.getChildren().get(0).getChildren().get(0) instanceof ProjectNode) {
      assertEquals(
          firstFiRoot.getChildren().get(0).getChildren().get(0).getOutputColumnNames(),
          ImmutableList.of(
              "last_value(root.sg.d1.s1)",
              "max_time(root.sg.d1.s1)",
              "last_value(root.sg.d1.s2)",
              "max_time(root.sg.d1.s2)"));
    }

    secondFiRoot = plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(secondFiRoot instanceof DeviceViewNode);
    if (secondFiRoot.getChildren().get(0) instanceof ProjectNode) {
      assertEquals(
          firstFiRoot.getChildren().get(0).getChildren().get(0).getOutputColumnNames(),
          ImmutableList.of(
              "last_value(root.sg.d1.s1)",
              "max_time(root.sg.d1.s1)",
              "last_value(root.sg.d1.s2)",
              "max_time(root.sg.d1.s2)"));
    }

    // order by time
    sql = "select last_value(s1),last_value(s2)from root.sg.d1 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(firstFiRoot instanceof AggregationMergeSortNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof DeviceViewNode);
    List<SortItem> sortItemList =
        ((AggregationMergeSortNode) firstFiRoot).getMergeOrderParameter().getSortItemList();
    assertEquals(sortItemList.get(0).getSortKey().toLowerCase(), "device");
    assertEquals(sortItemList.get(1).getSortKey().toLowerCase(), "time");
  }
}
