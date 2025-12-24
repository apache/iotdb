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
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesSourceNode;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignedByDeviceTest {
  
  // Helper method to count nodes of a specific type in a plan tree
  private int countNodesOfType(PlanNode root, Class<?> nodeType) {
    if (root == null) {
      return 0;
    }
    int count = nodeType.isInstance(root) ? 1 : 0;
    for (PlanNode child : root.getChildren()) {
      count += countNodesOfType(child, nodeType);
    }
    return count;
  }
  
  // Helper method to find first node of a specific type in a plan tree
  private <T extends PlanNode> T findFirstNodeOfType(PlanNode root, Class<T> nodeType) {
    if (root == null) {
      return null;
    }
    if (nodeType.isInstance(root)) {
      return nodeType.cast(root);
    }
    for (PlanNode child : root.getChildren()) {
      T result = findFirstNodeOfType(child, nodeType);
      if (result != null) {
        return result;
      }
    }
    return null;
  }
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
    
    // Count node types across all fragments (order-independent)
    int aggMergeSortCount = 0;
    int deviceViewCount = 0;
    int exchangeCount = 0;
    int seriesSourceCount = 0;
    int seriesAggScanCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      aggMergeSortCount += countNodesOfType(root, AggregationMergeSortNode.class);
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      seriesSourceCount += countNodesOfType(root, SeriesSourceNode.class);
      seriesAggScanCount += countNodesOfType(root, SeriesAggregationScanNode.class);
    }
    assertTrue("Expected at least one AggregationMergeSortNode", aggMergeSortCount >= 1);
    assertTrue("Expected at least two DeviceViewNodes", deviceViewCount >= 2);
    assertTrue("Expected at least one ExchangeNode", exchangeCount >= 1);
    assertTrue("Expected at least two SeriesSourceNodes", seriesSourceCount >= 2);
    assertTrue("Expected at least one SeriesAggregationScanNode", seriesAggScanCount >= 1);

    // test of MULTI_SERIES
    sql = "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    
    // Count node types across all fragments (order-independent)
    aggMergeSortCount = 0;
    deviceViewCount = 0;
    exchangeCount = 0;
    int projectCount = 0;
    int fullOuterJoinCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      aggMergeSortCount += countNodesOfType(root, AggregationMergeSortNode.class);
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      projectCount += countNodesOfType(root, ProjectNode.class);
      fullOuterJoinCount += countNodesOfType(root, FullOuterTimeJoinNode.class);
    }
    assertTrue("Expected at least one AggregationMergeSortNode", aggMergeSortCount >= 1);
    assertTrue("Expected at least two DeviceViewNodes", deviceViewCount >= 2);
    assertTrue("Expected at least one ExchangeNode", exchangeCount >= 1);
    assertTrue("Expected at least two ProjectNodes", projectCount >= 2);
    assertTrue("Expected at least two FullOuterTimeJoinNodes", fullOuterJoinCount >= 2);
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
    
    // Count node types across all fragments (order-independent)
    int aggMergeSortCount = 0;
    int deviceViewCount = 0;
    int seriesAggScanCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      aggMergeSortCount += countNodesOfType(root, AggregationMergeSortNode.class);
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      seriesAggScanCount += countNodesOfType(root, SeriesAggregationScanNode.class);
    }
    assertTrue("Expected at least one AggregationMergeSortNode", aggMergeSortCount >= 1);
    assertTrue("Expected at least two DeviceViewNodes", deviceViewCount >= 2);
    assertTrue("Expected at least three SeriesAggregationScanNodes", seriesAggScanCount >= 3);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 where s1 <= 4 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    
    // Count node types across all fragments (order-independent)
    aggMergeSortCount = 0;
    deviceViewCount = 0;
    int rawDataAggCount = 0;
    int leftOuterJoinCount = 0;
    int seriesSourceCount = 0;
    int exchangeCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      aggMergeSortCount += countNodesOfType(root, AggregationMergeSortNode.class);
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      rawDataAggCount += countNodesOfType(root, RawDataAggregationNode.class);
      leftOuterJoinCount += countNodesOfType(root, LeftOuterTimeJoinNode.class);
      seriesSourceCount += countNodesOfType(root, SeriesSourceNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
    }
    assertTrue("Expected at least one AggregationMergeSortNode", aggMergeSortCount >= 1);
    assertTrue("Expected at least two DeviceViewNodes", deviceViewCount >= 2);
    assertTrue("Expected at least two RawDataAggregationNodes", rawDataAggCount >= 2);
    assertTrue("Expected at least one LeftOuterTimeJoinNode", leftOuterJoinCount >= 1);
    assertTrue("Expected at least one SeriesSourceNode", seriesSourceCount >= 1);
    assertTrue("Expected at least one ExchangeNode", exchangeCount >= 1);
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
    
    // Count node types across all fragments (order-independent)
    int mergeSortCount = 0;
    int singleDeviceViewCount = 0;
    int aggNodeCount = 0;
    int seriesSourceCount = 0;
    int exchangeCount = 0;
    int shuffleSinkCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      mergeSortCount += countNodesOfType(root, MergeSortNode.class);
      singleDeviceViewCount += countNodesOfType(root, SingleDeviceViewNode.class);
      aggNodeCount += countNodesOfType(root, AggregationNode.class);
      seriesSourceCount += countNodesOfType(root, SeriesSourceNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      shuffleSinkCount += countNodesOfType(root, ShuffleSinkNode.class);
    }
    assertTrue("Expected at least one MergeSortNode", mergeSortCount >= 1);
    assertTrue("Expected at least two SingleDeviceViewNodes", singleDeviceViewCount >= 2);
    assertTrue("Expected at least two AggregationNodes", aggNodeCount >= 2);
    assertTrue("Expected at least two SeriesSourceNodes", seriesSourceCount >= 2);
    assertTrue("Expected at least two ExchangeNodes", exchangeCount >= 2);
    assertTrue("Expected at least one ShuffleSinkNode", shuffleSinkCount >= 1);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    
    // Count node types across all fragments (order-independent)
    mergeSortCount = 0;
    singleDeviceViewCount = 0;
    aggNodeCount = 0;
    seriesSourceCount = 0;
    exchangeCount = 0;
    shuffleSinkCount = 0;
    int projectCount = 0;
    int hConcatCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      mergeSortCount += countNodesOfType(root, MergeSortNode.class);
      singleDeviceViewCount += countNodesOfType(root, SingleDeviceViewNode.class);
      aggNodeCount += countNodesOfType(root, AggregationNode.class);
      seriesSourceCount += countNodesOfType(root, SeriesSourceNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      shuffleSinkCount += countNodesOfType(root, ShuffleSinkNode.class);
      projectCount += countNodesOfType(root, ProjectNode.class);
      hConcatCount += countNodesOfType(root, HorizontallyConcatNode.class);
    }
    assertTrue("Expected at least one MergeSortNode", mergeSortCount >= 1);
    assertTrue("Expected at least two SingleDeviceViewNodes", singleDeviceViewCount >= 2);
    assertTrue("Expected at least two AggregationNodes", aggNodeCount >= 2);
    assertTrue("Expected at least four SeriesSourceNodes", seriesSourceCount >= 4);
    assertTrue("Expected at least two ExchangeNodes", exchangeCount >= 2);
    assertTrue("Expected at least one ShuffleSinkNode", shuffleSinkCount >= 1);
    assertTrue("Expected at least two ProjectNodes", projectCount >= 2);
    assertTrue("Expected at least two HorizontallyConcatNodes", hConcatCount >= 2);
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
    
    // Count node types across all fragments (order-independent)
    int mergeSortCount = 0;
    int singleDeviceViewCount = 0;
    int aggNodeCount = 0;
    int seriesAggScanCount = 0;
    int exchangeCount = 0;
    int shuffleSinkCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      mergeSortCount += countNodesOfType(root, MergeSortNode.class);
      singleDeviceViewCount += countNodesOfType(root, SingleDeviceViewNode.class);
      aggNodeCount += countNodesOfType(root, AggregationNode.class);
      seriesAggScanCount += countNodesOfType(root, SeriesAggregationScanNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      shuffleSinkCount += countNodesOfType(root, ShuffleSinkNode.class);
    }
    assertTrue("Expected at least one MergeSortNode", mergeSortCount >= 1);
    assertTrue("Expected at least two SingleDeviceViewNodes", singleDeviceViewCount >= 2);
    assertTrue("Expected at least two AggregationNodes", aggNodeCount >= 2);
    assertTrue("Expected at least two SeriesAggregationScanNodes", seriesAggScanCount >= 2);
    assertTrue("Expected at least two ExchangeNodes", exchangeCount >= 2);
    assertTrue("Expected at least one ShuffleSinkNode", shuffleSinkCount >= 1);

    // test of MULTI_SERIES
    sql =
        "select count(s1),count(s2) from root.sg.d333,root.sg.d4444 where s1 <= 4 order by time align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    
    // Count node types across all fragments (order-independent)
    mergeSortCount = 0;
    singleDeviceViewCount = 0;
    int rawDataAggCount = 0;
    int leftOuterJoinCount = 0;
    int fullOuterJoinCount = 0;
    int seriesSourceCount = 0;
    exchangeCount = 0;
    shuffleSinkCount = 0;
    int seriesScanCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      mergeSortCount += countNodesOfType(root, MergeSortNode.class);
      singleDeviceViewCount += countNodesOfType(root, SingleDeviceViewNode.class);
      rawDataAggCount += countNodesOfType(root, RawDataAggregationNode.class);
      leftOuterJoinCount += countNodesOfType(root, LeftOuterTimeJoinNode.class);
      fullOuterJoinCount += countNodesOfType(root, FullOuterTimeJoinNode.class);
      seriesSourceCount += countNodesOfType(root, SeriesSourceNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      shuffleSinkCount += countNodesOfType(root, ShuffleSinkNode.class);
      seriesScanCount += countNodesOfType(root, SeriesScanNode.class);
    }
    assertTrue("Expected at least one MergeSortNode", mergeSortCount >= 1);
    assertTrue("Expected at least two SingleDeviceViewNodes", singleDeviceViewCount >= 2);
    assertTrue("Expected at least two RawDataAggregationNodes", rawDataAggCount >= 2);
    assertTrue("Expected at least two LeftOuterTimeJoinNodes", leftOuterJoinCount >= 2);
    assertTrue("Expected at least two FullOuterTimeJoinNodes", fullOuterJoinCount >= 2);
    assertTrue("Expected at least two SeriesSourceNodes", seriesSourceCount >= 2);
    assertTrue("Expected at least two ExchangeNodes", exchangeCount >= 2);
    assertTrue("Expected at least one ShuffleSinkNode", shuffleSinkCount >= 1);
    assertTrue("Expected at least four SeriesScanNodes", seriesScanCount >= 4);
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
    assertTrue(f1Root.getChildren().get(0) instanceof AggregationMergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof SeriesAggregationScanNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof DeviceViewNode);

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
    assertTrue(f1Root.getChildren().get(0) instanceof AggregationMergeSortNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FullOuterTimeJoinNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ProjectNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof FullOuterTimeJoinNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof ExchangeNode);
    assertTrue(f2Root instanceof IdentitySinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(f3Root instanceof IdentitySinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof DeviceViewNode);
  }

  /*
   * IdentitySinkNode-28
   *   └──AggregationMergeSort-23
   *       ├──DeviceView-14
   *       │   ├──AggregationNode-10
   *       │   │   └──FilterNode-9
   *       │   │       └──SeriesScanNode-8:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       │   └──AggregationNode-13
   *       │       └──FilterNode-12
   *       │           └──SeriesScanNode-11:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *       ├──ExchangeNode-24: [SourceAddress:192.0.2.1/test.2.0/26]
   *       └──ExchangeNode-25: [SourceAddress:192.0.4.1/test.3.0/27]
   *
   * IdentitySinkNode-26
   *   └──DeviceView-18
   *       └──AggregationNode-17
   *           └──FilterNode-16
   *               └──SeriesScanNode-15:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   * IdentitySinkNode-27
   *   └──DeviceView-22
   *       └──AggregationNode-21
   *           └──FilterNode-20
   *               └──SeriesScanNode-19:[SeriesPath: root.sg.d333.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:4)]
   */
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
    
    // Count node types across all fragments (order-independent)
    int aggMergeSortCount = 0;
    int deviceViewCount = 0;
    int seriesAggScanCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      aggMergeSortCount += countNodesOfType(root, AggregationMergeSortNode.class);
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      seriesAggScanCount += countNodesOfType(root, SeriesAggregationScanNode.class);
    }
    assertTrue("Expected at least one AggregationMergeSortNode", aggMergeSortCount >= 1);
    assertTrue("Expected at least three DeviceViewNodes", deviceViewCount >= 3);
    assertTrue("Expected at least one SeriesAggregationScanNode", seriesAggScanCount >= 1);

    // test of MULTI_SERIES
    sql = "select count(s1),count(s2) from root.sg.d1,root.sg.d333 where s1 <= 4 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    
    // Count node types across all fragments (order-independent)
    aggMergeSortCount = 0;
    deviceViewCount = 0;
    int rawDataAggCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      aggMergeSortCount += countNodesOfType(root, AggregationMergeSortNode.class);
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      rawDataAggCount += countNodesOfType(root, RawDataAggregationNode.class);
    }
    assertTrue("Expected at least one AggregationMergeSortNode", aggMergeSortCount >= 1);
    assertTrue("Expected at least three DeviceViewNodes", deviceViewCount >= 3);
    assertTrue("Expected at least two RawDataAggregationNodes", rawDataAggCount >= 2);
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
            instanceof ProjectNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
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
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof ProjectNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof AggregationNode);
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
            instanceof SeriesAggregationScanNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof AggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof SeriesAggregationScanNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesAggregationScanNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesAggregationScanNode);

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
            instanceof RawDataAggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof LeftOuterTimeJoinNode);
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
            instanceof FullOuterTimeJoinNode);
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
            instanceof FullOuterTimeJoinNode);
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
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f1Root.getChildren().get(0).getChildren().get(1) instanceof SingleDeviceViewNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0)
            instanceof RawDataAggregationNode);
    assertTrue(
        f1Root.getChildren().get(0).getChildren().get(1).getChildren().get(0).getChildren().get(0)
            instanceof LeftOuterTimeJoinNode);
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
            instanceof FullOuterTimeJoinNode);
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
            instanceof FullOuterTimeJoinNode);
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
                .getChildren()
                .get(1)
            instanceof ExchangeNode);
    assertTrue(f2Root instanceof ShuffleSinkNode);
    assertTrue(f2Root.getChildren().get(0) instanceof SeriesScanNode);
    assertTrue(f2Root.getChildren().get(1) instanceof SeriesScanNode);
    assertTrue(f3Root instanceof ShuffleSinkNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesScanNode);
    assertTrue(f3Root.getChildren().get(1) instanceof SeriesScanNode);
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
    
    // Count node types across all fragments (order-independent)
    int deviceViewCount = 0;
    int fullOuterJoinCount = 0;
    int transformCount = 0;
    int exchangeCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      fullOuterJoinCount += countNodesOfType(root, FullOuterTimeJoinNode.class);
      transformCount += countNodesOfType(root, TransformNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
    }
    assertTrue("Expected at least one DeviceViewNode", deviceViewCount >= 1);
    assertTrue("Expected at least one FullOuterTimeJoinNode", fullOuterJoinCount >= 1);
    assertTrue("Expected at least one TransformNode", transformCount >= 1);
    assertTrue("Expected at least one ExchangeNode", exchangeCount >= 1);
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
    assertTrue(f2Root.getChildren().get(0) instanceof FullOuterTimeJoinNode);
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
    
    // Count node types across all fragments (order-independent)
    int deviceViewCount = 0;
    int fullOuterJoinCount = 0;
    int transformCount = 0;
    int exchangeCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      deviceViewCount += countNodesOfType(root, DeviceViewNode.class);
      fullOuterJoinCount += countNodesOfType(root, FullOuterTimeJoinNode.class);
      transformCount += countNodesOfType(root, TransformNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
    }
    assertTrue("Expected at least one DeviceViewNode", deviceViewCount >= 1);
    assertTrue("Expected at least two FullOuterTimeJoinNodes", fullOuterJoinCount >= 2);
    assertTrue("Expected at least two TransformNodes", transformCount >= 2);
    assertTrue("Expected at least one ExchangeNode", exchangeCount >= 1);
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
    
    // Count node types across all fragments (order-independent)
    int mergeSortCount = 0;
    int singleDeviceViewCount = 0;
    int fullOuterJoinCount = 0;
    int transformCount = 0;
    int exchangeCount = 0;
    int shuffleSinkCount = 0;
    for (FragmentInstance instance : plan.getInstances()) {
      PlanNode root = instance.getFragment().getPlanNodeTree();
      mergeSortCount += countNodesOfType(root, MergeSortNode.class);
      singleDeviceViewCount += countNodesOfType(root, SingleDeviceViewNode.class);
      fullOuterJoinCount += countNodesOfType(root, FullOuterTimeJoinNode.class);
      transformCount += countNodesOfType(root, TransformNode.class);
      exchangeCount += countNodesOfType(root, ExchangeNode.class);
      shuffleSinkCount += countNodesOfType(root, ShuffleSinkNode.class);
    }
    assertTrue("Expected at least one MergeSortNode", mergeSortCount >= 1);
    assertTrue("Expected at least two SingleDeviceViewNodes", singleDeviceViewCount >= 2);
    assertTrue("Expected at least two FullOuterTimeJoinNodes", fullOuterJoinCount >= 2);
    assertTrue("Expected at least two TransformNodes", transformCount >= 2);
    assertTrue("Expected at least one ExchangeNode", exchangeCount >= 1);
    assertTrue("Expected at least two ShuffleSinkNodes", shuffleSinkCount >= 2);
  }

  @Test
  public void testOrderByWithoutRedundantMergeSortOperator() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select * from root.sg.d1 order by time asc align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    assertTrue(logicalPlanNode instanceof DeviceViewNode);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    sql = "select * from root.sg.d22 order by time asc align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    assertTrue(logicalPlanNode instanceof DeviceViewNode);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
  }
}
