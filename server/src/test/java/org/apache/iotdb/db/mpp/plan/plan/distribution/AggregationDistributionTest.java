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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationDistributionTest {

  @Test
  public void testAggregation1Series2Regions() throws IllegalPathException {
    QueryId queryId = new QueryId("test_1_series_2_regions");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1) from root.sg.d1";
    String d1s1Path = "root.sg.d1.s1";

    Analysis analysis = Util.analyze(sql, context);
    PlanNode rootNode = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, rootNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
    AggregationNode aggregationNode =
        (AggregationNode)
            fragmentInstances.get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    assertEquals(
        AggregationStep.FINAL, aggregationNode.getAggregationDescriptorList().get(0).getStep());
  }

  @Test
  public void testAggregation1Series2RegionsWithSlidingWindow() throws IllegalPathException {
    QueryId queryId = new QueryId("test_1_series_2_regions_sliding_window");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1) from root.sg.d1 group by ([0, 100), 5ms, 1ms)";
    String d1s1Path = "root.sg.d1.s1";

    Analysis analysis = Util.analyze(sql, context);
    PlanNode rootNode = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, rootNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
    AggregationNode aggregationNode =
        (AggregationNode)
            fragmentInstances
                .get(0)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(
        AggregationStep.INTERMEDIATE,
        aggregationNode.getAggregationDescriptorList().get(0).getStep());
  }

  @Test
  public void testTimeJoinAggregationSinglePerRegion() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1) from root.sg.d1, root.sg.d22";
    String d1s1Path = "root.sg.d1.s1";
    String d2s1Path = "root.sg.d22.s1";

    Analysis analysis = Util.analyze(sql, context);
    PlanNode timeJoinNode = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, timeJoinNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());

    List<FragmentInstance> fragmentInstances = plan.getInstances();
    List<AggregationStep> expected = Arrays.asList(AggregationStep.STATIC, AggregationStep.FINAL);
    verifyAggregationStep(expected, fragmentInstances.get(0).getFragment().getPlanNodeTree());

    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.SINGLE);
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
  }

  // verify SeriesAggregationSourceNode
  private void verifyAggregationStep(Map<String, AggregationStep> expected, PlanNode root) {
    if (root == null) {
      return;
    }
    if (root instanceof SeriesAggregationSourceNode) {
      SeriesAggregationSourceNode handle = (SeriesAggregationSourceNode) root;
      List<AggregationDescriptor> descriptorList = handle.getAggregationDescriptorList();
      descriptorList.forEach(
          d -> {
            assertEquals(expected.get(handle.getPartitionPath().getFullPath()), d.getStep());
          });
    }
    root.getChildren().forEach(child -> verifyAggregationStep(expected, child));
  }

  // verify AggregationNode
  private void verifyAggregationStep(List<AggregationStep> expected, PlanNode root) {
    if (root == null) {
      return;
    }
    if (root instanceof AggregationNode) {
      List<AggregationStep> actual =
          ((AggregationNode) root)
              .getAggregationDescriptorList().stream()
                  .map(AggregationDescriptor::getStep)
                  .collect(Collectors.toList());
      assertEquals(expected, actual);
    }
    root.getChildren().forEach(child -> verifyAggregationStep(expected, child));
  }

  @Test
  public void testTimeJoinAggregationWithSlidingWindow() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_agg_with_sliding");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String d1s1Path = "root.sg.d1.s1";
    String d3s1Path = "root.sg.d333.s1";
    String sql = "select count(s1) from root.sg.d1,root.sg.d333 group by ([0, 50), 5ms, 3ms)";

    Analysis analysis = Util.analyze(sql, context);
    PlanNode slidingWindowAggregationNode = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(
            analysis, new LogicalQueryPlan(context, slidingWindowAggregationNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d3s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
    AggregationNode aggregationNode =
        (AggregationNode)
            fragmentInstances
                .get(0)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    aggregationNode
        .getAggregationDescriptorList()
        .forEach(d -> Assert.assertEquals(AggregationStep.INTERMEDIATE, d.getStep()));
  }

  @Test
  public void testTimeJoinAggregationMultiPerRegion() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String d1s1Path = "root.sg.d1.s1";
    String d3s1Path = "root.sg.d333.s1";

    String sql = "select count(s1) from root.sg.d1, root.sg.d333";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode timeJoinNode = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, timeJoinNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d3s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
  }

  @Test
  public void testTimeJoinAggregationMultiPerRegion2() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    //    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    String d3s1Path = "root.sg.d333.s1";
    //    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));

    String d4s1Path = "root.sg.d4444.s1";
    //    timeJoinNode.addChild(genAggregationSourceNode(queryId, d4s1Path, AggregationType.COUNT));
    //    Analysis analysis = Util.constructAnalysis();
    String sql = "select count(s1) from root.sg.d333, root.sg.d4444";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode timeJoinNode = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, timeJoinNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d3s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d4s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
  }

  @Test
  public void testGroupByLevelWithTwoChildren() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_two_children");
    String d1s1Path = "root.sg.d1.s1";
    String d2s1Path = "root.sg.d22.s1";
    String groupedPath = "root.sg.*.s1";

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Arrays.asList(
                genAggregationSourceNode(queryId, d1s1Path, TAggregationType.COUNT),
                genAggregationSourceNode(queryId, d2s1Path, TAggregationType.COUNT)),
            Collections.singletonList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPath)))),
            null,
            Ordering.ASC);
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
  }

  @Test
  public void testAggregationWithMultiGroupByLevelNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_two_children");
    String d3s1Path = "root.sg.d333.s1";
    String d4s1Path = "root.sg.d4444.s1";
    String groupedPath = "root.sg.*.s1";

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Arrays.asList(
                genAggregationSourceNode(queryId, d3s1Path, TAggregationType.COUNT),
                genAggregationSourceNode(queryId, d4s1Path, TAggregationType.COUNT)),
            Collections.singletonList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d3s1Path)),
                        new TimeSeriesOperand(new PartialPath(d4s1Path))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPath)))),
            null,
            Ordering.ASC);
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d3s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d4s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));

    Map<String, List<String>> expectedDescriptorValue = new HashMap<>();
    expectedDescriptorValue.put(groupedPath, Arrays.asList(groupedPath, d3s1Path, d4s1Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue,
        (GroupByLevelNode)
            fragmentInstances.get(0).getFragment().getPlanNodeTree().getChildren().get(0));

    Map<String, List<String>> expectedDescriptorValue2 = new HashMap<>();
    expectedDescriptorValue2.put(groupedPath, Arrays.asList(d3s1Path, d4s1Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue2,
        (GroupByLevelNode)
            fragmentInstances.get(1).getFragment().getPlanNodeTree().getChildren().get(0));
  }

  @Test
  public void testGroupByLevelNodeWithSlidingWindow() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_with_sliding_window");
    String d3s1Path = "root.sg.d333.s1";
    String d4s1Path = "root.sg.d4444.s1";
    String groupedPath = "root.sg.*.s1";

    SlidingWindowAggregationNode slidingWindowAggregationNode =
        genSlidingWindowAggregationNode(
            queryId,
            Arrays.asList(new PartialPath(d3s1Path), new PartialPath(d4s1Path)),
            TAggregationType.COUNT,
            AggregationStep.PARTIAL,
            null);
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, TAggregationType.COUNT));
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d4s1Path, TAggregationType.COUNT));
    slidingWindowAggregationNode.addChild(timeJoinNode);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Collections.singletonList(slidingWindowAggregationNode),
            Collections.singletonList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d3s1Path)),
                        new TimeSeriesOperand(new PartialPath(d4s1Path))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPath)))),
            null,
            Ordering.ASC);

    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    List<FragmentInstance> fragmentInstances = plan.getInstances();
    List<AggregationStep> expected = Arrays.asList(AggregationStep.FINAL, AggregationStep.FINAL);
    verifyAggregationStep(expected, fragmentInstances.get(0).getFragment().getPlanNodeTree());

    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d3s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d4s1Path, AggregationStep.PARTIAL);

    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));

    Map<String, List<String>> expectedDescriptorValue = new HashMap<>();
    expectedDescriptorValue.put(groupedPath, Arrays.asList(groupedPath, d3s1Path, d4s1Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue,
        (GroupByLevelNode)
            fragmentInstances.get(0).getFragment().getPlanNodeTree().getChildren().get(0));

    Map<String, List<String>> expectedDescriptorValue2 = new HashMap<>();
    expectedDescriptorValue2.put(groupedPath, Arrays.asList(d3s1Path, d4s1Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue2,
        (GroupByLevelNode)
            fragmentInstances.get(1).getFragment().getPlanNodeTree().getChildren().get(0));

    verifySlidingWindowDescriptor(
        Arrays.asList(d3s1Path, d4s1Path),
        (SlidingWindowAggregationNode)
            fragmentInstances
                .get(0)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0));
    verifySlidingWindowDescriptor(
        Arrays.asList(d3s1Path, d4s1Path),
        (SlidingWindowAggregationNode)
            fragmentInstances
                .get(1)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0));
  }

  @Test
  public void testGroupByLevelTwoSeries() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_two_series");
    String d1s1Path = "root.sg.d1.s1";
    String d1s2Path = "root.sg.d1.s2";
    String groupedPathS1 = "root.sg.*.s1";
    String groupedPathS2 = "root.sg.*.s2";

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Arrays.asList(
                genAggregationSourceNode(queryId, d1s1Path, TAggregationType.COUNT),
                genAggregationSourceNode(queryId, d1s2Path, TAggregationType.COUNT)),
            Arrays.asList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s1Path))),
                    1,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPathS1))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s2Path))),
                    1,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPathS2)))),
            null,
            Ordering.ASC);
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d1s2Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));

    Map<String, List<String>> expectedDescriptorValue = new HashMap<>();
    expectedDescriptorValue.put(groupedPathS1, Arrays.asList(groupedPathS1, d1s1Path));
    expectedDescriptorValue.put(groupedPathS2, Arrays.asList(groupedPathS2, d1s2Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue,
        (GroupByLevelNode)
            fragmentInstances.get(0).getFragment().getPlanNodeTree().getChildren().get(0));

    Map<String, List<String>> expectedDescriptorValue2 = new HashMap<>();
    expectedDescriptorValue2.put(groupedPathS1, Collections.singletonList(d1s1Path));
    expectedDescriptorValue2.put(groupedPathS2, Collections.singletonList(d1s2Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue2,
        (GroupByLevelNode)
            fragmentInstances.get(1).getFragment().getPlanNodeTree().getChildren().get(0));
  }

  @Test
  public void testGroupByLevel2Series2Devices3Regions() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_two_series");
    String d1s1Path = "root.sg.d1.s1";
    String d1s2Path = "root.sg.d1.s2";
    String d2s1Path = "root.sg.d22.s1";
    String groupedPathS1 = "root.sg.*.s1";
    String groupedPathS2 = "root.sg.*.s2";

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Arrays.asList(
                genAggregationSourceNode(queryId, d1s1Path, TAggregationType.COUNT),
                genAggregationSourceNode(queryId, d1s2Path, TAggregationType.COUNT),
                genAggregationSourceNode(queryId, d2s1Path, TAggregationType.COUNT)),
            Arrays.asList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPathS1))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s2Path))),
                    1,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPathS2)))),
            null,
            Ordering.ASC);
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d1s2Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));

    Map<String, List<String>> expectedDescriptorValue = new HashMap<>();
    expectedDescriptorValue.put(groupedPathS1, Arrays.asList(groupedPathS1, d1s1Path, d2s1Path));
    expectedDescriptorValue.put(groupedPathS2, Arrays.asList(groupedPathS2, d1s2Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue,
        (GroupByLevelNode)
            fragmentInstances.get(0).getFragment().getPlanNodeTree().getChildren().get(0));

    Map<String, List<String>> expectedDescriptorValue2 = new HashMap<>();
    expectedDescriptorValue2.put(groupedPathS1, Collections.singletonList(d1s1Path));
    expectedDescriptorValue2.put(groupedPathS2, Collections.singletonList(d1s2Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue2,
        (GroupByLevelNode)
            fragmentInstances.get(2).getFragment().getPlanNodeTree().getChildren().get(0));
  }

  @Test
  public void testGroupByLevelWithSliding2Series2Devices3Regions() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_two_series");
    String d1s1Path = "root.sg.d1.s1";
    String d1s2Path = "root.sg.d1.s2";
    String d2s1Path = "root.sg.d22.s1";
    String groupedPathS1 = "root.sg.*.s1";
    String groupedPathS2 = "root.sg.*.s2";

    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, TAggregationType.COUNT));
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s2Path, TAggregationType.COUNT));
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d2s1Path, TAggregationType.COUNT));

    SlidingWindowAggregationNode slidingWindowAggregationNode =
        genSlidingWindowAggregationNode(
            queryId,
            Arrays.asList(
                new PartialPath(d1s1Path), new PartialPath(d1s2Path), new PartialPath(d2s1Path)),
            TAggregationType.COUNT,
            AggregationStep.PARTIAL,
            null);
    slidingWindowAggregationNode.addChild(timeJoinNode);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Collections.singletonList(slidingWindowAggregationNode),
            Arrays.asList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPathS1))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s2Path))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath(groupedPathS2)))),
            null,
            Ordering.ASC);
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d1s2Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));

    Map<String, List<String>> expectedDescriptorValue = new HashMap<>();
    expectedDescriptorValue.put(groupedPathS1, Arrays.asList(groupedPathS1, d1s1Path));
    expectedDescriptorValue.put(groupedPathS2, Arrays.asList(groupedPathS2, d1s2Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue,
        (GroupByLevelNode)
            fragmentInstances.get(0).getFragment().getPlanNodeTree().getChildren().get(0));

    Map<String, List<String>> expectedDescriptorValue2 = new HashMap<>();
    expectedDescriptorValue2.put(groupedPathS1, Collections.singletonList(d2s1Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue2,
        (GroupByLevelNode)
            fragmentInstances.get(1).getFragment().getPlanNodeTree().getChildren().get(0));

    Map<String, List<String>> expectedDescriptorValue3 = new HashMap<>();
    expectedDescriptorValue3.put(groupedPathS1, Collections.singletonList(d1s1Path));
    expectedDescriptorValue3.put(groupedPathS2, Collections.singletonList(d1s2Path));
    verifyGroupByLevelDescriptor(
        expectedDescriptorValue3,
        (GroupByLevelNode)
            fragmentInstances.get(2).getFragment().getPlanNodeTree().getChildren().get(0));

    verifySlidingWindowDescriptor(
        Arrays.asList(d1s1Path, d1s2Path),
        (SlidingWindowAggregationNode)
            fragmentInstances
                .get(0)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0));
    verifySlidingWindowDescriptor(
        Collections.singletonList(d2s1Path),
        (SlidingWindowAggregationNode)
            fragmentInstances
                .get(1)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0));
    verifySlidingWindowDescriptor(
        Arrays.asList(d1s1Path, d1s2Path),
        (SlidingWindowAggregationNode)
            fragmentInstances
                .get(2)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0));
  }

  @Test
  public void testAggregation1Series1Region() throws IllegalPathException {
    QueryId queryId = new QueryId("test_aggregation_1_series_1_region");
    String d2s1Path = "root.sg.d22.s1";

    PlanNode root = genAggregationSourceNode(queryId, d2s1Path, TAggregationType.COUNT);
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
    assertEquals(
        root, plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0));
  }

  @Test
  public void testAlignByDevice1Device2Region() throws IllegalPathException {
    QueryId queryId = new QueryId("test_align_by_device_1_device_2_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1), count(s2) from root.sg.d1 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root =
        plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f2Root =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(f1Root instanceof DeviceViewNode);
    assertTrue(f2Root instanceof HorizontallyConcatNode);
    assertTrue(f1Root.getChildren().get(0) instanceof AggregationNode);
    assertEquals(3, f1Root.getChildren().get(0).getChildren().size());
  }

  @Test
  public void testAlignByDevice2Device3Region() {
    QueryId queryId = new QueryId("test_align_by_device_2_device_3_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1), count(s2) from root.sg.d1,root.sg.d22 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    PlanNode f1Root =
        plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f2Root =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f3Root =
        plan.getInstances().get(2).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(f1Root instanceof DeviceViewNode);
    assertTrue(f2Root instanceof HorizontallyConcatNode);
    assertTrue(f3Root instanceof HorizontallyConcatNode);
    assertTrue(f3Root.getChildren().get(0) instanceof SeriesSourceNode);
    assertTrue(f1Root.getChildren().get(0) instanceof AggregationNode);
    assertTrue(f1Root.getChildren().get(1) instanceof ExchangeNode);
    assertEquals(3, f1Root.getChildren().get(0).getChildren().size());
  }

  @Test
  public void testAlignByDevice2Device2Region() {
    QueryId queryId = new QueryId("test_align_by_device_2_device_2_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1), count(s2) from root.sg.d333,root.sg.d4444 align by device";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
    PlanNode f1Root =
        plan.getInstances().get(0).getFragment().getPlanNodeTree().getChildren().get(0);
    PlanNode f2Root =
        plan.getInstances().get(1).getFragment().getPlanNodeTree().getChildren().get(0);
    assertTrue(f1Root instanceof DeviceViewNode);
    assertTrue(f2Root instanceof HorizontallyConcatNode);
    assertEquals(2, f1Root.getChildren().size());
  }

  private void verifyGroupByLevelDescriptor(
      Map<String, List<String>> expected, GroupByLevelNode node) {
    List<CrossSeriesAggregationDescriptor> descriptors = node.getGroupByLevelDescriptors();
    assertEquals(expected.size(), descriptors.size());
    for (CrossSeriesAggregationDescriptor descriptor : descriptors) {
      String outputExpression = descriptor.getOutputExpression().getExpressionString();
      assertEquals(expected.get(outputExpression).size(), descriptor.getInputExpressions().size());
      for (Expression inputExpression : descriptor.getInputExpressions()) {
        assertTrue(expected.get(outputExpression).contains(inputExpression.getExpressionString()));
      }
    }
  }

  private void verifySlidingWindowDescriptor(
      List<String> expected, SlidingWindowAggregationNode node) {
    List<AggregationDescriptor> descriptorList = node.getAggregationDescriptorList();
    assertEquals(expected.size(), descriptorList.size());
    Map<String, Integer> verification = new HashMap<>();
    descriptorList.forEach(
        d -> verification.put(d.getInputExpressions().get(0).getExpressionString(), 1));
    assertEquals(expected.size(), verification.size());
    expected.forEach(v -> assertEquals(1, (int) verification.get(v)));
  }

  private SlidingWindowAggregationNode genSlidingWindowAggregationNode(
      QueryId queryId,
      List<PartialPath> paths,
      TAggregationType type,
      AggregationStep step,
      GroupByTimeParameter groupByTimeParameter) {
    return new SlidingWindowAggregationNode(
        queryId.genPlanNodeId(),
        paths.stream()
            .map(
                path ->
                    new AggregationDescriptor(
                        type.name().toLowerCase(),
                        step,
                        Collections.singletonList(new TimeSeriesOperand(path))))
            .collect(Collectors.toList()),
        groupByTimeParameter,
        Ordering.ASC);
  }

  private SeriesAggregationSourceNode genAggregationSourceNode(
      QueryId queryId, String path, TAggregationType type) throws IllegalPathException {
    List<AggregationDescriptor> descriptors = new ArrayList<>();
    descriptors.add(
        new AggregationDescriptor(
            type.name().toLowerCase(),
            AggregationStep.FINAL,
            Collections.singletonList(new TimeSeriesOperand(new PartialPath(path)))));

    return new SeriesAggregationScanNode(
        queryId.genPlanNodeId(), new MeasurementPath(path, TSDataType.INT32), descriptors);
  }

  @Test
  public void testParallelPlanWithAlignedSeries() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_aligned");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "select d666666.s1, d666666.s2, d333.s1 from root.sg limit 10";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode root = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
  }

  @Test
  public void testEachSeriesOneRegion() {
    QueryId queryId = new QueryId("test_each_series_1_region");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    String sql = "select count(s1), count(s2) from root.sg.d22, root.sg.d55555";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());

    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        fragmentInstance ->
            assertTrue(
                fragmentInstance.getFragment().getPlanNodeTree().getChildren().get(0)
                    instanceof HorizontallyConcatNode));

    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put("root.sg.d22.s1", AggregationStep.SINGLE);
    expectedStep.put("root.sg.d22.s2", AggregationStep.SINGLE);
    expectedStep.put("root.sg.d55555.s1", AggregationStep.SINGLE);
    expectedStep.put("root.sg.d55555.s2", AggregationStep.SINGLE);
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
  }
}
