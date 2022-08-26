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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByLevelDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.aggregation.AggregationType;
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
  public void testTimeJoinAggregationSinglePerRegion() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    String d1s1Path = "root.sg.d1.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT));

    String d2s1Path = "root.sg.d22.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d2s1Path, AggregationType.COUNT));

    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, timeJoinNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.PARTIAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(
        f -> verifyAggregationStep(expectedStep, f.getFragment().getPlanNodeTree()));
  }

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

  @Test
  public void testTimeJoinAggregationWithSlidingWindow() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_agg_with_sliding");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    String d1s1Path = "root.sg.d1.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT));

    String d3s1Path = "root.sg.d333.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));

    SlidingWindowAggregationNode slidingWindowAggregationNode =
        genSlidingWindowAggregationNode(
            queryId,
            Arrays.asList(new PartialPath(d1s1Path), new PartialPath(d3s1Path)),
            AggregationType.COUNT,
            AggregationStep.PARTIAL,
            null);

    slidingWindowAggregationNode.addChild(timeJoinNode);

    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
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
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    String d1s1Path = "root.sg.d1.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT));

    String d3s1Path = "root.sg.d333.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));

    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
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
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    String d3s1Path = "root.sg.d333.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));

    String d4s1Path = "root.sg.d4444.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d4s1Path, AggregationType.COUNT));
    Analysis analysis = Util.constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
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
                genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT),
                genAggregationSourceNode(queryId, d2s1Path, AggregationType.COUNT)),
            Collections.singletonList(
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
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
                genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT),
                genAggregationSourceNode(queryId, d4s1Path, AggregationType.COUNT)),
            Collections.singletonList(
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d3s1Path)),
                        new TimeSeriesOperand(new PartialPath(d4s1Path))),
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
            AggregationType.COUNT,
            AggregationStep.PARTIAL,
            null);
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d4s1Path, AggregationType.COUNT));
    slidingWindowAggregationNode.addChild(timeJoinNode);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Collections.singletonList(slidingWindowAggregationNode),
            Collections.singletonList(
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d3s1Path)),
                        new TimeSeriesOperand(new PartialPath(d4s1Path))),
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
                genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT),
                genAggregationSourceNode(queryId, d1s2Path, AggregationType.COUNT)),
            Arrays.asList(
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s1Path))),
                    new TimeSeriesOperand(new PartialPath(groupedPathS1))),
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s2Path))),
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
                genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT),
                genAggregationSourceNode(queryId, d1s2Path, AggregationType.COUNT),
                genAggregationSourceNode(queryId, d2s1Path, AggregationType.COUNT)),
            Arrays.asList(
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
                    new TimeSeriesOperand(new PartialPath(groupedPathS1))),
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s2Path))),
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
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT));
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s2Path, AggregationType.COUNT));
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d2s1Path, AggregationType.COUNT));

    SlidingWindowAggregationNode slidingWindowAggregationNode =
        genSlidingWindowAggregationNode(
            queryId,
            Arrays.asList(
                new PartialPath(d1s1Path), new PartialPath(d1s2Path), new PartialPath(d2s1Path)),
            AggregationType.COUNT,
            AggregationStep.PARTIAL,
            null);
    slidingWindowAggregationNode.addChild(timeJoinNode);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Collections.singletonList(slidingWindowAggregationNode),
            Arrays.asList(
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
                    new TimeSeriesOperand(new PartialPath(groupedPathS1))),
                new GroupByLevelDescriptor(
                    AggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(new TimeSeriesOperand(new PartialPath(d1s2Path))),
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

    PlanNode root = genAggregationSourceNode(queryId, d2s1Path, AggregationType.COUNT);
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

  private void verifyGroupByLevelDescriptor(
      Map<String, List<String>> expected, GroupByLevelNode node) {
    List<GroupByLevelDescriptor> descriptors = node.getGroupByLevelDescriptors();
    assertEquals(expected.size(), descriptors.size());
    for (GroupByLevelDescriptor descriptor : descriptors) {
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
      AggregationType type,
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
      QueryId queryId, String path, AggregationType type) throws IllegalPathException {
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
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC);

    timeJoinNode.addChild(
        new AlignedSeriesScanNode(
            queryId.genPlanNodeId(),
            new AlignedPath("root.sg.d1", Arrays.asList("s1", "s2")),
            Ordering.ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d333.s1", TSDataType.INT32),
            Ordering.ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);
    Analysis analysis = Util.constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
  }
}
