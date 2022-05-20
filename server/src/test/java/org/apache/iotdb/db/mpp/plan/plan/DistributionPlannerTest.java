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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByLevelDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DistributionPlannerTest {

  @Test
  public void testSingleSeriesScan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SeriesScanNode root =
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
  }

  @Test
  public void testSingleSeriesScanRewriteSource() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SeriesScanNode root =
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    assertEquals(2, rootAfterRewrite.getChildren().size());
  }

  @Test
  public void testRewriteSourceNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");

    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d22.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);

    Analysis analysis = constructAnalysis();

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(queryId), root));
    PlanNode newRoot = planner.rewriteSource();
    assertEquals(4, newRoot.getChildren().get(0).getChildren().size());
  }

  @Test
  public void testRewriteMetaSourceNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SchemaQueryMergeNode metaMergeNode = new SchemaQueryMergeNode(queryId.genPlanNodeId(), false);
    metaMergeNode.addChild(
        new TimeSeriesSchemaScanNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1.s1"),
            null,
            null,
            10,
            0,
            false,
            false,
            false));
    metaMergeNode.addChild(
        new TimeSeriesSchemaScanNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1.s2"),
            null,
            null,
            10,
            0,
            false,
            false,
            false));
    metaMergeNode.addChild(
        new TimeSeriesSchemaScanNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d22.s1"),
            null,
            null,
            10,
            0,
            false,
            false,
            false));
    LimitNode root2 = new LimitNode(queryId.genPlanNodeId(), metaMergeNode, 10);
    Analysis analysis = constructAnalysis();
    DistributionPlanner planner2 =
        new DistributionPlanner(
            analysis, new LogicalQueryPlan(new MPPQueryContext(queryId), root2));
    PlanNode newRoot2 = planner2.rewriteSource();
    assertEquals(newRoot2.getChildren().get(0).getChildren().size(), 2);
  }

  @Test
  public void testAddExchangeNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d22.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);

    Analysis analysis = constructAnalysis();

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(queryId), root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    assertEquals(4, rootWithExchange.getChildren().get(0).getChildren().size());
    int exchangeNodeCount = 0;
    for (PlanNode child : rootWithExchange.getChildren().get(0).getChildren()) {
      exchangeNodeCount += child instanceof ExchangeNode ? 1 : 0;
    }
    assertEquals(2, exchangeNodeCount);
  }

  @Test
  public void testSplitFragment() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d22.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    SubPlan subPlan = planner.splitFragment(rootWithExchange);
    assertEquals(subPlan.getChildren().size(), 2);
  }

  @Test
  public void testParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d333.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
  }

  @Test
  public void testTimeJoinAggregationSinglePerRegion() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);
    String d1s1Path = "root.sg.d1.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT));

    String d2s1Path = "root.sg.d22.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d2s1Path, AggregationType.COUNT));

    Analysis analysis = constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, timeJoinNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.FINAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(f -> verifyAggregationStep(expectedStep, f.getFragment().getRoot()));
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
  public void testTimeJoinAggregationMultiPerRegion() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);
    String d1s1Path = "root.sg.d1.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d1s1Path, AggregationType.COUNT));

    String d3s1Path = "root.sg.d333.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));

    Analysis analysis = constructAnalysis();
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
    fragmentInstances.forEach(f -> verifyAggregationStep(expectedStep, f.getFragment().getRoot()));
  }

  @Test
  public void testTimeJoinAggregationMultiPerRegion2() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_time_join_aggregation");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);
    String d3s1Path = "root.sg.d333.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d3s1Path, AggregationType.COUNT));

    String d4s1Path = "root.sg.d4444.s1";
    timeJoinNode.addChild(genAggregationSourceNode(queryId, d4s1Path, AggregationType.COUNT));
    Analysis analysis = constructAnalysis();
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
    fragmentInstances.forEach(f -> verifyAggregationStep(expectedStep, f.getFragment().getRoot()));
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
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d1s1Path)),
                        new TimeSeriesOperand(new PartialPath(d2s1Path))),
                    new TimeSeriesOperand(new PartialPath(groupedPath)))));
    Analysis analysis = constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d1s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d2s1Path, AggregationStep.FINAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(f -> verifyAggregationStep(expectedStep, f.getFragment().getRoot()));
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
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath(d3s1Path)),
                        new TimeSeriesOperand(new PartialPath(d4s1Path))),
                    new TimeSeriesOperand(new PartialPath(groupedPath)))));
    Analysis analysis = constructAnalysis();
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, groupByLevelNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    Map<String, AggregationStep> expectedStep = new HashMap<>();
    expectedStep.put(d3s1Path, AggregationStep.PARTIAL);
    expectedStep.put(d4s1Path, AggregationStep.FINAL);
    List<FragmentInstance> fragmentInstances = plan.getInstances();
    fragmentInstances.forEach(f -> verifyAggregationStep(expectedStep, f.getFragment().getRoot()));
  }

  private SeriesAggregationSourceNode genAggregationSourceNode(
      QueryId queryId, String path, AggregationType type) throws IllegalPathException {
    List<AggregationDescriptor> descriptors = new ArrayList<>();
    descriptors.add(
        new AggregationDescriptor(
            type,
            AggregationStep.FINAL,
            Collections.singletonList(new TimeSeriesOperand(new PartialPath(path)))));

    return new SeriesAggregationScanNode(
        queryId.genPlanNodeId(), new MeasurementPath(path, TSDataType.INT32), descriptors);
  }

  @Test
  public void testParallelPlanWithAlignedSeries() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_aligned");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new AlignedSeriesScanNode(
            queryId.genPlanNodeId(),
            new AlignedPath("root.sg.d1", Arrays.asList("s1", "s2")),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d333.s1", TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);
    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
  }

  @Test
  public void testSingleAlignedSeries() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_aligned");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new AlignedSeriesScanNode(
            queryId.genPlanNodeId(),
            new AlignedPath("root.sg.d22", Arrays.asList("s1", "s2")),
            OrderBy.TIMESTAMP_ASC));

    LimitNode root = new LimitNode(queryId.genPlanNodeId(), timeJoinNode, 10);
    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
  }

  @Test
  public void testInsertRowNodeParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {
              "s1",
            },
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {10},
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
        });

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    context.setQueryType(QueryType.WRITE);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, insertRowNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
  }

  @Test
  public void testInsertRowsNodeParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode1 =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {10},
            false);
    insertRowNode1.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
        });

    InsertRowNode insertRowNode2 =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT32},
            100000L,
            new Object[] {10},
            false);
    insertRowNode2.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
        });

    InsertRowsNode node = new InsertRowsNode(queryId.genPlanNodeId());
    node.setInsertRowNodeList(Arrays.asList(insertRowNode1, insertRowNode2));
    node.setInsertRowNodeIndexList(Arrays.asList(0, 1));

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    context.setQueryType(QueryType.WRITE);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, node));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
  }

  private Analysis constructAnalysis() throws IllegalPathException {

    SeriesPartitionExecutor executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    Analysis analysis = new Analysis();

    String device1 = "root.sg.d1";
    String device2 = "root.sg.d22";
    String device3 = "root.sg.d333";
    String device4 = "root.sg.d4444";

    DataPartition dataPartition =
        new DataPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setExternalEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setExternalEndPoint(new TEndPoint("192.0.1.2", 9000)))));
    d1DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(21)
                    .setExternalEndPoint(new TEndPoint("192.0.2.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(22)
                    .setExternalEndPoint(new TEndPoint("192.0.2.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TTimePartitionSlot(), d1DataRegions);

    List<TRegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(31)
                    .setExternalEndPoint(new TEndPoint("192.0.3.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(32)
                    .setExternalEndPoint(new TEndPoint("192.0.3.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TTimePartitionSlot(), d2DataRegions);

    List<TRegionReplicaSet> d3DataRegions = new ArrayList<>();
    d3DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setExternalEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setExternalEndPoint(new TEndPoint("192.0.1.2", 9000)))));
    d3DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(41)
                    .setExternalEndPoint(new TEndPoint("192.0.4.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(42)
                    .setExternalEndPoint(new TEndPoint("192.0.4.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d3DataRegionMap = new HashMap<>();
    d3DataRegionMap.put(new TTimePartitionSlot(), d3DataRegions);

    List<TRegionReplicaSet> d4DataRegions = new ArrayList<>();
    d4DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setExternalEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setExternalEndPoint(new TEndPoint("192.0.1.2", 9000)))));
    d4DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(41)
                    .setExternalEndPoint(new TEndPoint("192.0.4.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(42)
                    .setExternalEndPoint(new TEndPoint("192.0.4.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d4DataRegionMap = new HashMap<>();
    d4DataRegionMap.put(new TTimePartitionSlot(), d4DataRegions);

    sgPartitionMap.put(executor.getSeriesPartitionSlot(device1), d1DataRegionMap);
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device2), d2DataRegionMap);
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device3), d3DataRegionMap);
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device4), d4DataRegionMap);

    dataPartitionMap.put("root.sg", sgPartitionMap);

    dataPartition.setDataPartitionMap(dataPartitionMap);

    analysis.setDataPartitionInfo(dataPartition);

    // construct AggregationExpression for GroupByLevel
    Map<String, Set<Expression>> aggregationExpression = new HashMap<>();
    Set<Expression> s1Expression = new HashSet<>();
    s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")));
    s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d22.s1")));
    s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d333.s1")));
    s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d4444.s1")));

    Set<Expression> s2Expression = new HashSet<>();
    s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")));
    s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d22.s2")));
    s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d333.s2")));
    s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d4444.s2")));

    aggregationExpression.put("root.sg.*.s1", s1Expression);
    aggregationExpression.put("root.sg.*.s2", s2Expression);
    analysis.setAggregationExpressions(aggregationExpression);

    // construct schema partition
    SchemaPartition schemaPartition =
        new SchemaPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();

    TRegionReplicaSet schemaRegion1 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 11),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setExternalEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setExternalEndPoint(new TEndPoint("192.0.1.2", 9000))));
    Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaRegionMap = new HashMap<>();

    TRegionReplicaSet schemaRegion2 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 21),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(21)
                    .setExternalEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(22)
                    .setExternalEndPoint(new TEndPoint("192.0.1.2", 9000))));

    schemaRegionMap.put(executor.getSeriesPartitionSlot(device1), schemaRegion1);
    schemaRegionMap.put(executor.getSeriesPartitionSlot(device2), schemaRegion2);
    schemaRegionMap.put(executor.getSeriesPartitionSlot(device3), schemaRegion2);
    schemaPartitionMap.put("root.sg", schemaRegionMap);
    schemaPartition.setSchemaPartitionMap(schemaPartitionMap);

    analysis.setDataPartitionInfo(dataPartition);
    analysis.setSchemaPartitionInfo(schemaPartition);
    return analysis;
  }
}
