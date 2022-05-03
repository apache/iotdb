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
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SeriesSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DistributionPlannerTest {

  @Test
  public void testSingleSeriesScan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SeriesScanNode root =
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    plan.getInstances().forEach(System.out::println);
    assertEquals(2, plan.getInstances().size());
  }

  @Test
  public void testSingleSeriesScanRewriteSource() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SeriesScanNode root =
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Sets.newHashSet("s1", "s2"),
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
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d22.s1", TSDataType.INT32),
            Sets.newHashSet("s1"),
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
    SeriesSchemaMergeNode metaMergeNode = new SeriesSchemaMergeNode(queryId.genPlanNodeId(), false);
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
    System.out.println(PlanNodeUtil.nodeToString(newRoot2));
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
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d22.s1", TSDataType.INT32),
            Sets.newHashSet("s1"),
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
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d22.s1", TSDataType.INT32),
            Sets.newHashSet("s1"),
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
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d333.s1", TSDataType.INT32),
            Sets.newHashSet("s1"),
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
    plan.getInstances().forEach(System.out::println);
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
    plan.getInstances().forEach(System.out::println);
    assertEquals(1, plan.getInstances().size());
  }

  private Analysis constructAnalysis() {

    SeriesPartitionExecutor executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    Analysis analysis = new Analysis();

    String device1 = "root.sg.d1";
    String device2 = "root.sg.d22";
    String device3 = "root.sg.d333";

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

    sgPartitionMap.put(executor.getSeriesPartitionSlot(device1), d1DataRegionMap);
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device2), d2DataRegionMap);
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device3), d3DataRegionMap);

    dataPartitionMap.put("root.sg", sgPartitionMap);

    dataPartition.setDataPartitionMap(dataPartitionMap);

    analysis.setDataPartitionInfo(dataPartition);

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
