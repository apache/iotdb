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

package org.apache.iotdb.db.mpp.sql.plan;

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.planner.DistributionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
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
  public void TestRewriteSourceNode() throws IllegalPathException {
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
    SchemaMergeNode metaMergeNode = new SchemaMergeNode(queryId.genPlanNodeId(), false);
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
  public void TestAddExchangeNode() throws IllegalPathException {
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
  public void TestSplitFragment() throws IllegalPathException {
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

    MPPQueryContext context = new MPPQueryContext("", queryId, null, new Endpoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    SubPlan subPlan = planner.splitFragment(rootWithExchange);
    assertEquals(subPlan.getChildren().size(), 2);
  }

  @Test
  public void TestParallelPlan() throws IllegalPathException {
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

    MPPQueryContext context = new MPPQueryContext("", queryId, null, new Endpoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
  }

  @Test
  public void TestInsertRowNodeParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
            },
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {10});

    Analysis analysis = constructAnalysis();

    MPPQueryContext context = new MPPQueryContext("", queryId, null, new Endpoint());
    context.setQueryType(QueryType.WRITE);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, insertRowNode));
    DistributedQueryPlan plan = planner.planFragments();
    plan.getInstances().forEach(System.out::println);
    assertEquals(1, plan.getInstances().size());
  }

  @Test
  public void TestInsertRowsNodeParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode1 =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
            },
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {10});

    InsertRowNode insertRowNode2 =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
            },
            new TSDataType[] {TSDataType.INT32},
            100000L,
            new Object[] {10});

    InsertRowsNode node = new InsertRowsNode(queryId.genPlanNodeId());
    node.setInsertRowNodeList(Arrays.asList(insertRowNode1, insertRowNode2));
    node.setInsertRowNodeIndexList(Arrays.asList(0, 1));

    Analysis analysis = constructAnalysis();

    MPPQueryContext context = new MPPQueryContext("", queryId, null, new Endpoint());
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
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    List<RegionReplicaSet> d1DataRegion1 = new ArrayList<>();
    d1DataRegion1.add(
        new RegionReplicaSet(
            new DataRegionId(1),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000)))));

    List<RegionReplicaSet> d1DataRegion2 = new ArrayList<>();
    d1DataRegion2.add(
        new RegionReplicaSet(
            new DataRegionId(2),
            Arrays.asList(
                new DataNodeLocation(21, new Endpoint("192.0.2.1", 9000)),
                new DataNodeLocation(22, new Endpoint("192.0.2.2", 9000)))));

    Map<TimePartitionSlot, List<RegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TimePartitionSlot(0), d1DataRegion1);
    d1DataRegionMap.put(new TimePartitionSlot(1), d1DataRegion2);

    List<RegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(3),
            Arrays.asList(
                new DataNodeLocation(31, new Endpoint("192.0.3.1", 9000)),
                new DataNodeLocation(32, new Endpoint("192.0.3.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TimePartitionSlot(0), d2DataRegions);

    List<RegionReplicaSet> d3DataRegions = new ArrayList<>();
    d3DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(1),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000)))));
    d3DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(4),
            Arrays.asList(
                new DataNodeLocation(41, new Endpoint("192.0.4.1", 9000)),
                new DataNodeLocation(42, new Endpoint("192.0.4.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d3DataRegionMap = new HashMap<>();
    d3DataRegionMap.put(new TimePartitionSlot(0), d3DataRegions);

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
    Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap = new HashMap<>();

    RegionReplicaSet schemaRegion1 =
        new RegionReplicaSet(
            new SchemaRegionId(11),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000))));
    Map<SeriesPartitionSlot, RegionReplicaSet> schemaRegionMap = new HashMap<>();

    RegionReplicaSet schemaRegion2 =
        new RegionReplicaSet(
            new SchemaRegionId(21),
            Arrays.asList(
                new DataNodeLocation(21, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(22, new Endpoint("192.0.1.2", 9000))));

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
