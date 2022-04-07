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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.commons.partition.DataNodeLocation;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class DistributionPlannerTest {

  @Test
  public void TestRewriteSourceNode() throws IllegalPathException {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(
            PlanNodeIdAllocator.generateId(), OrderBy.TIMESTAMP_ASC, FilterNullPolicy.NO_FILTER);

    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s1")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s2")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d22.s1")));

    LimitNode root = new LimitNode(PlanNodeIdAllocator.generateId(), 10, timeJoinNode);

    Analysis analysis = constructAnalysis();

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(), root));
    PlanNode newRoot = planner.rewriteSource();

    System.out.println(PlanNodeUtil.nodeToString(newRoot));
    assertEquals(newRoot.getChildren().get(0).getChildren().size(), 3);
  }

  @Test
  public void TestAddExchangeNode() throws IllegalPathException {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(
            PlanNodeIdAllocator.generateId(), OrderBy.TIMESTAMP_ASC, FilterNullPolicy.NO_FILTER);

    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s1")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s2")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d22.s1")));

    LimitNode root = new LimitNode(PlanNodeIdAllocator.generateId(), 10, timeJoinNode);

    Analysis analysis = constructAnalysis();

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(), root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    //    PlanNodeUtil.printPlanNode(rootWithExchange);
    assertEquals(rootWithExchange.getChildren().get(0).getChildren().size(), 3);
  }

  @Test
  public void TestSplitFragment() throws IllegalPathException {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(
            PlanNodeIdAllocator.generateId(), OrderBy.TIMESTAMP_ASC, FilterNullPolicy.NO_FILTER);

    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s1")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s2")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d22.s1")));

    LimitNode root = new LimitNode(PlanNodeIdAllocator.generateId(), 10, timeJoinNode);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context = new MPPQueryContext("", new QueryId("query1"), null, QueryType.READ);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    SubPlan subPlan = planner.splitFragment(rootWithExchange);
    assertEquals(subPlan.getChildren().size(), 2);
  }

  @Test
  public void TestParallelPlan() throws IllegalPathException {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(
            PlanNodeIdAllocator.generateId(), OrderBy.TIMESTAMP_ASC, FilterNullPolicy.NO_FILTER);

    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s1")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d1.s2")));
    timeJoinNode.addChild(
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d333.s1")));

    LimitNode root = new LimitNode(PlanNodeIdAllocator.generateId(), 10, timeJoinNode);

    Analysis analysis = constructAnalysis();

    MPPQueryContext context = new MPPQueryContext("", new QueryId("query1"), null, QueryType.READ);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    plan.getInstances().forEach(System.out::println);
    assertEquals(plan.getInstances().size(), 3);
  }

  private Analysis constructAnalysis() {
    Analysis analysis = new Analysis();

    String device1 = "root.sg.d1";
    String device2 = "root.sg.d22";
    String device3 = "root.sg.d333";

    DataPartition dataPartition = new DataPartition();
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    List<RegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(
        new RegionReplicaSet(
            new ConsensusGroupId(GroupType.DataRegion, 1),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000)))));
    d1DataRegions.add(
        new RegionReplicaSet(
            new ConsensusGroupId(GroupType.DataRegion, 2),
            Arrays.asList(
                new DataNodeLocation(21, new Endpoint("192.0.2.1", 9000)),
                new DataNodeLocation(22, new Endpoint("192.0.2.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TimePartitionSlot(), d1DataRegions);

    List<RegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new RegionReplicaSet(
            new ConsensusGroupId(GroupType.DataRegion, 3),
            Arrays.asList(
                new DataNodeLocation(31, new Endpoint("192.0.3.1", 9000)),
                new DataNodeLocation(32, new Endpoint("192.0.3.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TimePartitionSlot(), d2DataRegions);

    List<RegionReplicaSet> d3DataRegions = new ArrayList<>();
    d3DataRegions.add(
        new RegionReplicaSet(
            new ConsensusGroupId(GroupType.DataRegion, 1),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000)))));
    d3DataRegions.add(
        new RegionReplicaSet(
            new ConsensusGroupId(GroupType.DataRegion, 4),
            Arrays.asList(
                new DataNodeLocation(41, new Endpoint("192.0.4.1", 9000)),
                new DataNodeLocation(42, new Endpoint("192.0.4.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d3DataRegionMap = new HashMap<>();
    d3DataRegionMap.put(new TimePartitionSlot(), d3DataRegions);

    sgPartitionMap.put(new SeriesPartitionSlot(device1.length()), d1DataRegionMap);
    sgPartitionMap.put(new SeriesPartitionSlot(device2.length()), d2DataRegionMap);
    sgPartitionMap.put(new SeriesPartitionSlot(device3.length()), d3DataRegionMap);

    dataPartitionMap.put("root.sg", sgPartitionMap);

    dataPartition.setDataPartitionMap(dataPartitionMap);

    analysis.setDataPartitionInfo(dataPartition);
    return analysis;
  }
}
