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

import org.apache.iotdb.commons.partition.*;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.*;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.planner.DistributionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

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

    //        PlanNodeUtil.printPlanNode(newRoot);
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
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d22.s1")));

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

    DataPartitionInfo dataPartitionInfo = new DataPartitionInfo();
    Map<String, Map<DeviceGroupId, Map<TimePartitionId, List<DataRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<DeviceGroupId, Map<TimePartitionId, List<DataRegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();
    List<DataRegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(
        new DataRegionReplicaSet(
            new DataRegionId(1),
            Arrays.asList(new EndPoint("192.0.0.1", 9000), new EndPoint("192.0.0.2", 9000))));
    d1DataRegions.add(
        new DataRegionReplicaSet(
            new DataRegionId(2),
            Arrays.asList(new EndPoint("192.0.0.3", 9000), new EndPoint("192.0.0.4", 9000))));
    Map<TimePartitionId, List<DataRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TimePartitionId(), d1DataRegions);

    List<DataRegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new DataRegionReplicaSet(
            new DataRegionId(3),
            Arrays.asList(new EndPoint("192.0.0.5", 9000), new EndPoint("192.0.0.6", 9000))));
    Map<TimePartitionId, List<DataRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TimePartitionId(), d2DataRegions);

    sgPartitionMap.put(new DeviceGroupId(device1.length()), d1DataRegionMap);
    sgPartitionMap.put(new DeviceGroupId(device2.length()), d2DataRegionMap);

    dataPartitionMap.put("root.sg", sgPartitionMap);

    dataPartitionInfo.setDataPartitionMap(dataPartitionMap);

    analysis.setDataPartitionInfo(dataPartitionInfo);
    return analysis;
  }
}
