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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.*;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.DistributionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d2.s1")));

    LimitNode root = new LimitNode(PlanNodeIdAllocator.generateId(), 10, timeJoinNode);

    Analysis analysis = constructAnalysis();

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(), root));
    PlanNode newRoot = planner.rewriteSource();

    //    PlanNodeUtil.printPlanNode(newRoot);
    assertEquals(newRoot.getChildren().get(0).getChildren().size(), 3);
    assertEquals(newRoot.getChildren().get(0).getChildren().get(0).getChildren().size(), 2);
    assertEquals(newRoot.getChildren().get(0).getChildren().get(1).getChildren().size(), 2);
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
        new SeriesScanNode(PlanNodeIdAllocator.generateId(), new PartialPath("root.sg.d2.s1")));

    LimitNode root = new LimitNode(PlanNodeIdAllocator.generateId(), 10, timeJoinNode);

    Analysis analysis = constructAnalysis();

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(), root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    //    PlanNodeUtil.printPlanNode(rootWithExchange);
    assertEquals(rootWithExchange.getChildren().get(0).getChildren().size(), 3);
    assertEquals(
        rootWithExchange.getChildren().get(0).getChildren().get(0).getChildren().size(), 2);
    assertTrue(rootWithExchange.getChildren().get(0).getChildren().get(1) instanceof ExchangeNode);
    assertEquals(
        rootWithExchange.getChildren().get(0).getChildren().get(1).getChildren().size(), 1);
    assertTrue(rootWithExchange.getChildren().get(0).getChildren().get(2) instanceof ExchangeNode);
    assertEquals(
        rootWithExchange.getChildren().get(0).getChildren().get(2).getChildren().size(), 1);
  }

  private Analysis constructAnalysis() {
    Analysis analysis = new Analysis();
    Map<String, Map<DataRegionTimeSlice, List<DataRegion>>> dataPartitionInfo = new HashMap<>();
    List<DataRegion> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(new DataRegion(1, "192.0.0.1"));
    d1DataRegions.add(new DataRegion(2, "192.0.0.1"));
    Map<DataRegionTimeSlice, List<DataRegion>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new DataRegionTimeSlice(), d1DataRegions);

    List<DataRegion> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(new DataRegion(3, "192.0.0.1"));
    Map<DataRegionTimeSlice, List<DataRegion>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new DataRegionTimeSlice(), d2DataRegions);

    dataPartitionInfo.put("root.sg.d1", d1DataRegionMap);
    dataPartitionInfo.put("root.sg.d2", d2DataRegionMap);

    analysis.setDataPartitionInfo(dataPartitionInfo);
    return analysis;
  }
}
