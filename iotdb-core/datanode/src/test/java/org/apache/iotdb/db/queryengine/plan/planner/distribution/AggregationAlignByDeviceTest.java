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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;

import org.junit.Test;

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
  PlanNode firstFiTopNode;
  PlanNode mergeSortNode;

  /*
   * IdentitySinkNode-27
   *   └──LimitNode-22
   *       └──FilterNode-12
   *           └──DeviceView-14
   *               ├──AggregationNode-5
   *               │   └──FilterNode-4
   *               │       └──FullOuterTimeJoinNode-3
   *               │           ├──SeriesScanNode-15:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │           ├──SeriesScanNode-17:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:1)]
   *               │           └──ExchangeNode-23: [SourceAddress:192.0.2.1/test.2.0/25]
   *               └──ExchangeNode-24: [SourceAddress:192.0.3.1/test.3.0/26]
   *
   *  IdentitySinkNode-25
   *   └──FullOuterTimeJoinNode-19
   *       ├──SeriesScanNode-16:[SeriesPath: root.sg.d1.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *       └──SeriesScanNode-18:[SeriesPath: root.sg.d1.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:2)]
   *
   *  IdentitySinkNode-26
   *   └──AggregationNode-10
   *       └──FilterNode-9
   *           └──FullOuterTimeJoinNode-8
   *               ├──SeriesScanNode-20:[SeriesPath: root.sg.d22.s1, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   *               └──SeriesScanNode-21:[SeriesPath: root.sg.d22.s2, DataRegion: TConsensusGroupId(type:DataRegion, id:3)]
   */
  @Test
  public void oneMeasurementOneRegionTest() {
    // aggregation + order by device, no value filter
    sql = "select first_value(s1) from root.sg.d22 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(firstFiRoot instanceof IdentitySinkNode);
    assertTrue(firstFiRoot.getChildren().get(0) instanceof LimitNode);
    PlanNode filterNode = ((LimitNode) firstFiRoot.getChildren().get(0)).getChild();
    assertTrue(filterNode instanceof FilterNode);
    assertTrue(filterNode.getChildren().get(0) instanceof DeviceViewNode);
    assertTrue(filterNode.getChildren().get(0).getChildren().get(0) instanceof AggregationNode);
    assertTrue(
        filterNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof FilterNode);
    PlanNode thirdFiRoot = plan.getInstances().get(2).getFragment().getPlanNodeTree();
    assertTrue(thirdFiRoot instanceof IdentitySinkNode);
    assertTrue(thirdFiRoot.getChildren().get(0) instanceof AggregationNode);
    assertTrue(thirdFiRoot.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
  }

  @Test
  public void twoMeasurementMultiRegionTest() {
    // aggregation + order by device, no value filter
    sql = "select count(s1), first_value(s2) from root.sg.d1,root.sg.d333 align by device";
    analysis = Util.analyze(sql, context);
    logicalPlanNode = Util.genLogicalPlan(analysis, context);
    planner = new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
    firstFiRoot = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    System.out.println("aa");
  }
}
