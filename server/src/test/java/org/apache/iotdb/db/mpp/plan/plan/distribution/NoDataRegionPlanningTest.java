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
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NoDataRegionPlanningTest {
  @Test
  public void testParallelPlan() throws IllegalPathException {
    String d1s1 = "root.sg.d1.s1";
    String d1s2 = "root.sg.d1.s2";
    String d3s1 = "root.sg.d333.s1";
    String d5s1 = "root.sg.d55555.s1";

    QueryId queryId = new QueryId("test_query");
    TimeJoinNode timeJoinNode = new TimeJoinNode(queryId.genPlanNodeId(), OrderBy.TIMESTAMP_ASC);

    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath(d1s1, TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath(d1s2, TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath(d3s1, TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));
    timeJoinNode.addChild(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath(d5s1, TSDataType.INT32),
            OrderBy.TIMESTAMP_ASC));

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
