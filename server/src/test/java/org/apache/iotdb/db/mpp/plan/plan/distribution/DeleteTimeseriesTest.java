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
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteTimeseriesTest {

  @Test
  public void test1DataRegion1SchemaRegion() throws IllegalPathException {
    QueryId queryId = new QueryId("test_group_by_level_two_series");
    String d2s1Path = "root.sg.d22.s1";
    List<PartialPath> paths = Collections.singletonList(new PartialPath(d2s1Path));
    Analysis analysis = Util.constructAnalysis();
    LogicalQueryPlan logicalQueryPlan = constructLogicalPlan(queryId, paths, analysis);

    System.out.println(PlanNodeUtil.nodeToString(logicalQueryPlan.getRootNode()));

    DistributionPlanner planner = new DistributionPlanner(analysis, logicalQueryPlan);
    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    distributedQueryPlan.getInstances().forEach(System.out::println);
    Assert.assertEquals(6, distributedQueryPlan.getInstances().size());
  }

  private LogicalQueryPlan constructLogicalPlan(
      QueryId queryId, List<PartialPath> paths, Analysis analysis) throws IllegalPathException {
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DeleteTimeSeriesStatement statement = new DeleteTimeSeriesStatement();

    analysis.setStatement(statement);
    statement.setPartialPaths(paths);
    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    return planner.plan(analysis);
  }
}
