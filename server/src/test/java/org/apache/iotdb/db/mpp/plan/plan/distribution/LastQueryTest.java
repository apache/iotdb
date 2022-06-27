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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanBuilder;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LastQueryTest {

  @Test
  public void testLastQuery1Series1Region() throws IllegalPathException {
    String d2s1Path = "root.sg.d22.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_1_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Collections.singletonList(d2s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(1, distributedQueryPlan.getInstances().size());
  }

  @Test
  public void testLastQuery1Series2Region() throws IllegalPathException {
    String d1s1Path = "root.sg.d1.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_2_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Collections.singletonList(d1s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(2, distributedQueryPlan.getInstances().size());
  }

  @Test
  public void testLastQuery2Series3Region() throws IllegalPathException {
    String d1s1Path = "root.sg.d1.s1";
    String d2s1Path = "root.sg.d22.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_2_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Arrays.asList(d1s1Path, d2s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(3, distributedQueryPlan.getInstances().size());
  }

  @Test
  public void testLastQuery2Series2Region() throws IllegalPathException {
    String d3s1Path = "root.sg.d333.s1";
    String d4s1Path = "root.sg.d4444.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_2_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Arrays.asList(d3s1Path, d4s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(2, distributedQueryPlan.getInstances().size());
  }

  private LogicalQueryPlan constructLastQuery(List<String> paths, MPPQueryContext context)
      throws IllegalPathException {
    LogicalPlanBuilder builder = new LogicalPlanBuilder(context);
    Set<Expression> expressions = new HashSet<>();
    for (String path : paths) {
      expressions.add(new TimeSeriesOperand(new MeasurementPath(path)));
    }
    PlanNode root = builder.planLast(expressions, null).getRoot();
    return new LogicalQueryPlan(context, root);
  }
}
