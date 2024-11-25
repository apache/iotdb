/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.optimization.AggregationPushDown;
import org.apache.iotdb.db.queryengine.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.optimization.PredicatePushDown;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLANNER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLAN_OPTIMIZE;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TREE_TYPE;

/** Generate a logical plan for the statement. */
public class LogicalPlanner {

  private final MPPQueryContext context;
  private final List<PlanOptimizer> optimizers =
      Arrays.asList(new PredicatePushDown(), new AggregationPushDown());

  public LogicalPlanner(MPPQueryContext context) {
    this.context = context;
  }

  public LogicalQueryPlan plan(Analysis analysis) {
    long startTime = System.nanoTime();
    PlanNode rootNode =
        new LogicalPlanVisitor(analysis).process(analysis.getTreeStatement(), context);

    // optimize the query logical plan
    if (analysis.isQuery()) {

      long planFinishTime = System.nanoTime();
      long logicalPlanCost = planFinishTime - startTime;
      context.setLogicalPlanCost(logicalPlanCost);
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TREE_TYPE, LOGICAL_PLANNER, logicalPlanCost);

      for (PlanOptimizer optimizer : optimizers) {
        rootNode = optimizer.optimize(rootNode, analysis, context);
      }
      long logicalOptimizationCost = System.nanoTime() - planFinishTime;
      context.setLogicalOptimizationCost(logicalOptimizationCost);
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TREE_TYPE, LOGICAL_PLAN_OPTIMIZE, logicalOptimizationCost);
    }

    return new LogicalQueryPlan(context, rootNode);
  }
}
