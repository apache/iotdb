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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>Only when exist JoinNode need execute this optimize rule before
 * TableModelTypeProviderExtractor.
 */
public class AddTableScanColumnsToTypeProviderOptimizer implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!context.getAnalysis().hasJoinNode()) {
      return plan;
    }

    return plan.accept(new Rewriter(context.getQueryContext()), null);
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Void> {

    private final MPPQueryContext queryContext;

    public Rewriter(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      node.getChildren().forEach(child -> child.accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Void context) {
      node.getAssignments()
          .forEach((k, v) -> queryContext.getTypeProvider().putTableModelType(k, v.getType()));
      return node;
    }
  }
}
