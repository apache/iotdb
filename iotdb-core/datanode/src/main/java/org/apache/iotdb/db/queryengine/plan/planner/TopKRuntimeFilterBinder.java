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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.filter.TopKRuntimeFilter;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

/**
 * Binds {@link TopKRuntimeFilter} runtime objects according to plan marks set by {@link
 * org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.TopKRuntimeFilterOptimizer}.
 */
public class TopKRuntimeFilterBinder {

  private TopKRuntimeFilterBinder() {}

  public static void bind(PlanNode planRoot, DataNodeQueryContext dataNodeQueryContext) {
    planRoot.accept(new Binder(), dataNodeQueryContext);
  }

  private static class Binder implements PlanVisitor<Void, DataNodeQueryContext> {

    @Override
    public Void visitPlan(PlanNode node, DataNodeQueryContext context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return null;
    }

    @Override
    public Void visitTopK(TopKNode node, DataNodeQueryContext context) {
      if (node.isUseTopKRuntimeFilter()) {
        context.registerTopKRuntimeFilter(
            node.getPlanNodeId().getId(),
            new TopKRuntimeFilter(node.isTopKRuntimeFilterAscending()));
      }
      return visitPlan(node, context);
    }
  }
}
