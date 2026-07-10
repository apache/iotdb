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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

/**
 * <b>Optimization phase:</b> Distributed plan planning (after exchange nodes are inserted).
 *
 * <p>Marks TopK and table scan nodes for TopK runtime filter, similar to Doris {@code TOPN OPT} /
 * {@code TOPN OPT: N} plan annotations.
 */
public class TopKRuntimeFilterOptimizer implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    if (!context.getAnalysis().isQuery()) {
      return plan;
    }
    return plan.accept(new Rewriter(), null);
  }

  private static class Rewriter implements PlanVisitor<PlanNode, Void> {

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitTopK(TopKNode node, Void context) {
      TopKNode topKNode = (TopKNode) node.clone();
      for (PlanNode child : node.getChildren()) {
        topKNode.addChild(child.accept(this, context));
      }

      if (TopKRuntimeFilterUtils.isOrderByTimeOnly(topKNode.getOrderingScheme())
          && TopKRuntimeFilterUtils.qualifiesForRuntimeFilter(topKNode)) {
        topKNode.setUseTopKRuntimeFilter(true);
        topKNode.setTopKRuntimeFilterAscending(
            topKNode
                .getOrderingScheme()
                .getOrdering(topKNode.getOrderingScheme().getOrderBy().get(0))
                .isAscending());
        markScansInSubtree(topKNode, topKNode.getPlanNodeId());
      }
      return topKNode;
    }

    private void markScansInSubtree(PlanNode root, PlanNodeId sourceId) {
      root.accept(
          new PlanVisitor<Void, Void>() {
            @Override
            public Void visitPlan(PlanNode node, Void unused) {
              for (PlanNode child : node.getChildren()) {
                child.accept(this, null);
              }
              return null;
            }

            @Override
            public Void visitDeviceTableScan(DeviceTableScanNode scanNode, Void unused) {
              scanNode.setTopKRuntimeFilterSourceId(sourceId);
              return null;
            }
          },
          null);
    }
  }
}
