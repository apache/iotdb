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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

/**
 * <b>Optimization phase:</b> Distributed plan planning (after exchange nodes are inserted).
 *
 * <p>Marks the {@code TopK + DeviceTableScan} structure for TopK runtime filter.
 *
 * <p>The topmost TopK establishes the <b>root TopK id</b>. In the single-region case ({@code Output
 * -> TopK -> Scan}), a static {@link #FAKE_ROOT_TOPK_ID} is used because there is no coordinator
 * TopK above Exchange. A per-region TopK that sits directly on top of {@link
 * DeviceTableScanNode}(s) becomes the runtime filter producer, and both that TopK and its scan
 * children are tagged with the <b>root</b> TopK id (not the region TopK's own id). Because {@code
 * DataNodeQueryContext} is shared by all fragment instances of the same query on one DataNode,
 * using the root id lets multiple regions on the same DataNode share a single filter.
 */
public class TopKRuntimeFilterOptimizer implements PlanOptimizer {

  /**
   * Shared root id for single-region plans ({@code Output -> TopK -> Scan}) without a coordinator
   * TopK.
   */
  static final PlanNodeId FAKE_ROOT_TOPK_ID = new PlanNodeId("");

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    if (!context.getAnalysis().isQuery()) {
      return plan;
    }
    return plan.accept(new Rewriter(), null);
  }

  /** Context carries the root TopK id, or {@code null} until the first (topmost) TopK is seen. */
  private static class Rewriter implements PlanVisitor<PlanNode, PlanNodeId> {

    @Override
    public PlanNode visitPlan(PlanNode node, PlanNodeId rootTopKId) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, rootTopKId));
      }
      return newNode;
    }

    @Override
    public PlanNode visitTopK(TopKNode node, PlanNodeId rootTopKId) {
      TopKNode topKNode = (TopKNode) node.clone();

      boolean orderByTimeOnly = TopKRuntimeFilterUtils.isOrderByTimeOnly(node.getOrderingScheme());
      PlanNodeId effectiveRootTopKId =
          resolveEffectiveRootTopKId(node, rootTopKId, orderByTimeOnly);

      // A TopK qualifies as a runtime filter producer only when it orders by time and directly
      // parents raw DeviceTableScan(s). Detect qualification and tag both the producer TopK and its
      // scan children (with the root id) in a single pass over the children.
      for (PlanNode child : node.getChildren()) {
        boolean isRawDeviceTableScan =
            child instanceof DeviceTableScanNode && !(child instanceof AggregationTableScanNode);
        if (orderByTimeOnly && isRawDeviceTableScan) {
          if (topKNode.getTopKRuntimeFilterSourceId() == null) {
            topKNode.setTopKRuntimeFilterSourceId(effectiveRootTopKId);
          }
          DeviceTableScanNode scanNode = (DeviceTableScanNode) child.clone();
          scanNode.setTopKRuntimeFilterSourceId(effectiveRootTopKId.getId());
          topKNode.addChild(scanNode);
        } else {
          topKNode.addChild(child.accept(this, effectiveRootTopKId));
        }
      }
      return topKNode;
    }

    private static PlanNodeId resolveEffectiveRootTopKId(
        TopKNode node, PlanNodeId rootTopKId, boolean orderByTimeOnly) {
      if (rootTopKId != null) {
        return rootTopKId;
      }
      if (orderByTimeOnly && hasDirectRawDeviceTableScanChild(node)) {
        // Single region: Output -> TopK -> Scan (no Exchange/coordinator TopK above).
        return FAKE_ROOT_TOPK_ID;
      }
      // Multi-region coordinator TopK: establish the root id for nested region TopKs.
      return node.getPlanNodeId();
    }

    private static boolean hasDirectRawDeviceTableScanChild(TopKNode node) {
      for (PlanNode child : node.getChildren()) {
        if (child instanceof DeviceTableScanNode && !(child instanceof AggregationTableScanNode)) {
          return true;
        }
      }
      return false;
    }
  }
}
