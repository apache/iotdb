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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

/**
 * <b>Optimization phase:</b> Logical plan planning (after {@code PushTopKThroughUnion}).
 *
 * <p>Marks the {@code TopK + DeviceTableScan} pattern for TopK runtime filter. A qualifying TopK
 * uses its own plan node id as {@code topKRuntimeFilterSourceId}; the same id is stamped on direct
 * raw {@link DeviceTableScanNode} children. Distributed planning later copies this id onto
 * per-region TopK nodes and clears it on the root TopK.
 */
public class TopKRuntimeFilterOptimizer implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableTopKRuntimeFilter()) {
      return plan;
    }
    return plan.accept(new Rewriter(), null);
  }

  private static class Rewriter implements PlanVisitor<PlanNode, Void> {

    @Override
    public PlanNode visitPlan(PlanNode node, Void unused) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, null);
      }
      return node;
    }

    @Override
    public PlanNode visitTopK(TopKNode node, Void unused) {
      String topKId = node.getPlanNodeId().getId();
      for (PlanNode child : node.getChildren()) {
        boolean isRawDeviceTableScan =
            child instanceof DeviceTableScanNode && !(child instanceof AggregationTableScanNode);
        if (isRawDeviceTableScan
            && isOrderByTimeOnly(node.getOrderingScheme(), (DeviceTableScanNode) child)) {
          node.setTopKRuntimeFilterSourceId(topKId);
          ((DeviceTableScanNode) child).setTopKRuntimeFilterSourceId(topKId);
        } else {
          child.accept(this, null);
        }
      }
      return node;
    }
  }

  private static boolean isOrderByTimeOnly(
      OrderingScheme orderingScheme, DeviceTableScanNode scanNode) {
    if (orderingScheme.getOrderBy().size() != 1) {
      return false;
    }
    return scanNode.isTimeColumn(orderingScheme.getOrderBy().get(0));
  }
}
