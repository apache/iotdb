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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.SAME_WITH_ALL_CHILDREN;
import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.SAME_WITH_SOME_CHILD;

public class AddExchangeNodes extends PlanVisitor<PlanNode, DistributedPlanGenerator.PlanContext> {

  private final MPPQueryContext queryContext;

  public AddExchangeNodes(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
  }

  public PlanNode addExchangeNodes(PlanNode node, DistributedPlanGenerator.PlanContext context) {
    return node.accept(this, context);
  }

  @Override
  public PlanNode visitPlan(PlanNode node, DistributedPlanGenerator.PlanContext context) {
    if (node instanceof WritePlanNode) {
      return node;
    }

    PlanNode newNode = node.clone();
    if (node.getChildren().size() == 1) {
      newNode.addChild(node.getChildren().get(0).accept(this, context));
      context.nodeDistributionMap.put(
          node.getPlanNodeId(),
          new NodeDistribution(
              SAME_WITH_ALL_CHILDREN,
              context
                  .nodeDistributionMap
                  .get(node.getChildren().get(0).getPlanNodeId())
                  .getRegion()));
      return newNode;
    }

    for (PlanNode child : node.getChildren()) {
      PlanNode rewriteNode = child.accept(this, context);

      TRegionReplicaSet region =
          context.nodeDistributionMap.get(rewriteNode.getPlanNodeId()).getRegion();
      if (!region.equals(context.mostUsedDataRegion)) {
        ExchangeNode exchangeNode = new ExchangeNode(queryContext.getQueryId().genPlanNodeId());
        exchangeNode.addChild(rewriteNode);
        newNode.addChild(exchangeNode);
      } else {
        newNode.addChild(rewriteNode);
      }
    }

    context.nodeDistributionMap.put(
        node.getPlanNodeId(),
        new NodeDistribution(SAME_WITH_SOME_CHILD, context.mostUsedDataRegion));

    return newNode;
  }

  @Override
  public PlanNode visitTableScan(TableScanNode node, DistributedPlanGenerator.PlanContext context) {
    context.nodeDistributionMap.put(
        node.getPlanNodeId(),
        new NodeDistribution(SAME_WITH_ALL_CHILDREN, node.getRegionReplicaSet()));
    return node;
  }
}
