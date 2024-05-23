/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.SAME_WITH_ALL_CHILDREN;

public class ExchangeNodeGenerator extends SimplePlanRewriter<ExchangeNodeGenerator.PlanContext> {

  @Override
  public List<PlanNode> visitTableScan(TableScanNode node, PlanContext context) {

    if (node.getRegionReplicaSetList().size() > 1) {
      context.hasExchangeNode = true;
      List<Symbol> orderBy = node.getOutputSymbols().subList(0, 1);
      Map<Symbol, SortOrder> orderings =
          Collections.singletonMap(node.getOutputSymbols().get(0), SortOrder.ASC_NULLS_LAST);
      OrderingScheme orderingScheme = new OrderingScheme(orderBy, orderings);
      MergeSortNode mergeSortNode =
          new MergeSortNode(
              context.queryContext.getQueryId().genPlanNodeId(),
              orderingScheme,
              node.getOutputSymbols());

      for (int i = 0; i < node.getRegionReplicaSetList().size(); i++) {
        TRegionReplicaSet regionReplicaSet = node.getRegionReplicaSetList().get(i);
        TableScanNode subTableScanNode = node.clone();
        subTableScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        subTableScanNode.setRegionReplicaSet(regionReplicaSet);
        context.nodeDistributionMap.put(
            subTableScanNode.getPlanNodeId(),
            new NodeDistribution(SAME_WITH_ALL_CHILDREN, regionReplicaSet));

        // TODO not use 0 replica set as root replica?
        if (i == 0) {
          mergeSortNode.addChild(subTableScanNode);
          context.nodeDistributionMap.put(
              mergeSortNode.getPlanNodeId(),
              new NodeDistribution(SAME_WITH_ALL_CHILDREN, regionReplicaSet));
        } else {
          ExchangeNode exchangeNode =
              new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
          exchangeNode.addChild(subTableScanNode);
          mergeSortNode.addChild(exchangeNode);
        }
      }
      return Collections.singletonList(mergeSortNode);
    } else {
      node.setRegionReplicaSet(node.getRegionReplicaSetList().get(0));
      return Collections.singletonList(node);
    }
  }

  public static class PlanContext {
    final MPPQueryContext queryContext;
    final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
    TRegionReplicaSet mostlyUsedDataRegion;
    boolean hasExchangeNode = false;

    public PlanContext(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
      this.nodeDistributionMap = new HashMap<>();
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }
  }
}
