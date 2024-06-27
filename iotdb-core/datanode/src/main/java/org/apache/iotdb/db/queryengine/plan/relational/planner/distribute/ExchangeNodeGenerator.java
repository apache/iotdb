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
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.AbstractSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.SAME_WITH_ALL_CHILDREN;

public class ExchangeNodeGenerator extends SimplePlanRewriter<ExchangeNodeGenerator.PlanContext> {

  @Override
  public List<PlanNode> visitTableScan(TableScanNode node, PlanContext context) {

    Map<TRegionReplicaSet, TableScanNode> tableScanNodeMap = new HashMap<>();

    for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          context
              .analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetWithTimeFilter(
                  node.getQualifiedObjectName().getDatabaseName(),
                  deviceEntry.getDeviceID(),
                  node.getTimeFilter());
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        TableScanNode tableScanNode =
            tableScanNodeMap.computeIfAbsent(
                regionReplicaSet,
                k -> {
                  TableScanNode scanNode =
                      new TableScanNode(
                          context.queryContext.getQueryId().genPlanNodeId(),
                          node.getQualifiedObjectName(),
                          node.getOutputSymbols(),
                          node.getAssignments(),
                          new ArrayList<>(),
                          node.getIdAndAttributeIndexMap(),
                          node.getScanOrder(),
                          node.getTimePredicate().orElse(null),
                          node.getPushDownPredicate());
                  scanNode.setRegionReplicaSet(regionReplicaSet);
                  return scanNode;
                });
        tableScanNode.appendDeviceEntry(deviceEntry);
      }
    }

    int i = 0;
    if (tableScanNodeMap.size() > 1) {
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

      for (Map.Entry<TRegionReplicaSet, TableScanNode> entry : tableScanNodeMap.entrySet()) {
        TRegionReplicaSet regionReplicaSet = entry.getKey();
        TableScanNode subTableScanNode = entry.getValue();
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
        i++;
      }
      return Collections.singletonList(mergeSortNode);
    } else {
      return Collections.singletonList(tableScanNodeMap.entrySet().iterator().next().getValue());
    }
  }

  @Override
  public List<PlanNode> visitSchemaQueryMerge(SchemaQueryMergeNode node, PlanContext context) {
    return Collections.singletonList(
        addExchangeNodeForSchemaMerge(rewriteSchemaQuerySource(node, context), context));
  }

  private SchemaQueryMergeNode rewriteSchemaQuerySource(
      SchemaQueryMergeNode node, PlanContext context) {
    SchemaQueryMergeNode root = (SchemaQueryMergeNode) node.clone();

    String database = ((TableDeviceSourceNode) node.getChildren().get(0)).getDatabase();
    Set<TRegionReplicaSet> schemaRegionSet = new HashSet<>();
    context
        .analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .get(database)
        .forEach(
            (deviceGroupId, schemaRegionReplicaSet) -> schemaRegionSet.add(schemaRegionReplicaSet));

    for (PlanNode child : node.getChildren()) {
      for (TRegionReplicaSet schemaRegion : schemaRegionSet) {
        SourceNode clonedChild = (SourceNode) child.clone();
        clonedChild.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        clonedChild.setRegionReplicaSet(schemaRegion);
        root.addChild(clonedChild);
      }
    }
    return root;
  }

  private PlanNode addExchangeNodeForSchemaMerge(
      AbstractSchemaMergeNode node, PlanContext context) {
    node.getChildren()
        .forEach(
            child ->
                context.putNodeDistribution(
                    child.getPlanNodeId(),
                    new NodeDistribution(
                        NodeDistributionType.NO_CHILD,
                        ((SourceNode) child).getRegionReplicaSet())));
    NodeDistribution nodeDistribution =
        new NodeDistribution(NodeDistributionType.DIFFERENT_FROM_ALL_CHILDREN);
    PlanNode newNode = node.clone();
    nodeDistribution.setRegion(calculateSchemaRegionByChildren(node.getChildren(), context));
    context.putNodeDistribution(newNode.getPlanNodeId(), nodeDistribution);
    node.getChildren()
        .forEach(
            child -> {
              if (!nodeDistribution
                  .getRegion()
                  .equals(context.getNodeDistribution(child.getPlanNodeId()).getRegion())) {
                ExchangeNode exchangeNode =
                    new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
                exchangeNode.setChild(child);
                exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
                context.hasExchangeNode = true;
                newNode.addChild(exchangeNode);
              } else {
                newNode.addChild(child);
              }
            });
    return newNode;
  }

  private TRegionReplicaSet calculateSchemaRegionByChildren(
      List<PlanNode> children, PlanContext context) {
    // We always make the schemaRegion of SchemaMergeNode to be the same as its first child.
    return context.getNodeDistribution(children.get(0).getPlanNodeId()).getRegion();
  }

  public static class PlanContext {
    final MPPQueryContext queryContext;
    final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
    TRegionReplicaSet mostlyUsedDataRegion;
    boolean hasExchangeNode = false;

    final Analysis analysis;

    public PlanContext(MPPQueryContext queryContext, Analysis analysis) {
      this.queryContext = queryContext;
      this.nodeDistributionMap = new HashMap<>();
      this.analysis = analysis;
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }

    public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
      this.nodeDistributionMap.put(nodeId, distribution);
    }
  }
}
