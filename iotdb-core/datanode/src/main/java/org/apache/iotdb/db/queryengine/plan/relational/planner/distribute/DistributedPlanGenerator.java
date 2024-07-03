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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.AbstractSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.commons.conf.IoTDBConstant.TIME;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;

public class DistributedPlanGenerator
    extends PlanVisitor<List<PlanNode>, DistributedPlanGenerator.PlanContext> {
  private final MPPQueryContext queryContext;
  private final Analysis analysis;
  Map<PlanNodeId, OrderingScheme> planNodeOrderingSchemeMap = new HashMap<>();

  public DistributedPlanGenerator(MPPQueryContext queryContext, Analysis analysis) {
    this.queryContext = queryContext;
    this.analysis = analysis;
  }

  public List<PlanNode> genResult(PlanNode node, PlanContext context) {
    return node.accept(this, context);
  }

  @Override
  public List<PlanNode> visitPlan(PlanNode node, DistributedPlanGenerator.PlanContext context) {
    if (node instanceof WritePlanNode) {
      return Collections.singletonList(node);
    }

    List<List<PlanNode>> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(toImmutableList());

    PlanNode newNode = node.clone();
    for (List<PlanNode> planNodes : children) {
      planNodes.forEach(newNode::addChild);
    }
    return Collections.singletonList(newNode);
  }

  @Override
  public List<PlanNode> visitOutput(OutputNode outputNode, PlanContext context) {
    // TODO only consider the order of IDs
    context.expectedOrderingScheme =
        new OrderingScheme(
            outputNode.getOutputSymbols(),
            outputNode.getOutputSymbols().stream()
                .collect(Collectors.toMap(symbol -> symbol, symbol -> SortOrder.ASC_NULLS_LAST)));

    List<PlanNode> childrenNodes = outputNode.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      outputNode.setChild(childrenNodes.get(0));
      return Collections.singletonList(outputNode);
    }

    return connectViaMergeSort(outputNode, childrenNodes);
  }

  @Override
  public List<PlanNode> visitLimit(LimitNode limitNode, PlanContext context) {
    List<PlanNode> childrenNodes = limitNode.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      limitNode.setChild(childrenNodes.get(0));
      return Collections.singletonList(limitNode);
    }

    return connectViaMergeSort(limitNode, childrenNodes);
  }

  @Override
  public List<PlanNode> visitOffset(OffsetNode offsetNode, PlanContext context) {
    List<PlanNode> childrenNodes = offsetNode.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      offsetNode.setChild(childrenNodes.get(0));
      return Collections.singletonList(offsetNode);
    }

    return connectViaMergeSort(offsetNode, childrenNodes);
  }

  @Override
  public List<PlanNode> visitProject(ProjectNode projectNode, PlanContext context) {
    List<PlanNode> childrenNodes = projectNode.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      projectNode.setChild(childrenNodes.get(0));
      return Collections.singletonList(projectNode);
    }

    for (Expression expression : projectNode.getAssignments().getMap().values()) {
      if (containsDiffFunction(expression)) {
        return connectViaMergeSort(projectNode, childrenNodes);
      }
    }

    return childrenNodes.stream()
        .map(
            child ->
                new ProjectNode(
                    queryContext.getQueryId().genPlanNodeId(), child, projectNode.getAssignments()))
        .collect(Collectors.toList());
  }

  @Override
  public List<PlanNode> visitSort(SortNode sortNode, PlanContext context) {
    context.expectedOrderingScheme = sortNode.getOrderingScheme();
    context.hasSortNode = true;

    List<PlanNode> childrenNodes = sortNode.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      sortNode.setChild(childrenNodes.get(0));
      return Collections.singletonList(sortNode);
    }

    MergeSortNode mergeSortNode =
        new MergeSortNode(
            queryContext.getQueryId().genPlanNodeId(),
            sortNode.getOrderingScheme(),
            sortNode.getOutputSymbols());
    for (PlanNode child : childrenNodes) {
      SortNode subSortNode =
          new SortNode(
              queryContext.getQueryId().genPlanNodeId(),
              child,
              sortNode.getOrderingScheme(),
              false);
      mergeSortNode.addChild(subSortNode);
      planNodeOrderingSchemeMap.put(subSortNode.getPlanNodeId(), sortNode.getOrderingScheme());
    }
    planNodeOrderingSchemeMap.put(mergeSortNode.getPlanNodeId(), sortNode.getOrderingScheme());
    return Collections.singletonList(mergeSortNode);
  }

  @Override
  public List<PlanNode> visitFilter(FilterNode filterNode, PlanContext context) {
    List<PlanNode> childrenNodes = filterNode.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      filterNode.setChild(childrenNodes.get(0));
      return Collections.singletonList(filterNode);
    }

    if (containsDiffFunction(filterNode.getPredicate())) {
      return connectViaMergeSort(filterNode, childrenNodes);
    }

    return childrenNodes.stream()
        .map(
            child ->
                new FilterNode(
                    queryContext.getQueryId().genPlanNodeId(), child, filterNode.getPredicate()))
        .collect(Collectors.toList());
  }

  @Override
  public List<PlanNode> visitTableScan(TableScanNode node, PlanContext context) {

    Map<TRegionReplicaSet, TableScanNode> tableScanNodeMap = new HashMap<>();

    for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          analysis
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
                          queryContext.getQueryId().genPlanNodeId(),
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

    context.hasExchangeNode = tableScanNodeMap.size() > 1;

    List<PlanNode> tableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (Map.Entry<TRegionReplicaSet, TableScanNode> entry : tableScanNodeMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet = entry.getKey();
      TableScanNode subTableScanNode = entry.getValue();
      subTableScanNode.setPlanNodeId(queryContext.getQueryId().genPlanNodeId());
      subTableScanNode.setRegionReplicaSet(regionReplicaSet);
      tableScanNodeList.add(subTableScanNode);

      if (mostUsedDataRegion == null
          || subTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = regionReplicaSet;
        maxDeviceEntrySizeOfTableScan = subTableScanNode.getDeviceEntries().size();
      }
    }
    context.mostUsedDataRegion = mostUsedDataRegion;

    List<Symbol> newOrderingSymbols = new ArrayList<>();
    List<SortOrder> newSortOrders = new ArrayList<>();
    OrderingScheme expectedOrderingScheme = context.expectedOrderingScheme;

    for (Symbol symbol : expectedOrderingScheme.getOrderBy()) {
      if (!context.hasSortNode && TIME.equalsIgnoreCase(symbol.getName())) {
        continue;
      }

      if (!node.getIdAndAttributeIndexMap().containsKey(symbol)) {
        break;
      }

      newOrderingSymbols.add(symbol);
      newSortOrders.add(expectedOrderingScheme.getOrdering(symbol));
    }

    List<Function<DeviceEntry, String>> orderingRules = new ArrayList<>();
    for (Symbol symbol : newOrderingSymbols) {
      int idx = node.getIdAndAttributeIndexMap().get(symbol);
      if (node.getAssignments().get(symbol).getColumnCategory() == TsTableColumnCategory.ID) {
        // segments[0] is always tableName
        orderingRules.add(deviceEntry -> (String) deviceEntry.getDeviceID().getSegments()[idx + 1]);
      } else {
        orderingRules.add(deviceEntry -> deviceEntry.getAttributeColumnValues().get(idx));
      }
    }

    Comparator<DeviceEntry> comparator;
    if (newSortOrders.get(0).isNullsFirst()) {
      if (newSortOrders.get(0).isAscending()) {
        comparator = Comparator.nullsFirst(Comparator.comparing(orderingRules.get(0)));
      } else {
        comparator = Comparator.nullsFirst(Comparator.comparing(orderingRules.get(0))).reversed();
      }
    } else {
      if (newSortOrders.get(0).isAscending()) {
        comparator = Comparator.nullsLast(Comparator.comparing(orderingRules.get(0)));
      } else {
        comparator = Comparator.nullsLast(Comparator.comparing(orderingRules.get(0))).reversed();
      }
    }
    for (int i = 1; i < orderingRules.size(); i++) {
      Comparator<DeviceEntry> thenComparator;
      if (newSortOrders.get(i).isNullsFirst()) {
        if (newSortOrders.get(i).isAscending()) {
          thenComparator = Comparator.nullsFirst(Comparator.comparing(orderingRules.get(i)));
        } else {
          thenComparator =
              Comparator.nullsFirst(Comparator.comparing(orderingRules.get(i))).reversed();
        }
      } else {
        if (newSortOrders.get(i).isAscending()) {
          thenComparator = Comparator.nullsLast(Comparator.comparing(orderingRules.get(i)));
        } else {
          thenComparator =
              Comparator.nullsLast(Comparator.comparing(orderingRules.get(i))).reversed();
        }
      }
      comparator = comparator.thenComparing(thenComparator);
    }

    OrderingScheme newOrderingScheme =
        new OrderingScheme(
            newOrderingSymbols,
            IntStream.range(0, newOrderingSymbols.size())
                .boxed()
                .collect(Collectors.toMap(newOrderingSymbols::get, newSortOrders::get)));
    for (PlanNode planNode : tableScanNodeList) {
      TableScanNode tableScanNode = (TableScanNode) planNode;
      planNodeOrderingSchemeMap.put(tableScanNode.getPlanNodeId(), newOrderingScheme);
      List<DeviceEntry> deviceEntries = tableScanNode.getDeviceEntries();
      deviceEntries.sort(comparator);
    }

    return tableScanNodeList;
  }

  private List<PlanNode> connectViaMergeSort(
      SingleChildProcessNode node, List<PlanNode> childrenNodes) {
    OrderingScheme childrenOrderingScheme =
        planNodeOrderingSchemeMap.get(childrenNodes.get(0).getPlanNodeId());
    MergeSortNode mergeSortNode =
        new MergeSortNode(
            queryContext.getQueryId().genPlanNodeId(),
            childrenOrderingScheme,
            node.getOutputSymbols());
    childrenNodes.forEach(mergeSortNode::addChild);
    node.setChild(mergeSortNode);
    planNodeOrderingSchemeMap.put(node.getPlanNodeId(), childrenOrderingScheme);
    return Collections.singletonList(node);
  }

  // ------------------- schema related interface ---------------------------------------------

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
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .get(database)
        .forEach(
            (deviceGroupId, schemaRegionReplicaSet) -> schemaRegionSet.add(schemaRegionReplicaSet));

    for (PlanNode child : node.getChildren()) {
      for (TRegionReplicaSet schemaRegion : schemaRegionSet) {
        SourceNode clonedChild = (SourceNode) child.clone();
        clonedChild.setPlanNodeId(queryContext.getQueryId().genPlanNodeId());
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
                    new ExchangeNode(queryContext.getQueryId().genPlanNodeId());
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
    final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
    boolean hasExchangeNode = false;
    boolean hasSortNode = false;
    OrderingScheme expectedOrderingScheme;
    TRegionReplicaSet mostUsedDataRegion;

    public PlanContext() {
      this.nodeDistributionMap = new HashMap<>();
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }

    public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
      this.nodeDistributionMap.put(nodeId, distribution);
    }
  }
}
