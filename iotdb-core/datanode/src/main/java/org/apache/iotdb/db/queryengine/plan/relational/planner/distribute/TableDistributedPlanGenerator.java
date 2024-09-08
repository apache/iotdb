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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.AbstractTableDeviceQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Util.split;
import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.tsfile.utils.Preconditions.checkArgument;

/** This class is used to generate distributed plan for table model. */
public class TableDistributedPlanGenerator
    extends PlanVisitor<List<PlanNode>, TableDistributedPlanGenerator.PlanContext> {
  private final QueryId queryId;
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  Map<PlanNodeId, OrderingScheme> nodeOrderingMap = new HashMap<>();

  public TableDistributedPlanGenerator(
      MPPQueryContext queryContext, Analysis analysis, SymbolAllocator symbolAllocator) {
    this.queryId = queryContext.getQueryId();
    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
  }

  public List<PlanNode> genResult(PlanNode node, PlanContext context) {
    List<PlanNode> res = node.accept(this, context);
    if (res.size() == 1) {
      return res;
    } else if (res.size() > 1) {
      CollectNode collectNode =
          new CollectNode(queryId.genPlanNodeId(), res.get(0).getOutputSymbols());
      res.forEach(collectNode::addChild);
      return Collections.singletonList(collectNode);
    } else {
      throw new IllegalStateException("List<PlanNode>.size should >= 1, but now is 0");
    }
  }

  @Override
  public List<PlanNode> visitPlan(
      PlanNode node, TableDistributedPlanGenerator.PlanContext context) {
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
  public List<PlanNode> visitOutput(OutputNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitLimit(LimitNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    // push down LimitNode in distributed plan optimize rule
    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitOffset(OffsetNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitProject(ProjectNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    for (Expression expression : node.getAssignments().getMap().values()) {
      if (containsDiffFunction(expression)) {
        node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
        return Collections.singletonList(node);
      }
    }

    List<PlanNode> resultNodeList = new ArrayList<>();
    for (int i = 0; i < childrenNodes.size(); i++) {
      PlanNode child = childrenNodes.get(i);
      ProjectNode subProjectNode =
          new ProjectNode(queryId.genPlanNodeId(), child, node.getAssignments());
      resultNodeList.add(subProjectNode);
      if (i == 0) {
        nodeOrderingMap.put(subProjectNode.getPlanNodeId(), childOrdering);
      }
    }
    return resultNodeList;
  }

  @Override
  public List<PlanNode> visitTopK(TopKNode node, PlanContext context) {
    context.expectedOrderingScheme = node.getOrderingScheme();
    context.hasSortProperty = true;
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());

    checkArgument(
        node.getChildren().size() == 1, "Size of TopKNode can only be 1 in logical plan.");
    List<PlanNode> childrenNodes = node.getChildren().get(0).accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChildren(Collections.singletonList(childrenNodes.get(0)));
      return Collections.singletonList(node);
    }

    TopKNode newTopKNode = (TopKNode) node.clone();
    for (int i = 0; i < childrenNodes.size(); i++) {
      PlanNode child = childrenNodes.get(i);
      TopKNode subTopKNode =
          new TopKNode(
              queryId.genPlanNodeId(),
              Collections.singletonList(child),
              node.getOrderingScheme(),
              node.getCount(),
              node.getOutputSymbols(),
              node.isChildrenDataInOrder());
      newTopKNode.addChild(subTopKNode);
    }
    nodeOrderingMap.put(newTopKNode.getPlanNodeId(), newTopKNode.getOrderingScheme());

    return Collections.singletonList(newTopKNode);
  }

  @Override
  public List<PlanNode> visitSort(SortNode node, PlanContext context) {
    context.expectedOrderingScheme = node.getOrderingScheme();
    context.hasSortProperty = true;
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());

    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    // may have ProjectNode above SortNode later, so use MergeSortNode but not return SortNode list
    MergeSortNode mergeSortNode =
        new MergeSortNode(
            queryId.genPlanNodeId(), node.getOrderingScheme(), node.getOutputSymbols());
    for (PlanNode child : childrenNodes) {
      SortNode subSortNode =
          new SortNode(queryId.genPlanNodeId(), child, node.getOrderingScheme(), false, false);
      mergeSortNode.addChild(subSortNode);
    }
    nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), mergeSortNode.getOrderingScheme());

    return Collections.singletonList(mergeSortNode);
  }

  @Override
  public List<PlanNode> visitStreamSort(StreamSortNode node, PlanContext context) {
    context.expectedOrderingScheme = node.getOrderingScheme();
    context.hasSortProperty = true;
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());

    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    // may have ProjectNode above SortNode later, so use MergeSortNode but not return SortNode list
    MergeSortNode mergeSortNode =
        new MergeSortNode(
            queryId.genPlanNodeId(), node.getOrderingScheme(), node.getOutputSymbols());
    for (PlanNode child : childrenNodes) {
      StreamSortNode subSortNode =
          new StreamSortNode(
              queryId.genPlanNodeId(),
              child,
              node.getOrderingScheme(),
              false,
              node.isOrderByAllIdsAndTime(),
              node.getStreamCompareKeyEndIndex());
      mergeSortNode.addChild(subSortNode);
    }
    nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), mergeSortNode.getOrderingScheme());

    return Collections.singletonList(mergeSortNode);
  }

  @Override
  public List<PlanNode> visitFilter(FilterNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    if (containsDiffFunction(node.getPredicate())) {
      node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
      return Collections.singletonList(node);
    }

    List<PlanNode> resultNodeList = new ArrayList<>();
    for (int i = 0; i < childrenNodes.size(); i++) {
      PlanNode child = childrenNodes.get(i);
      FilterNode subFilterNode =
          new FilterNode(queryId.genPlanNodeId(), child, node.getPredicate());
      resultNodeList.add(subFilterNode);
      if (i == 0) {
        nodeOrderingMap.put(subFilterNode.getPlanNodeId(), childOrdering);
      }
    }
    return resultNodeList;
  }

  @Override
  public List<PlanNode> visitJoin(JoinNode node, PlanContext context) {
    // child of JoinNode must be SortNode, so after rewritten, the child must be MergeSortNode or
    // SortNode
    List<PlanNode> leftChildrenNodes = node.getLeftChild().accept(this, context);
    checkArgument(
        leftChildrenNodes.size() == 1, "The size of left children node of JoinNode should be 1");
    node.setLeftChild(leftChildrenNodes.get(0));

    List<PlanNode> rightChildrenNodes = node.getRightChild().accept(this, context);
    checkArgument(
        rightChildrenNodes.size() == 1, "The size of right children node of JoinNode should be 1");
    node.setRightChild(rightChildrenNodes.get(0));

    return Collections.singletonList(node);
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
                          queryId.genPlanNodeId(),
                          node.getQualifiedObjectName(),
                          node.getOutputSymbols(),
                          node.getAssignments(),
                          new ArrayList<>(),
                          node.getIdAndAttributeIndexMap(),
                          node.getScanOrder(),
                          node.getTimePredicate().orElse(null),
                          node.getPushDownPredicate(),
                          node.getPushDownLimit(),
                          node.getPushDownOffset(),
                          node.isPushLimitToEachDevice());
                  scanNode.setRegionReplicaSet(regionReplicaSet);
                  return scanNode;
                });
        tableScanNode.appendDeviceEntry(deviceEntry);
      }
    }

    List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (Map.Entry<TRegionReplicaSet, TableScanNode> entry : tableScanNodeMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet = entry.getKey();
      TableScanNode subTableScanNode = entry.getValue();
      subTableScanNode.setPlanNodeId(queryId.genPlanNodeId());
      subTableScanNode.setRegionReplicaSet(regionReplicaSet);
      resultTableScanNodeList.add(subTableScanNode);

      if (mostUsedDataRegion == null
          || subTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = regionReplicaSet;
        maxDeviceEntrySizeOfTableScan = subTableScanNode.getDeviceEntries().size();
      }
    }
    context.mostUsedRegion = mostUsedDataRegion;

    if (!context.hasSortProperty) {
      return resultTableScanNodeList;
    }

    processSortProperty(node, resultTableScanNodeList, context);
    return resultTableScanNodeList;
  }

  @Override
  public List<PlanNode> visitAggregation(AggregationNode node, PlanContext context) {
    if (node.isStreamable()) {
      context.expectedOrderingScheme = constructOrderingSchema(node.getPreGroupedSymbols());
      context.hasSortProperty = true;
    }
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    Pair<AggregationNode, AggregationNode> splitResult = split(node, symbolAllocator, queryId);
    AggregationNode intermediate = splitResult.right;

    childrenNodes =
        childrenNodes.stream()
            .map(
                child ->
                    new AggregationNode(
                        queryId.genPlanNodeId(),
                        child,
                        intermediate.getAggregations(),
                        intermediate.getGroupingSets(),
                        intermediate.getPreGroupedSymbols(),
                        intermediate.getStep(),
                        intermediate.getHashSymbol(),
                        intermediate.getGroupIdSymbol()))
            .collect(Collectors.toList());
    splitResult.left.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(splitResult.left);
  }

  @Override
  public List<PlanNode> visitAggregationTableScan(
      AggregationTableScanNode node, PlanContext context) {

    Map<TRegionReplicaSet, AggregationTableScanNode> tableScanNodeMap = new HashMap<>();
    boolean needSplit = false;
    List<List<TRegionReplicaSet>> regionReplicaSetsList = new ArrayList<>();
    for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetWithTimeFilter(
                  node.getQualifiedObjectName().getDatabaseName(),
                  deviceEntry.getDeviceID(),
                  node.getTimeFilter());
      if (regionReplicaSets.size() > 1) {
        needSplit = true;
      }
      regionReplicaSetsList.add(regionReplicaSets);
    }
    // Step is SINGLE, has date_bin(time) and device data in more than one region, we need to split
    // this node into two-stage Aggregation
    needSplit = needSplit && node.getProjection() != null && node.getStep() == SINGLE;
    AggregationNode finalAggregation = null;
    if (needSplit) {
      Pair<AggregationNode, AggregationTableScanNode> splitResult =
          split(node, symbolAllocator, queryId);
      finalAggregation = splitResult.left;
      AggregationTableScanNode partialAggregation = splitResult.right;
      for (int i = 0; i < regionReplicaSetsList.size(); i++) {
        for (TRegionReplicaSet regionReplicaSet : regionReplicaSetsList.get(i)) {
          AggregationTableScanNode aggregationTableScanNode =
              tableScanNodeMap.computeIfAbsent(
                  regionReplicaSet,
                  k -> {
                    AggregationTableScanNode scanNode =
                        new AggregationTableScanNode(
                            queryId.genPlanNodeId(),
                            partialAggregation.getQualifiedObjectName(),
                            partialAggregation.getOutputSymbols(),
                            partialAggregation.getAssignments(),
                            new ArrayList<>(),
                            partialAggregation.getIdAndAttributeIndexMap(),
                            partialAggregation.getScanOrder(),
                            partialAggregation.getTimePredicate().orElse(null),
                            partialAggregation.getPushDownPredicate(),
                            partialAggregation.getPushDownLimit(),
                            partialAggregation.getPushDownOffset(),
                            partialAggregation.isPushLimitToEachDevice(),
                            partialAggregation.getProjection(),
                            partialAggregation.getAggregations(),
                            partialAggregation.getGroupingSets(),
                            partialAggregation.getPreGroupedSymbols(),
                            partialAggregation.getStep(),
                            partialAggregation.getGroupIdSymbol());
                    scanNode.setRegionReplicaSet(regionReplicaSet);
                    return scanNode;
                  });
          aggregationTableScanNode.appendDeviceEntry(node.getDeviceEntries().get(i));
        }
      }
    } else {
      for (int i = 0; i < regionReplicaSetsList.size(); i++) {
        for (TRegionReplicaSet regionReplicaSet : regionReplicaSetsList.get(i)) {
          AggregationTableScanNode aggregationTableScanNode =
              tableScanNodeMap.computeIfAbsent(
                  regionReplicaSet,
                  k -> {
                    AggregationTableScanNode scanNode =
                        new AggregationTableScanNode(
                            queryId.genPlanNodeId(),
                            node.getQualifiedObjectName(),
                            node.getOutputSymbols(),
                            node.getAssignments(),
                            new ArrayList<>(),
                            node.getIdAndAttributeIndexMap(),
                            node.getScanOrder(),
                            node.getTimePredicate().orElse(null),
                            node.getPushDownPredicate(),
                            node.getPushDownLimit(),
                            node.getPushDownOffset(),
                            node.isPushLimitToEachDevice(),
                            node.getProjection(),
                            node.getAggregations(),
                            node.getGroupingSets(),
                            node.getPreGroupedSymbols(),
                            node.getStep(),
                            node.getGroupIdSymbol());
                    scanNode.setRegionReplicaSet(regionReplicaSet);
                    return scanNode;
                  });
          aggregationTableScanNode.appendDeviceEntry(node.getDeviceEntries().get(i));
        }
      }
    }

    List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (Map.Entry<TRegionReplicaSet, AggregationTableScanNode> entry :
        tableScanNodeMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet = entry.getKey();
      TableScanNode subTableScanNode = entry.getValue();
      subTableScanNode.setPlanNodeId(queryId.genPlanNodeId());
      subTableScanNode.setRegionReplicaSet(regionReplicaSet);
      resultTableScanNodeList.add(subTableScanNode);

      if (mostUsedDataRegion == null
          || subTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = regionReplicaSet;
        maxDeviceEntrySizeOfTableScan = subTableScanNode.getDeviceEntries().size();
      }
    }
    context.mostUsedRegion = mostUsedDataRegion;

    if (node.isStreamable()) {
      context.expectedOrderingScheme = constructOrderingSchema(node.getPreGroupedSymbols());
      processSortProperty(node, resultTableScanNodeList, context);
    } else {
      context.expectedOrderingScheme = null;
      context.hasSortProperty = false;
    }

    if (needSplit) {
      if (resultTableScanNodeList.size() == 1) {
        finalAggregation.setChild(resultTableScanNodeList.get(0));
      } else if (resultTableScanNodeList.size() > 1) {
        finalAggregation.setChild(
            mergeChildrenViaCollectOrMergeSort(
                context.expectedOrderingScheme, resultTableScanNodeList));
      } else {
        throw new IllegalStateException("List<PlanNode>.size should >= 1, but now is 0");
      }
      resultTableScanNodeList = Collections.singletonList(finalAggregation);
    }

    return resultTableScanNodeList;
  }

  private static OrderingScheme constructOrderingSchema(List<Symbol> symbols) {
    Map<Symbol, SortOrder> orderings = new HashMap<>();
    symbols.forEach(symbol -> orderings.put(symbol, SortOrder.ASC_NULLS_LAST));
    return new OrderingScheme(symbols, orderings);
  }

  private PlanNode mergeChildrenViaCollectOrMergeSort(
      OrderingScheme childOrdering, List<PlanNode> childrenNodes) {
    PlanNode firstChild = childrenNodes.get(0);

    // children has sort property, use MergeSort to merge children
    if (childOrdering != null) {
      MergeSortNode mergeSortNode =
          new MergeSortNode(queryId.genPlanNodeId(), childOrdering, firstChild.getOutputSymbols());
      childrenNodes.forEach(mergeSortNode::addChild);
      nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), childOrdering);
      return mergeSortNode;
    }

    // children has no sort property, use CollectNode to merge children
    CollectNode collectNode =
        new CollectNode(queryId.genPlanNodeId(), firstChild.getOutputSymbols());
    childrenNodes.forEach(collectNode::addChild);
    return collectNode;
  }

  private void processSortProperty(
      final TableScanNode tableScanNode,
      final List<PlanNode> resultTableScanNodeList,
      final PlanContext context) {
    final List<Symbol> newOrderingSymbols = new ArrayList<>();
    final List<SortOrder> newSortOrders = new ArrayList<>();
    final OrderingScheme expectedOrderingScheme = context.expectedOrderingScheme;

    for (final Symbol symbol : expectedOrderingScheme.getOrderBy()) {
      if (TIMESTAMP_STR.equalsIgnoreCase(symbol.getName())) {
        if (!expectedOrderingScheme.getOrderings().get(symbol).isAscending()) {
          // TODO(beyyes) move scan order judgement into logical plan optimizer
          resultTableScanNodeList.forEach(
              node -> ((TableScanNode) node).setScanOrder(Ordering.DESC));
        }
        break;
      } else if (!tableScanNode.getIdAndAttributeIndexMap().containsKey(symbol)) {
        break;
      }

      newOrderingSymbols.add(symbol);
      newSortOrders.add(expectedOrderingScheme.getOrdering(symbol));
    }

    // no sort property can be pushed down into TableScanNode
    if (newOrderingSymbols.isEmpty()) {
      return;
    }

    final List<Function<DeviceEntry, String>> orderingRules = new ArrayList<>();
    for (final Symbol symbol : newOrderingSymbols) {
      final int idx = tableScanNode.getIdAndAttributeIndexMap().get(symbol);
      if (tableScanNode.getAssignments().get(symbol).getColumnCategory()
          == TsTableColumnCategory.ID) {
        // segments[0] is always tableName
        orderingRules.add(deviceEntry -> (String) deviceEntry.getNthSegment(idx + 1));
      } else {
        orderingRules.add(deviceEntry -> deviceEntry.getAttributeColumnValues().get(idx));
      }
    }

    Comparator<DeviceEntry> comparator;
    if (newSortOrders.get(0).isNullsFirst()) {
      comparator =
          newSortOrders.get(0).isAscending()
              ? Comparator.comparing(
                  orderingRules.get(0), Comparator.nullsFirst(Comparator.naturalOrder()))
              : Comparator.comparing(
                      orderingRules.get(0), Comparator.nullsFirst(Comparator.naturalOrder()))
                  .reversed();
    } else {
      comparator =
          newSortOrders.get(0).isAscending()
              ? Comparator.comparing(
                  orderingRules.get(0), Comparator.nullsLast(Comparator.naturalOrder()))
              : Comparator.comparing(
                      orderingRules.get(0), Comparator.nullsLast(Comparator.naturalOrder()))
                  .reversed();
    }
    for (int i = 1; i < orderingRules.size(); i++) {
      final Comparator<DeviceEntry> thenComparator;
      if (newSortOrders.get(i).isNullsFirst()) {
        thenComparator =
            newSortOrders.get(i).isAscending()
                ? Comparator.comparing(
                    orderingRules.get(i), Comparator.nullsFirst(Comparator.naturalOrder()))
                : Comparator.comparing(
                        orderingRules.get(i), Comparator.nullsFirst(Comparator.naturalOrder()))
                    .reversed();
      } else {
        thenComparator =
            newSortOrders.get(i).isAscending()
                ? Comparator.comparing(
                    orderingRules.get(i), Comparator.nullsLast(Comparator.naturalOrder()))
                : Comparator.comparing(
                        orderingRules.get(i), Comparator.nullsLast(Comparator.naturalOrder()))
                    .reversed();
      }
      comparator = comparator.thenComparing(thenComparator);
    }

    final OrderingScheme newOrderingScheme =
        new OrderingScheme(
            newOrderingSymbols,
            IntStream.range(0, newOrderingSymbols.size())
                .boxed()
                .collect(Collectors.toMap(newOrderingSymbols::get, newSortOrders::get)));
    for (final PlanNode planNode : resultTableScanNodeList) {
      final TableScanNode scanNode = (TableScanNode) planNode;
      nodeOrderingMap.put(scanNode.getPlanNodeId(), newOrderingScheme);
      scanNode.getDeviceEntries().sort(comparator);
    }
  }

  // ------------------- schema related interface ---------------------------------------------
  @Override
  public List<PlanNode> visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node, final PlanContext context) {
    return visitAbstractTableDeviceQuery(node, context);
  }

  @Override
  public List<PlanNode> visitTableDeviceQueryCount(
      final TableDeviceQueryCountNode node, final PlanContext context) {
    return visitAbstractTableDeviceQuery(node, context);
  }

  private List<PlanNode> visitAbstractTableDeviceQuery(
      final AbstractTableDeviceQueryNode node, final PlanContext context) {
    final String database = PathUtils.qualifyDatabaseName(node.getDatabase());
    final Set<TRegionReplicaSet> schemaRegionSet = new HashSet<>();
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .get(database)
        .forEach(
            (deviceGroupId, schemaRegionReplicaSet) -> schemaRegionSet.add(schemaRegionReplicaSet));

    context.mostUsedRegion = schemaRegionSet.iterator().next();
    if (schemaRegionSet.size() == 1) {
      node.setRegionReplicaSet(schemaRegionSet.iterator().next());
      return Collections.singletonList(node);
    } else {
      final List<PlanNode> res = new ArrayList<>(schemaRegionSet.size());
      for (final TRegionReplicaSet schemaRegion : schemaRegionSet) {
        final AbstractTableDeviceQueryNode clonedChild =
            (AbstractTableDeviceQueryNode) node.clone();
        clonedChild.setPlanNodeId(queryId.genPlanNodeId());
        clonedChild.setRegionReplicaSet(schemaRegion);
        res.add(clonedChild);
      }
      return res;
    }
  }

  @Override
  public List<PlanNode> visitTableDeviceFetch(
      final TableDeviceFetchNode node, final PlanContext context) {
    final String database = PathUtils.qualifyDatabaseName(node.getDatabase());
    final Set<TRegionReplicaSet> schemaRegionSet = new HashSet<>();
    final SchemaPartition schemaPartition = analysis.getSchemaPartitionInfo();
    final Map<TSeriesPartitionSlot, TRegionReplicaSet> databaseMap =
        schemaPartition.getSchemaPartitionMap().get(database);

    databaseMap.forEach(
        (deviceGroupId, schemaRegionReplicaSet) -> schemaRegionSet.add(schemaRegionReplicaSet));

    if (schemaRegionSet.size() == 1) {
      context.mostUsedRegion = schemaRegionSet.iterator().next();
      node.setRegionReplicaSet(context.mostUsedRegion);
      return Collections.singletonList(node);
    } else {
      final Map<TRegionReplicaSet, TableDeviceFetchNode> tableDeviceFetchMap = new HashMap<>();

      for (final Object[] deviceIdArray : node.getDeviceIdList()) {
        final IDeviceID deviceID =
            IDeviceID.Factory.DEFAULT_FACTORY.create((String[]) deviceIdArray);
        final TRegionReplicaSet regionReplicaSet =
            databaseMap.get(schemaPartition.calculateDeviceGroupId(deviceID));
        tableDeviceFetchMap
            .computeIfAbsent(
                regionReplicaSet,
                k ->
                    new TableDeviceFetchNode(
                        queryId.genPlanNodeId(),
                        node.getDatabase(),
                        node.getTableName(),
                        new ArrayList<>(),
                        node.getColumnHeaderList(),
                        regionReplicaSet))
            .addDeviceId(deviceIdArray);
      }

      final List<PlanNode> res = new ArrayList<>();
      TRegionReplicaSet mostUsedDataRegion = null;
      int maxDeviceEntrySizeOfTableScan = 0;
      for (final Map.Entry<TRegionReplicaSet, TableDeviceFetchNode> entry :
          tableDeviceFetchMap.entrySet()) {
        final TRegionReplicaSet regionReplicaSet = entry.getKey();
        final TableDeviceFetchNode subTableDeviceFetchNode = entry.getValue();
        res.add(subTableDeviceFetchNode);

        if (subTableDeviceFetchNode.getDeviceIdList().size() > maxDeviceEntrySizeOfTableScan) {
          mostUsedDataRegion = regionReplicaSet;
          maxDeviceEntrySizeOfTableScan = subTableDeviceFetchNode.getDeviceIdList().size();
        }
      }
      context.mostUsedRegion = mostUsedDataRegion;
      return res;
    }
  }

  public static class PlanContext {
    final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
    boolean hasExchangeNode = false;
    boolean hasSortProperty = false;
    OrderingScheme expectedOrderingScheme;
    TRegionReplicaSet mostUsedRegion;

    public PlanContext() {
      this.nodeDistributionMap = new HashMap<>();
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }
  }
}
